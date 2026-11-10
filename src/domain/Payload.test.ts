import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Str from "effect/String"
import { AddTally, CurrentTally, OldAddTally, ProjectedTally } from "../../examples/fixture/domain/src/Evolution.ts"
import * as Domain from "./index.ts"

// A permissive union, first-match path or inferred legacy default would accept these invalid declarations/payloads.
it.effect("INV-SCHEMA-3 missing paths and ambiguous defaults cannot silently reinterpret persisted payloads", () =>
  Effect.gen(function* () {
    const read = Schema.decodeUnknownEffect(Domain.Payload.codec(AddTally))
    const old = { _tag: "AddTally", target: "one", pairs: 3 }
    expect((yield* read(old)).units).toBe(6)
    expect((yield* read({ _tag: "AddTally", target: "one", units: 5, _version: 2 })).units).toBe(5)
    expect(
      yield* Schema.encodeEffect(Domain.Payload.codec(AddTally))(new AddTally({ target: "one", units: 7 }))
    ).toEqual({ _tag: "AddTally", target: "one", units: 7, _version: 2 })
    for (const version of [0, -1, 1.5, 3, "2", null])
      expect((yield* read({ ...old, _version: version }).pipe(Effect.flip))._tag).toBe("SchemaError")
    expect((yield* read({ _tag: "AddTally", target: "one", units: 5 }).pipe(Effect.flip))._tag).toBe("SchemaError")
    expect(
      (yield* Schema.decodeEffect(Domain.Payload.codec(OldAddTally))({ ...old, _version: 2 }).pipe(Effect.flip))._tag
    ).toBe("SchemaError")
    expect(
      Domain.Payload.inspect({ version: 2, unversioned: null, history: [Domain.Payload.reject(1, "Unsafe meaning")] })
    ).toEqual({
      version: 2,
      unversioned: null,
      history: [{ version: 1, description: "Unsafe meaning", supported: false }]
    })
    expect(() => Domain.Payload.inspect({ version: 2, unversioned: 1, history: [] })).toThrow("exactly one")
    expect(() =>
      Domain.Payload.inspect({
        version: 3,
        unversioned: 1,
        history: [Domain.Payload.reject(1, "one"), Domain.Payload.reject(1, "duplicate")]
      })
    ).toThrow("exactly one")
    expect(() => Domain.Payload.inspect({ version: 1, unversioned: 2, history: [] })).toThrow(
      "explicit supported version"
    )
    const refused = Domain.Command("AddTally", {
      target: Schema.String,
      input: { units: Schema.Int },
      events: [],
      evolution: { version: 2, unversioned: null, history: [Domain.Payload.reject(1, "Cannot preserve old intent")] }
    })
    expect((yield* Schema.decodeEffect(Domain.Payload.codec(refused))(old).pipe(Effect.flip))._tag).toBe("SchemaError")
    expect(
      (yield* Schema.decodeEffect(Domain.Payload.codec(refused))({ ...old, _version: 1 }).pipe(Effect.flip))._tag
    ).toBe("SchemaError")
    const moved = Domain.Command("AddTally", {
      target: Schema.String,
      input: { units: Schema.Int },
      events: [],
      evolution: {
        version: 2,
        unversioned: 1,
        history: [
          Domain.Payload.from(1, OldAddTally, "Incorrect retarget", () => ({
            _tag: "AddTally" as const,
            target: "other",
            units: 4
          }))
        ]
      }
    })
    expect(Exit.hasDies(yield* Schema.decodeEffect(Domain.Payload.codec(moved))(old).pipe(Effect.exit))).toBe(true)
    const wireId = Schema.String.pipe(
      Schema.decodeTo(Schema.String, {
        decode: SchemaGetter.transform((value) => Str.slice(5)(value)),
        encode: SchemaGetter.transform((value) => `wire:${value}`)
      })
    )
    const oldWire = Domain.Command("Wire", { target: wireId, input: { value: Schema.DateTimeUtc }, events: [] })
    const newWire = Domain.Command("Wire", {
      target: wireId,
      input: { value: Schema.DateTimeUtc },
      events: [],
      evolution: {
        version: 2,
        unversioned: 1,
        history: [Domain.Payload.from(1, oldWire, "Identity and instant unchanged", (value) => value)]
      }
    })
    const wire = { _tag: "Wire", target: "wire:one", value: "2026-01-02T03:04:05.000Z" }
    const decoded = yield* Schema.decodeEffect(Domain.Payload.codec(newWire))(wire)
    expect(decoded.target).toBe("one")
    expect(yield* Schema.encodeEffect(Domain.Payload.codec(newWire))(decoded)).toEqual({ ...wire, _version: 2 })
    const projection = Domain.Payload.projection(Schema.Struct({ at: Schema.DateTimeUtc }), 3)
    const state = { _projection: 3, state: { at: wire.value } }
    expect(yield* Schema.encodeEffect(projection)(yield* Schema.decodeEffect(projection)(state))).toEqual(state)
    const targetedFact = Domain.Event("TargetedFact", { target: Schema.Int })
    expect(
      (yield* Schema.decodeEffect(Domain.Payload.codec(targetedFact))({ _tag: "TargetedFact", target: 42 })).target
    ).toBe(42)
    const invalid = Domain.Command("AddTally", {
      target: Schema.String,
      input: { units: Schema.Int },
      events: [],
      evolution: {
        version: 2,
        unversioned: 1,
        history: [
          Domain.Payload.from(1, OldAddTally, "Broken numeric adapter", (value) => ({
            _tag: "AddTally" as const,
            target: value.target,
            units: NaN
          }))
        ]
      }
    })
    expect(Exit.hasDies(yield* Schema.decodeEffect(Domain.Payload.codec(invalid))(old).pipe(Effect.exit))).toBe(true)
    const oldFact = Domain.Event("RefinedFact", { amount: Schema.Int })
    const currentFact = Domain.Event(
      "RefinedFact",
      { amount: Schema.Int.check(Schema.isGreaterThan(0)) },
      {
        version: 2,
        unversioned: 1,
        history: [
          Domain.Payload.from(1, oldFact, "Broken identity adapter ignores the tightened contract", (value) => value)
        ]
      }
    )
    expect(
      Exit.hasDies(
        yield* Schema.decodeEffect(Domain.Payload.codec(currentFact))({ _tag: "RefinedFact", amount: -1 }).pipe(
          Effect.exit
        )
      )
    ).toBe(true)
    expect(() =>
      Domain.Aggregate("Duplicate", { ...CurrentTally.definition, commands: [AddTally, AddTally], indexes: {} })
    ).toThrow("Conflicting command tags")
    expect(
      (yield* Schema.decodeUnknownEffect(ProjectedTally.definition.state)({ value: 6, scope: "user:alice" }).pipe(
        Effect.flip
      ))._tag
    ).toBe("SchemaError")
    expect(
      Domain.replay(CurrentTally, [
        { _tag: "TallyAdded", pairs: 3, scope: "user:alice" },
        { _tag: "TallyAdded", units: 5, scope: "user:alice", _version: 2 }
      ])
    ).toEqual({ value: 11, scope: "user:alice" })
  })
)
