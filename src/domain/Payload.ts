import * as Effect from "effect/Effect"
import * as Arr from "effect/Array"
import * as Option from "effect/Option"
import * as Order from "effect/Order"
import * as Str from "effect/String"
import { memoize } from "effect/Function"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as SchemaIssue from "effect/SchemaIssue"
import type { Bivariant, Codec } from "./internal/classes.ts"

/** One direct, pure translation to the current semantic representation, or an explicit refusal. */
export interface Historical<A> {
  readonly version: number
  readonly description: string
  readonly adapter?: {
    readonly schema: Codec
    readonly adapt: Bivariant<[old: unknown], A>
  }
}

export const from = <S extends Codec, A>(
  version: number,
  schema: S,
  description: string,
  adapt: (old: S["Type"]) => A
): Historical<A> => ({
  version,
  description,
  adapter: { schema, adapt }
})

export const reject = (version: number, description: string): Historical<never> => ({ version, description })

export interface Evolution<A> {
  readonly version: number
  /** The known version of payloads written before version stamps; null refuses them. Never guess by shape. */
  readonly unversioned: number | null
  /** Every earlier positive integer version needs exactly one adapter or explicit rejection. */
  readonly history: ReadonlyArray<Historical<A>>
}

const Version = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }))
const envelope = Schema.Struct({
  _version: Schema.optionalKey(Version),
  _tag: Schema.String
})
const addressed = Schema.Struct({ target: Schema.String })
const object = Schema.Record(Schema.String, Schema.Unknown)

/** Deterministic deployment inventory; construction refuses holes, duplicate paths and unspecified legacy defaults. */
export const inspect = <A>(evolution: Evolution<A>) => {
  if (!Schema.is(Version)(evolution.version)) throw new Error("Invalid payload version")
  const history = Arr.sort(
    evolution.history,
    Order.mapInput(Order.Number, (entry: Historical<A>) => entry.version)
  )
  if (history.length !== evolution.version - 1 || Arr.some(history, (entry, i) => entry.version !== i + 1))
    throw new Error("Every historical version needs exactly one adapter or rejection")
  if (
    evolution.unversioned !== null &&
    (!Schema.is(Version)(evolution.unversioned) || evolution.unversioned > evolution.version)
  )
    throw new Error("Unversioned payloads need an explicit supported version or null")
  for (const entry of history) if (Str.trim(entry.description) === "") throw new Error("Explain historical semantics")
  return {
    version: evolution.version,
    unversioned: evolution.unversioned,
    history: Arr.map(history, (entry) => ({
      version: entry.version,
      description: entry.description,
      supported: entry.adapter !== undefined
    }))
  }
}

/** Kept on command/event classes without changing their constructors or decoded instance types. */
export interface Versioned extends Codec {
  readonly evolution?: Evolution<unknown>
}

export const codec = memoize(<S extends Versioned>(schema: S) => {
  const options = schema.evolution
  if (options !== undefined) inspect(options)
  const json = Schema.toCodecJson(schema)
  return Schema.Unknown.pipe(
    Schema.decodeTo(Schema.toType(schema), {
      decode: SchemaGetter.transformEffect(
        Effect.fnUntraced(function* (value) {
          const stamp = yield* Schema.decodeUnknownEffect(envelope)(value).pipe(Effect.mapError((e) => e.issue))
          const version = stamp["_version"] ?? (options === undefined ? 1 : options.unversioned)
          if (version === (options?.version ?? 1))
            return yield* Schema.decodeUnknownEffect(json)(value).pipe(Effect.mapError((e) => e.issue))
          const old = Option.getOrUndefined(Arr.findFirst(options?.history ?? [], (entry) => entry.version === version))
          const adapter = old?.adapter
          if (adapter === undefined)
            return yield* Effect.fail(new SchemaIssue.InvalidValue({ message: "Unsupported payload version" }))
          const historical = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(adapter.schema))(value).pipe(
            Effect.mapError((e) => e.issue)
          )
          // Invalid adapter outputs are programming defects, not incompatible-input notices.
          const current = yield* Effect.sync(() => schema.make(adapter.adapt(historical)))
          // Same-tag historical class instances share Effect's class marker; make alone can skip field checks.
          const encoded = yield* Schema.encodeEffect(json)(current).pipe(Effect.orDie)
          const identity = yield* Schema.decodeUnknownEffect(envelope)(current).pipe(Effect.orDie)
          if (identity._tag !== stamp._tag) return yield* Effect.die(new Error("Payload adapters must preserve tag"))
          if ("kind" in schema && schema.kind === "Command") {
            const before = yield* Schema.decodeUnknownEffect(addressed)(value).pipe(Effect.mapError((e) => e.issue))
            const after = yield* Schema.decodeUnknownEffect(addressed)(encoded).pipe(Effect.orDie)
            if (before.target !== after.target)
              return yield* Effect.die(new Error("Command adapters must preserve target"))
          }
          return current
        })
      ),
      encode: SchemaGetter.transformEffect(
        Effect.fnUntraced(function* (value) {
          const encoded = yield* Schema.encodeEffect(json)(value).pipe(Effect.mapError((e) => e.issue))
          if (options === undefined) return encoded
          const fields = yield* Schema.decodeUnknownEffect(object)(encoded).pipe(Effect.mapError((e) => e.issue))
          return { ...fields, _version: options.version }
        })
      )
    })
  )
})

/** Domain objects alone use payload dispatch; ids, rejection and state codecs retain their own representation. */
export const persisted = <S extends Codec>(schema: S) =>
  "kind" in schema && (schema.kind === "Command" || schema.kind === "Event")
    ? codec(schema)
    : Schema.toCodecJson(schema)

/** Projection versions are independent of payload versions. No state upcast: rebuild from facts or reingest catalogs. */
export const projection = <S extends Codec>(schema: S, version: number) => {
  if (!Schema.is(Version)(version)) throw new Error("Invalid projection version")
  const json = Schema.toCodecJson(schema)
  return Schema.Struct({ _projection: Schema.Literal(version), state: Schema.Unknown }).pipe(
    Schema.decodeTo(Schema.toType(schema), {
      decode: SchemaGetter.transformEffect((value) =>
        Schema.decodeUnknownEffect(json)(value.state).pipe(Effect.mapError((e) => e.issue))
      ),
      encode: SchemaGetter.transformEffect((state) =>
        Schema.encodeEffect(json)(state).pipe(
          Effect.map((encoded) => ({ _projection: version, state: encoded })),
          Effect.mapError((e) => e.issue)
        )
      )
    })
  )
}
