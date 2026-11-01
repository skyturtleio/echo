/* Pure synchronous domain evaluation and selectors; the client wraps failures at its Effect boundary. */
/* oxlint-disable effecttsgo/schema-sync */
import * as Array from "effect/Array"
import * as DateTime from "effect/DateTime"
import * as Equal from "effect/Equal"
import { memoize } from "effect/Function"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Aggregate from "../domain/Aggregate.ts"
import type * as Command from "../domain/Command.ts"
import type * as Index from "../domain/Indexes.ts"
import { canRead, canWrite, Grant, type Principal, ScopeForbidden } from "../domain/Scope.ts"
import { Batch, Receipt } from "../protocol.ts"

export const Row = Schema.Struct({
  aggregate: Schema.String,
  id: Schema.String,
  scope: Schema.String,
  seq: Schema.Number,
  state: Schema.Unknown
})
export type Row = typeof Row.Type
export const Intent = Schema.Struct({
  ...Batch.fields,
  now: Schema.Number,
  attemptEpoch: Schema.NullOr(Schema.String),
  dismissed: Schema.Boolean,
  phase: Schema.Literals(["Pending", "Sent", "Accepted", "Unknown", "Rejected"]),
  receipt: Schema.NullOr(Receipt),
  rejection: Schema.Unknown
})
export type Intent = typeof Intent.Type
export const Journal = Schema.Struct({
  principal: Schema.String,
  clientId: Schema.String,
  epoch: Schema.NullOr(Schema.String),
  intents: Schema.Array(Intent)
})
export type Journal = typeof Journal.Type
export const Replica = Schema.Struct({
  epoch: Schema.NullOr(Schema.String),
  revision: Schema.Number,
  grants: Schema.Array(Grant),
  cursors: Schema.Record(Schema.String, Schema.Number),
  rows: Schema.Array(Row)
})
export type Replica = typeof Replica.Type

/** Borrowed immutable snapshots, including nested state. Copy before editing; identity may be reused. */
export interface Read {
  get<A extends Aggregate.Any>(aggregate: A, id: Aggregate.IdOf<A>): Aggregate.StateOf<A> | null
  scan<A extends Aggregate.Any>(
    aggregate: A
  ): ReadonlyArray<{ readonly id: Aggregate.IdOf<A>; readonly state: Aggregate.StateOf<A> }>
  index<I extends Index.Any>(index: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
}

/** Schema registry is the boundary where persisted unknown values regain domain types. */
const codec = memoize((schema: Schema.Top) => {
  // Platform serialization boundary: domain schemas must be synchronous and service-free.
  const json = Schema.make<Schema.Codec<unknown, unknown>>(Schema.toCodecJson(schema).ast)
  return { decode: Schema.decodeUnknownSync(json), encode: Schema.encodeSync(json) }
})
export const decode = (schema: Schema.Top, value: unknown): unknown => codec(schema).decode(value)
export const encode = (schema: Schema.Top, value: unknown): unknown => codec(schema).encode(value)

export type DomainRejection<A extends Aggregate.Any> =
  | Aggregate.InstanceOf<Aggregate.CommandsOf<A>["rejects"][number]>
  | ScopeForbidden

/** Restore named domain rejection classes at the public local-execution boundary. */
export const decodeRejection = <A extends Aggregate.Any>(aggregate: A, value: unknown): DomainRejection<A> => {
  const schemas = [...Array.flatMap(aggregate.definition.commands, (command) => command.rejects), ScopeForbidden]
  const schema = Schema.make<Schema.Codec<DomainRejection<A>, unknown>>(Schema.toCodecJson(Schema.Union(schemas)).ast)
  return Schema.decodeUnknownSync(schema)(value)
}

/** One cache per client principal. Engine rows and domain definitions are immutable once installed. */
export const makeReader = (principal: Principal, grants: ReadonlyArray<Grant> = []) => {
  // Weak row keys let old snapshots go; aggregate identity includes both codec and scope semantics.
  const entry = memoize((aggregate: Aggregate.Any) =>
    memoize((row: Row) => {
      const state = decode(aggregate.definition.state, row.state)
      const scope = aggregate.definition.scope(state)
      return scope !== null && canRead(principal, scope, grants)
        ? { id: aggregate.definition.id.make(row.id), state }
        : null
    })
  )
  return memoize((rows: ReadonlyArray<Row>): Read => {
    let grouped: Map<string, Map<string, Row>> | undefined
    const group = (aggregate: Aggregate.Any) => {
      if (grouped === undefined) {
        grouped = new Map()
        for (const row of rows) {
          let members = grouped.get(row.aggregate)
          if (members === undefined) {
            members = new Map()
            grouped.set(row.aggregate, members)
          }
          members.set(row.id, row)
        }
      }
      return grouped.get(aggregate.tag)
    }
    const visible = memoize((aggregate: Aggregate.Any) => {
      const entries: Array<{ readonly id: string; readonly state: unknown }> = []
      for (const row of group(aggregate)?.values() ?? []) {
        const value = entry(aggregate)(row)
        if (value !== null) entries.push(value)
      }
      return entries
    })
    function get<A extends Aggregate.Any>(aggregate: A, id: Aggregate.IdOf<A>): Aggregate.StateOf<A> | null {
      const row = group(aggregate)?.get(id)
      const value = row === undefined ? null : entry(aggregate)(row)
      return value === null ? null : value.state
    }
    function scan<A extends Aggregate.Any>(
      aggregate: A
    ): ReadonlyArray<{ readonly id: Aggregate.IdOf<A>; readonly state: Aggregate.StateOf<A> }> {
      return visible(aggregate)
    }
    function index<I extends Index.Any>(ref: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
    function index(ref: Index.Any, value: unknown): unknown {
      const owner = Aggregate.ofIndex(ref)
      if (Option.isNone(owner)) return []
      const values = ref.fields.length === 1 ? [value] : value
      if (!Array.isArray(values)) throw new Error("Index requires one value per field")
      return Array.filter(scan(owner.value), ({ state }) =>
        Array.every(
          ref.fields,
          (field, i) => Predicate.hasProperty(state, field) && Equal.equals(state[field], values[i])
        )
      )
    }
    return { get, scan, index }
  })
}

export const reader = (rows: ReadonlyArray<Row>, principal: Principal, grants: ReadonlyArray<Grant> = []): Read =>
  makeReader(principal, grants)(rows)

export const decide = (
  aggregates: ReadonlyArray<Aggregate.Any>,
  rows: ReadonlyArray<Row>,
  principal: Principal,
  intent: Intent,
  grants: ReadonlyArray<Grant> = []
): { readonly rows: ReadonlyArray<Row>; readonly rejection: unknown | null } => {
  if (intent.action !== undefined) return { rows, rejection: null }
  let next = rows
  for (const item of intent.commands) {
    const result = decideCommand(aggregates, next, principal, { ...item, now: intent.now }, grants)
    if (result.rejection !== null) return { rows, rejection: result.rejection }
    next = result.rows
  }
  return { rows: next, rejection: null }
}

const decideCommand = (
  aggregates: ReadonlyArray<Aggregate.Any>,
  rows: ReadonlyArray<Row>,
  principal: Principal,
  intent: { readonly aggregate: string; readonly command: unknown; readonly now: number },
  grants: ReadonlyArray<Grant>
): { readonly rows: ReadonlyArray<Row>; readonly rejection: unknown | null } => {
  const aggregate = Option.getOrUndefined(Array.findFirst(aggregates, (a) => a.tag === intent.aggregate))
  if (aggregate === undefined) throw new Error(`Unknown aggregate ${intent.aggregate}`)
  const envelope = Schema.decodeUnknownSync(Schema.Struct({ _tag: Schema.String, target: Schema.String }))(
    intent.command
  )
  const commandClass = Option.getOrUndefined(
    Array.findFirst(aggregate.definition.commands, (c) => c.tag === envelope._tag)
  )
  if (commandClass === undefined) throw new Error(`Unknown command ${envelope._tag}`)
  if (commandClass.authority === "server") throw new Error("Server-only commands require a trusted action")
  const command = decode(commandClass, intent.command)
  if (!Predicate.hasProperty(command, "_tag") || typeof command._tag !== "string")
    throw new Error("Invalid command tag")
  const row = Option.getOrUndefined(
    Array.findFirst(rows, (r) => r.aggregate === aggregate.tag && r.id === envelope.target)
  )
  let state = row === undefined ? aggregate.definition.initial : decode(aggregate.definition.state, row.state)
  const scope = aggregate.definition.scope(state)
  if (scope !== null && !canWrite(principal, scope, grants))
    return { rows, rejection: encode(ScopeForbidden, new ScopeForbidden({ scope })) }
  const read = reader(rows, principal, grants)
  function declared<R extends Aggregate.Any>(ref: R, id: Aggregate.IdOf<R>): Aggregate.StateOf<R> | null
  function declared<I extends Index.Any>(ref: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
  function declared(ref: Command.Read, value: unknown): unknown {
    if (!commandClass?.reads.includes(ref)) throw new Error(`Undeclared read by ${envelope._tag}`)
    if (ref.kind === "Aggregate") return read.get(ref, Schema.decodeUnknownSync(Schema.String)(value))
    const owner = Aggregate.ofIndex(ref)
    if (Option.isNone(owner)) return []
    const values = ref.fields.length === 1 ? [value] : value
    if (!Array.isArray(values)) throw new Error("Index requires one value per field")
    return Array.filter(read.scan(owner.value), ({ state: candidate }) =>
      Array.every(
        ref.fields,
        (field, i) => Predicate.hasProperty(candidate, field) && Equal.equals(candidate[field], values[i])
      )
    )
  }
  const handler = aggregate.definition.decide[envelope._tag]
  if (handler === undefined) throw new Error("Missing decision handler")
  const outcome = handler(
    state,
    { ...command, _tag: command._tag },
    { principal, now: DateTime.makeUnsafe(intent.now), read: declared, grants }
  )
  if (!Array.isArray(outcome)) {
    const rejection = Option.getOrUndefined(
      Array.findFirst(commandClass.rejects, (schema) => Schema.is(schema)(outcome))
    )
    if (rejection === undefined) throw new Error("Undeclared rejection")
    return { rows, rejection: encode(rejection, outcome) }
  }
  if (outcome.length === 0) return { rows, rejection: null }
  for (const event of outcome) {
    const tagged = Schema.decodeUnknownSync(Schema.Struct({ _tag: Schema.String }))(event)
    const evolve = aggregate.definition.evolve[tagged._tag]
    if (evolve === undefined) throw new Error("Missing evolve handler")
    if (!Predicate.hasProperty(event, "_tag")) throw new Error("Invalid event")
    state = evolve(state, { ...event, _tag: tagged._tag })
  }
  const nextScope = aggregate.definition.scope(state)
  if (nextScope !== null && !canWrite(principal, nextScope, grants))
    return { rows, rejection: encode(ScopeForbidden, new ScopeForbidden({ scope: nextScope })) }
  return {
    rows: [
      ...Array.filter(rows, (r) => r !== row),
      {
        aggregate: aggregate.tag,
        id: envelope.target,
        scope: nextScope ?? "",
        seq: row?.seq ?? 0,
        state: encode(aggregate.definition.state, state)
      }
    ],
    rejection: null
  }
}
