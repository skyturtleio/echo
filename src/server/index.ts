import * as PgClient from "@effect/sql-pg/PgClient"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Str from "effect/String"
import { HttpServerRequest } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { Reactivity } from "effect/reactivity"
import { SqlClient } from "effect/sql"
import * as Aggregate from "../domain/Aggregate.ts"
import type * as Command from "../domain/Command.ts"
import type * as Index from "../domain/Indexes.ts"
import { canRead, canWrite, Grant, privateScope, Principal, Scope, ScopeForbidden } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"
import type * as Action from "./Action.ts"
export * as Action from "./Action.ts"

// Platform boundary: registered domain codecs are synchronous and service-free, as decide/evolve are.
const codec = <S extends Schema.Top>(schema: S) =>
  // oxlint-disable-next-line typescript/consistent-type-assertions
  Schema.toCodecJson(schema) as unknown as Schema.Codec<S["Type"], Schema.Json>
const decode = <S extends Schema.Top>(schema: S, value: unknown) => Schema.decodeUnknownEffect(codec(schema))(value)
const encode = (schema: Schema.Top, value: unknown) => Schema.encodeEffect(codec(schema))(value)
const fail = (message: string): never => {
  throw new Wire.ServerError({ message })
}
const checkEpoch = (expected: string | null, current: string) =>
  expected === null || expected === current ? Effect.void : Effect.fail(new Wire.ResyncRequired({ epoch: current }))
const sanitize = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt
      const error = Option.getOrUndefined(Cause.findErrorOption(cause))
      return Effect.fail(
        Schema.is(Wire.ResyncRequired)(error)
          ? error
          : new Wire.ServerError({ message: "Echo storage operation failed" })
      )
    })
  )

/** Run explicitly during application migration, never automatically during request handling. */
export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE IF NOT EXISTS echo_engine (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), epoch text NOT NULL, cursor bigint NOT NULL DEFAULT 0)`
  yield* sql`INSERT INTO echo_engine(singleton,epoch) VALUES(true,gen_random_uuid()::text) ON CONFLICT DO NOTHING`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_scopes (scope text PRIMARY KEY,cursor bigint NOT NULL)`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_streams (aggregate text NOT NULL,id text NOT NULL,scope text NOT NULL,seq integer NOT NULL,state jsonb NOT NULL,PRIMARY KEY(aggregate,id))`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_events (aggregate text NOT NULL,id text NOT NULL,seq integer NOT NULL,payload jsonb NOT NULL,batch_id text NOT NULL,principal text NOT NULL,at timestamptz NOT NULL,PRIMARY KEY(aggregate,id,seq))`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_receipts (principal text NOT NULL,client_id text NOT NULL,batch_id text NOT NULL,request jsonb NOT NULL,receipt jsonb NOT NULL,PRIMARY KEY(principal,client_id,batch_id))`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_catalog_releases (scope text NOT NULL,release text NOT NULL,cursor bigint NOT NULL,PRIMARY KEY(scope,release))`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_grants (scope text NOT NULL,principal text NOT NULL,role text NOT NULL CHECK(role IN ('reader','writer','owner')),PRIMARY KEY(scope,principal))`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_grant_events (position bigserial PRIMARY KEY,scope text NOT NULL,subject text NOT NULL,role text,actor text NOT NULL,batch_id text NOT NULL,at timestamptz NOT NULL)`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_imports (id text PRIMARY KEY,request jsonb NOT NULL)`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_connections (id text PRIMARY KEY,principal text NOT NULL,expires_at timestamptz NOT NULL)`
  yield* sql`CREATE TABLE IF NOT EXISTS echo_presence (connection_id text NOT NULL REFERENCES echo_connections(id) ON DELETE CASCADE,room text NOT NULL,client_id text NOT NULL,state jsonb NOT NULL,expires_at timestamptz NOT NULL,PRIMARY KEY(connection_id,room,client_id))`
})

interface Row {
  readonly aggregate: string
  readonly id: string
  readonly scope: string
  readonly seq: number
  readonly state: unknown
}
interface Engine {
  readonly epoch: string
  readonly cursor: string
}
const tagged = Schema.Struct({ _tag: Schema.String })
const commandInput = Schema.Struct({ _tag: Schema.String, target: Schema.String })

/** Trusted catalog ingestion replaces one complete catalog, atomically. */
export interface Catalog {
  readonly scope: string
  readonly release: string
  readonly rows: ReadonlyArray<{ readonly aggregate: string; readonly id: string; readonly state: unknown }>
}

export interface ImportedStream {
  readonly aggregate: string
  readonly id: string
  readonly events: ReadonlyArray<unknown>
}
export const importedStream = <A extends Aggregate.Any>(
  a: A,
  id: Aggregate.IdOf<A>,
  events: ReadonlyArray<Aggregate.EventInstance<A>>
): ImportedStream => ({ aggregate: a.tag, id, events })

export interface Options {
  readonly aggregates: ReadonlyArray<Aggregate.Any>
  readonly actions?: ReadonlyArray<Action.Action>
  readonly onFailure?: (operation: string, cause: Cause.Cause<unknown>) => Effect.Effect<void>
  readonly onRejected?: (outcome: RejectedOutcome) => Effect.Effect<void>
}
/** Best-effort, post-commit diagnostic; never called for receipt replay or transaction retries. */
export interface RejectedOutcome {
  readonly operation: "Batch" | "Action"
  readonly rejectionTag: string | null
}
export interface ImportRequest {
  readonly id: string
  readonly principal: Principal
  readonly streams: ReadonlyArray<ImportedStream>
  readonly grants?: ReadonlyArray<Grant>
}
export type ServerFailure = Wire.ServerError | Wire.ResyncRequired
export interface Server {
  readonly execute: (
    principal: Principal,
    request: Wire.ExecuteRequest
  ) => Effect.Effect<Wire.ExecuteResponse, ServerFailure>
  readonly pull: (principal: Principal, request: Wire.PullRequest) => Effect.Effect<Wire.PullResponse, ServerFailure>
  readonly get: <A extends Aggregate.Any>(
    principal: Principal,
    aggregate: A,
    id: Aggregate.IdOf<A>
  ) => Effect.Effect<Aggregate.StateOf<A> | null, ServerFailure>
  readonly ingestCatalog: (catalog: Catalog) => Effect.Effect<void, ServerFailure>
  readonly importStreams: (request: ImportRequest) => Effect.Effect<void, ServerFailure>
  readonly resetEpoch: Effect.Effect<string, ServerFailure>
  readonly changes: (principal: Principal) => Stream.Stream<Wire.Change, Wire.ServerError>
  readonly presence: (principal: Principal, request: Wire.PresenceRequest) => Effect.Effect<void, ServerFailure>
}

export const make = Effect.fn("Echo.Server.make")(function* (
  options: Options
): Effect.fn.Return<Server, Wire.ServerError, PgClient.PgClient> {
  const sql = yield* PgClient.PgClient
  const protect =
    (operation: string) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.onError((cause) =>
          Effect.suspend(() => options.onFailure?.(operation, cause) ?? Effect.void).pipe(
            Effect.catchCause(() => Effect.void)
          )
        ),
        sanitize
      )
  const registry = new Map(Arr.map(options.aggregates, (a) => [a.tag, a]))
  if (registry.size !== options.aggregates.length)
    return yield* new Wire.ServerError({ message: "Duplicate aggregate tag" })
  const aggregate = (tag: string) => registry.get(tag) ?? fail(`Unknown aggregate ${tag}`)
  const actions = new Map(Arr.map(options.actions ?? [], (action) => [action.name, action]))
  if (actions.size !== (options.actions ?? []).length)
    return yield* new Wire.ServerError({ message: "Duplicate action" })
  const loadGrants = Effect.gen(function* () {
    return yield* Schema.decodeUnknownEffect(Schema.Array(Grant))(
      yield* sql`SELECT scope,principal,role FROM echo_grants`
    )
  })
  const transaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`
          // Even a rejected/no-op batch must invalidate a waiting writer's old MVCC snapshot.
          yield* sql`UPDATE echo_engine SET cursor=cursor`
          return yield* body
        })
      )
      .pipe(
        Effect.retry({
          times: 8,
          while: (error) => {
            if (!Predicate.hasProperty(error, "reason") || !Predicate.hasProperty(error.reason, "cause")) return false
            const cause = error.reason.cause
            return Predicate.hasProperty(cause, "code") && (cause.code === "40001" || cause.code === "40P01")
          }
        })
      )
  const engine = Effect.gen(function* () {
    const rows = yield* sql<Engine>`SELECT epoch,cursor::text FROM echo_engine`
    return rows[0] ?? fail("Run Echo migrate before serving requests")
  })
  const changed = Effect.fnUntraced(function* (scopes: ReadonlyArray<string>) {
    yield* sql`UPDATE echo_engine SET cursor=cursor+1`
    for (const scope of new Set(scopes))
      yield* sql`INSERT INTO echo_scopes(scope,cursor) SELECT ${scope},cursor FROM echo_engine ON CONFLICT(scope) DO UPDATE SET cursor=EXCLUDED.cursor`
    yield* sql`SELECT pg_notify('echo_changes','')`
  })
  const execute = Effect.fn("Echo.Execute")(function* (principal: Principal, request: Wire.ExecuteRequest) {
    const now = yield* DateTime.now
    const receipts: Array<Wire.Receipt> = []
    let epoch = ""
    for (const batch of request.batches) {
      const result = yield* transaction(
        Effect.gen(function* () {
          const current = yield* engine
          yield* checkEpoch(epoch === "" ? request.epoch : epoch, current.epoch)
          const prior = yield* sql<{
            readonly receipt: unknown
            readonly same: boolean
          }>`SELECT receipt, request = ${sql.json(batch)} AS same FROM echo_receipts WHERE principal=${principal} AND client_id=${request.clientId} AND batch_id=${batch.id}`
          if (prior[0]) {
            if (!prior[0].same)
              return yield* new Wire.ServerError({ message: "Batch id reused with different commands" })
            return {
              epoch: current.epoch,
              receipt: yield* Schema.decodeUnknownEffect(Wire.Receipt)(prior[0].receipt),
              fresh: false
            }
          }
          let grants = yield* loadGrants
          const grantChanges: Array<Exclude<Action.Operation, { readonly kind: "command" }>> = []
          const changedScopes = new Set<string>()
          const planned = yield* Effect.gen(function* () {
            const events: Array<{ aggregate: string; id: string; seq: number; payload: unknown }> = []
            const touched = new Map<string, Row>()
            const rejected = (rejection: unknown): Wire.Receipt => ({ _tag: "Rejected", batchId: batch.id, rejection })
            let operations: ReadonlyArray<Action.Operation> = Arr.map(batch.commands, (item) => ({
              kind: "command",
              ...item
            }))
            if (batch.action !== undefined) {
              if (batch.commands.length !== 0)
                return yield* new Wire.ServerError({ message: "Action cannot include commands" })
              const action = actions.get(batch.action.name)
              if (action === undefined) return yield* new Wire.ServerError({ message: "Unknown action" })
              const stored = yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams WHERE ${sql.in(
                "aggregate",
                Arr.map(action.reads, (a) => a.tag)
              )}`
              const rows: Array<Row> = []
              for (const row of stored)
                rows.push({ ...row, state: yield* decode(aggregate(row.aggregate).definition.state, row.state) })
              const scan = <A extends Aggregate.Any>(
                ref: A
              ): ReadonlyArray<{ readonly id: Aggregate.IdOf<A>; readonly state: Aggregate.StateOf<A> }> => {
                if (!action.reads.includes(ref)) return fail("Undeclared action read")
                return Arr.map(
                  Arr.filter(rows, (row) => row.aggregate === ref.tag),
                  (row) => ({ id: ref.definition.id.make(row.id), state: row.state })
                )
              }
              const plan = yield* Effect.try({
                try: () =>
                  action.decide(batch.action?.input, {
                    principal,
                    now,
                    grants,
                    read: {
                      scan,
                      get: (ref, id) =>
                        Option.getOrUndefined(Arr.findFirst(scan(ref), (row) => row.id === id))?.state ?? null
                    }
                  }),
                catch: (cause) => new Cause.UnknownError(cause)
              })
              if ("rejection" in plan) return { receipt: rejected(plan.rejection), events: [], touched: [] }
              operations = plan.operations
            }
            for (const item of operations) {
              if (item.kind !== "command") {
                const scope = item.kind === "grant" ? item.grant.scope : item.scope
                const subject = item.kind === "grant" ? item.grant.principal : item.principal
                if (Str.startsWith("user:")(scope) || Str.startsWith("catalog:")(scope))
                  return yield* new Wire.ServerError({ message: "Intrinsic scope grants cannot change" })
                grants = Arr.filter(grants, (g) => g.scope !== scope || g.principal !== subject)
                if (item.kind === "grant") grants = [...grants, yield* Schema.decodeUnknownEffect(Grant)(item.grant)]
                grantChanges.push(item)
                changedScopes.add(scope)
                continue
              }
              const a = registry.get(item.aggregate)
              if (a === undefined) return yield* new Wire.ServerError({ message: "Unknown aggregate" })
              const raw = yield* Schema.decodeUnknownEffect(commandInput)(item.command)
              const c = Option.getOrUndefined(
                Arr.findFirst(a.definition.commands, (candidate) => candidate.tag === raw._tag)
              )
              if (c === undefined) return yield* new Wire.ServerError({ message: "Unknown command" })
              if (c.authority === "server" && batch.action === undefined)
                return { receipt: rejected({ _tag: "ServerOnly" }), events: [], touched: [] }
              const reads = c.reads
              const command = yield* decode(
                c,
                batch.action === undefined ? item.command : yield* encode(c, item.command)
              )
              const readTags = new Set<string>()
              for (const ref of reads) {
                const owner = ref.kind === "Aggregate" ? Option.some(ref) : Aggregate.ofIndex(ref)
                if (Option.isSome(owner)) readTags.add(owner.value.tag)
              }
              // Targets include hidden rows for authorization; declared reads include only visible scopes.
              // Load per command so an earlier rejection still wins over a malformed later command.
              const stored = yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams
                WHERE (aggregate=${a.tag} AND id=${command.target})
                   OR (${sql.in("aggregate", [...readTags])} AND (scope=${privateScope(principal)} OR scope LIKE 'catalog:%' OR ${sql.in(
                     "scope",
                     Arr.map(
                       Arr.filter(grants, (g) => g.principal === principal),
                       (g) => g.scope
                     )
                   )}))`
              const rows: Array<Row> = []
              for (const storedRow of stored) {
                if (touched.has(`${storedRow.aggregate.length}:${storedRow.aggregate}${storedRow.id}`)) continue
                rows.push({
                  ...storedRow,
                  state: yield* decode(aggregate(storedRow.aggregate).definition.state, storedRow.state)
                })
              }
              // Earlier commands have not been persisted yet; their state wins over the transaction snapshot.
              rows.push(...touched.values())
              const row = Option.getOrUndefined(
                Arr.findFirst(rows, (r) => r.aggregate === a.tag && r.id === command.target)
              )
              if (row && c.authority !== "server" && !canWrite(principal, Scope.make(row.scope), grants)) {
                return {
                  receipt: rejected(
                    yield* encode(ScopeForbidden, new ScopeForbidden({ scope: privateScope(principal) }))
                  ),
                  events: [],
                  touched: []
                }
              }
              function read<A extends Aggregate.Any>(ref: A, id: Aggregate.IdOf<A>): Aggregate.StateOf<A> | null
              function read<I extends Index.Any>(ref: I, value: Index.Value<I>): ReadonlyArray<Index.Entry<I>>
              function read(ref: Command.Read, value: unknown): unknown {
                if (!reads.includes(ref)) return fail(`${raw._tag} used an undeclared read`)
                const visible = Arr.filter(rows, (r) => canRead(principal, Scope.make(r.scope), grants))
                if (ref.kind === "Aggregate")
                  return (
                    Option.getOrUndefined(Arr.findFirst(visible, (r) => r.aggregate === ref.tag && r.id === value))
                      ?.state ?? null
                  )
                const owner = Aggregate.ofIndex(ref)
                if (Option.isNone(owner)) return []
                const values = ref.fields.length === 1 ? [value] : value
                if (!Arr.isArray(values)) return fail("Index read requires a tuple")
                return Arr.map(
                  Arr.filter(
                    visible,
                    (r) =>
                      r.aggregate === owner.value.tag &&
                      Arr.every(
                        ref.fields,
                        (field, i) => Predicate.hasProperty(r.state, field) && Equal.equals(r.state[field], values[i])
                      )
                  ),
                  (r) => ({ id: r.id, state: r.state })
                )
              }
              const handler = a.definition.decide[c.tag] ?? fail("Missing decide handler")
              const decision = yield* Effect.try({
                try: () =>
                  handler(row?.state ?? a.definition.initial, command, {
                    principal,
                    now,
                    read,
                    grants: Arr.filter(grants, (g) => g.principal === principal)
                  }),
                catch: (cause) => new Cause.UnknownError(cause)
              })
              if (!Arr.isArray(decision)) {
                const tag = (yield* Schema.decodeUnknownEffect(tagged)(decision))._tag
                const rejection =
                  Option.getOrUndefined(Arr.findFirst(c.rejects, (r) => r.tag === tag)) ?? fail("Undeclared rejection")
                return { receipt: rejected(yield* encode(rejection, decision)), events: [], touched: [] }
              }
              let state = row?.state ?? a.definition.initial
              let seq = row?.seq ?? 0
              for (const event of decision) {
                const tag = (yield* Schema.decodeUnknownEffect(tagged)(event))._tag
                const eventSchema =
                  Option.getOrUndefined(Arr.findFirst(c.events, (e) => e.tag === tag)) ?? fail("Undeclared event")
                const evolve = a.definition.evolve[tag] ?? fail("Missing evolve handler")
                const payload = yield* encode(eventSchema, event)
                state = evolve(state, yield* decode(eventSchema, payload))
                seq++
                events.push({ aggregate: a.tag, id: command.target, seq, payload })
              }
              if (decision.length > 0) {
                const scope = a.definition.scope(state)
                if (scope === null || (c.authority !== "server" && !canWrite(principal, scope, grants))) {
                  return {
                    receipt: rejected(
                      yield* encode(ScopeForbidden, new ScopeForbidden({ scope: privateScope(principal) }))
                    ),
                    events: [],
                    touched: []
                  }
                }
                const next: Row = { aggregate: a.tag, id: command.target, seq, scope, state }
                if (row) changedScopes.add(row.scope)
                changedScopes.add(scope)
                touched.set(`${a.tag.length}:${a.tag}${command.target}`, next)
              }
            }
            const receipt: Wire.Receipt = {
              _tag: "Accepted",
              batchId: batch.id,
              revision: Number(current.cursor) + 1,
              seqs: Array.from(touched.values(), (r) => ({ aggregate: r.aggregate, id: r.id, seq: r.seq }))
            }
            return { receipt, events, touched: Array.from(touched.values()) }
          })
          if (planned.receipt._tag === "Accepted") {
            for (const change of grantChanges) {
              const scope = change.kind === "grant" ? change.grant.scope : change.scope
              const subject = change.kind === "grant" ? change.grant.principal : change.principal
              const role = change.kind === "grant" ? change.grant.role : null
              if (role === null) {
                yield* sql`DELETE FROM echo_grants WHERE scope=${scope} AND principal=${subject}`
                yield* sql`DELETE FROM echo_presence p USING echo_connections c WHERE p.connection_id=c.id AND p.room=${scope} AND c.principal=${subject}`
              } else
                yield* sql`INSERT INTO echo_grants VALUES(${scope},${subject},${role}) ON CONFLICT(scope,principal) DO UPDATE SET role=EXCLUDED.role`
              yield* sql`INSERT INTO echo_grant_events(scope,subject,role,actor,batch_id,at) VALUES(${scope},${subject},${role},${principal},${batch.id},${DateTime.formatIso(now)})`
            }
          }
          for (const event of planned.events)
            yield* sql`INSERT INTO echo_events VALUES (${event.aggregate},${event.id},${event.seq},${sql.json(event.payload)},${batch.id},${principal},${DateTime.formatIso(now)})`
          for (const row of planned.touched)
            yield* sql`INSERT INTO echo_streams VALUES (${row.aggregate},${row.id},${row.scope},${row.seq},${sql.json(yield* encode(aggregate(row.aggregate).definition.state, row.state))}) ON CONFLICT(aggregate,id) DO UPDATE SET scope=EXCLUDED.scope,seq=EXCLUDED.seq,state=EXCLUDED.state`
          yield* sql`INSERT INTO echo_receipts VALUES (${principal},${request.clientId},${batch.id},${sql.json(batch)},${sql.json(planned.receipt)})`
          if (planned.receipt._tag === "Accepted") yield* changed([...changedScopes])
          return { epoch: current.epoch, receipt: planned.receipt, fresh: true }
        })
      )
      epoch = result.epoch
      receipts.push(result.receipt)
      if (result.fresh && result.receipt._tag === "Rejected") {
        const rejection = result.receipt.rejection
        yield* Effect.suspend(
          () =>
            options.onRejected?.({
              operation: batch.action === undefined ? "Batch" : "Action",
              rejectionTag: Schema.is(tagged)(rejection) ? rejection._tag : null
            }) ?? Effect.void
        ).pipe(Effect.ignoreCause)
      }
    }
    if (request.batches.length === 0) {
      const current = yield* engine
      yield* checkEpoch(request.epoch, current.epoch)
      epoch = current.epoch
    }
    return { epoch, receipts }
  }, protect("Execute"))
  const pull = Effect.fn("Echo.Pull")(function* (
    principal: Principal,
    request: Wire.PullRequest
  ): Effect.fn.Return<Wire.PullResponse, unknown> {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`
        const current = yield* engine
        yield* checkEpoch(request.epoch, current.epoch)
        const grants = Arr.filter(yield* loadGrants, (grant) => grant.principal === principal)
        const positions = yield* sql<{
          readonly scope: string
          readonly cursor: string
        }>`SELECT scope,cursor::text FROM echo_scopes WHERE scope=${privateScope(principal)} OR scope LIKE 'catalog:%' OR ${sql.in(
          "scope",
          Arr.map(grants, (g) => g.scope)
        )}`
        const scopes = new Map([
          [String(privateScope(principal)), 0],
          ...Arr.map(positions, (r): [string, number] => [r.scope, Number(r.cursor)])
        ])
        const result: Array<Wire.PullResponse["scopes"][number]> = []
        for (const [scope, cursor] of scopes) {
          const reset = request.cursors[scope] !== cursor
          const rows = reset
            ? yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams WHERE scope=${scope}`
            : []
          result.push({
            scope,
            cursor,
            reset,
            entries: Arr.map(rows, (r) => ({
              _tag: "Upsert",
              aggregate: r.aggregate,
              id: r.id,
              seq: r.seq,
              state: r.state
            }))
          })
        }
        return { epoch: current.epoch, revision: Number(current.cursor), grants, scopes: result }
      })
    )
  }, protect("Pull"))

  const ingestCatalog = Effect.fn("Echo.ingestCatalog")(function* (catalog: Catalog) {
    return yield* transaction(
      Effect.gen(function* () {
        if (!Str.startsWith("catalog:")(catalog.scope))
          return yield* new Wire.ServerError({ message: "Catalog scope required" })
        const rows = yield* Effect.forEach(
          catalog.rows,
          Effect.fnUntraced(function* (row) {
            const a = aggregate(row.aggregate)
            yield* decode(a.definition.id, row.id)
            const state = yield* decode(a.definition.state, row.state)
            if (a.definition.scope(state) !== catalog.scope)
              return yield* new Wire.ServerError({ message: "Catalog row scope mismatch" })
            return { ...row, state: yield* encode(a.definition.state, state) }
          })
        )
        const existing = yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams`
        for (const row of rows)
          if (Arr.some(existing, (r) => r.aggregate === row.aggregate && r.id === row.id && r.scope !== catalog.scope))
            return yield* new Wire.ServerError({ message: "Catalog id belongs to another scope" })
        yield* sql`DELETE FROM echo_streams WHERE scope=${catalog.scope}`
        for (const row of rows)
          yield* sql`INSERT INTO echo_streams VALUES (${row.aggregate},${row.id},${catalog.scope},${(Option.getOrUndefined(Arr.findFirst(existing, (r) => r.aggregate === row.aggregate && r.id === row.id))?.seq ?? 0) + 1},${sql.json(row.state)})`
        yield* changed([catalog.scope])
        yield* sql`INSERT INTO echo_catalog_releases(scope,release,cursor) SELECT ${catalog.scope},${catalog.release},cursor FROM echo_engine`
      })
    )
  }, protect("ingestCatalog"))
  /** Trusted only: never mounted by httpApp. An id permanently identifies one exact import. */
  const importStreams = Effect.fn("Echo.importStreams")(function* (input: {
    readonly id: string
    readonly principal: Principal
    readonly streams: ReadonlyArray<ImportedStream>
    readonly grants?: ReadonlyArray<Grant>
  }) {
    const now = yield* DateTime.now
    // Composes with an application's migration transaction; do not change its isolation level.
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`UPDATE echo_engine SET cursor=cursor`
        const canonical: Array<ImportedStream> = []
        for (const stream of input.streams) {
          const a = aggregate(stream.aggregate)
          yield* decode(a.definition.id, stream.id)
          if (stream.events.length === 0) return yield* new Wire.ServerError({ message: "Import needs events" })
          const events: Array<unknown> = []
          for (const value of stream.events) {
            const tag = (yield* Schema.decodeUnknownEffect(tagged)(value))._tag
            const schema = Option.getOrUndefined(
              Arr.findFirst(
                [...(a.definition.events ?? []), ...Arr.flatMap(a.definition.commands, (c) => c.events)],
                (e) => e.tag === tag
              )
            )
            if (schema === undefined) return yield* new Wire.ServerError({ message: "Unregistered import event" })
            events.push(yield* encode(schema, value))
          }
          canonical.push({ ...stream, events })
        }
        const request = { ...input, streams: canonical }
        const prior = yield* sql<{
          readonly same: boolean
        }>`SELECT request=${sql.json(request)} AS same FROM echo_imports WHERE id=${input.id}`
        if (prior[0]) {
          if (!prior[0].same) return yield* new Wire.ServerError({ message: "Import identity conflict" })
          return
        }
        const scopes = new Set<string>()
        for (const stream of canonical) {
          const exists = yield* sql`SELECT 1 FROM echo_streams WHERE aggregate=${stream.aggregate} AND id=${stream.id}`
          if (exists.length > 0) return yield* new Wire.ServerError({ message: "Import stream already exists" })
          const a = aggregate(stream.aggregate)
          let state = a.definition.initial
          let seq = 0
          for (const payload of stream.events) {
            const tag = (yield* Schema.decodeUnknownEffect(tagged)(payload))._tag
            const schema =
              Option.getOrUndefined(
                Arr.findFirst(
                  [...(a.definition.events ?? []), ...Arr.flatMap(a.definition.commands, (c) => c.events)],
                  (e) => e.tag === tag
                )
              ) ?? fail("Unregistered import event")
            const event = yield* decode(schema, payload)
            state = (a.definition.evolve[tag] ?? fail("Missing evolve handler"))(state, event)
            seq++
            yield* sql`INSERT INTO echo_events VALUES(${a.tag},${stream.id},${seq},${sql.json(payload)},${input.id},${input.principal},${DateTime.formatIso(now)})`
          }
          const scope = a.definition.scope(state)
          if (scope === null || Str.startsWith("catalog:")(scope))
            return yield* new Wire.ServerError({ message: "Import requires noncatalog scope" })
          yield* sql`INSERT INTO echo_streams VALUES(${a.tag},${stream.id},${scope},${seq},${sql.json(yield* encode(a.definition.state, state))})`
          scopes.add(scope)
        }
        for (const value of input.grants ?? []) {
          const grant = yield* Schema.decodeUnknownEffect(Grant)(value)
          if (Str.startsWith("user:")(grant.scope) || Str.startsWith("catalog:")(grant.scope))
            return yield* new Wire.ServerError({ message: "Intrinsic scope grants cannot change" })
          yield* sql`INSERT INTO echo_grants VALUES(${grant.scope},${grant.principal},${grant.role})`
          yield* sql`INSERT INTO echo_grant_events(scope,subject,role,actor,batch_id,at) VALUES(${grant.scope},${grant.principal},${grant.role},${input.principal},${input.id},${DateTime.formatIso(now)})`
          scopes.add(grant.scope)
        }
        yield* sql`INSERT INTO echo_imports VALUES(${input.id},${sql.json(request)})`
        if (scopes.size > 0) yield* changed([...scopes])
      })
    )
  }, protect("importStreams"))
  const resetEpoch = transaction(
    Effect.gen(function* () {
      yield* sql`UPDATE echo_engine SET epoch=gen_random_uuid()::text,cursor=cursor+1`
      yield* sql`SELECT pg_notify('echo_changes','')`
      return (yield* engine).epoch
    })
  ).pipe(protect("resetEpoch"))
  const get: Server["get"] = Effect.fn("Echo.get")(function* <A extends Aggregate.Any>(
    principal: Principal,
    ref: A,
    id: Aggregate.IdOf<A>
  ): Effect.fn.Return<Aggregate.StateOf<A> | null, unknown> {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`
        const grants = yield* loadGrants
        const rows =
          yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams WHERE aggregate=${ref.tag} AND id=${id}`
        const row = rows[0]
        if (row === undefined) return null
        const state = yield* decode(ref.definition.state, row.state)
        const scope = ref.definition.scope(state)
        return scope !== null && canRead(principal, scope, grants) ? state : null
      })
    )
  }, protect("get"))
  const presence = Effect.fn("Echo.Presence")(function* (principal: Principal, request: Wire.PresenceRequest) {
    if ((yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(request.state)).length > 8192)
      return yield* new Wire.ServerError({ message: "Presence state too large" })
    yield* transaction(
      Effect.gen(function* () {
        const connection =
          yield* sql`SELECT 1 FROM echo_connections WHERE id=${request.connectionId} AND principal=${principal} AND expires_at>now()`
        if (connection.length === 0) return yield* new Wire.ServerError({ message: "Presence connection expired" })
        if (!canRead(principal, Scope.make(request.room), yield* loadGrants))
          return yield* new Wire.ServerError({ message: "Presence scope forbidden" })
        if (request.state === null)
          yield* sql`DELETE FROM echo_presence WHERE connection_id=${request.connectionId} AND room=${request.room} AND client_id=${request.clientId}`
        else {
          yield* sql`DELETE FROM echo_presence WHERE expires_at<now()`
          const rows =
            yield* sql`SELECT 1 FROM echo_presence WHERE connection_id=${request.connectionId} AND NOT (room=${request.room} AND client_id=${request.clientId})`
          if (rows.length >= 64) return yield* new Wire.ServerError({ message: "Presence room limit" })
          yield* sql`INSERT INTO echo_presence VALUES(${request.connectionId},${request.room},${request.clientId},${sql.json(request.state)},now()+interval '60 seconds') ON CONFLICT(connection_id,room,client_id) DO UPDATE SET state=EXCLUDED.state,expires_at=EXCLUDED.expires_at`
        }
        yield* sql`SELECT pg_notify('echo_changes','presence')`
      })
    )
  }, protect("Presence"))
  const changes = (principal: Principal) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // LISTEN owns a separate physical connection, never a slot in the application query pool.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- dedicated driver construction; the enclosing stream owns its scope
        const listener = yield* PgClient.makeClient(sql.config).pipe(Effect.provide(Reactivity.layer))
        // LISTEN completes before the initial poke: writes in the connection gap cannot be lost.
        const queue = yield* listener.listen("echo_changes")
        const connectionId = yield* Effect.acquireRelease(
          Effect.gen(function* () {
            yield* sql`DELETE FROM echo_connections WHERE expires_at<now()`
            const rows = yield* sql<{
              readonly id: string
            }>`INSERT INTO echo_connections SELECT gen_random_uuid()::text,${principal},now()+interval '60 seconds' RETURNING id`
            return rows[0]?.id ?? fail("Connection creation failed")
          }),
          (id) =>
            Effect.gen(function* () {
              yield* sql`DELETE FROM echo_connections WHERE id=${id}`
              yield* sql`SELECT pg_notify('echo_changes','presence')`
            }).pipe(Effect.orDie)
        )
        const snapshot = (tag: "Poke" | "Keepalive") =>
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
              yield* sql`UPDATE echo_connections SET expires_at=now()+interval '60 seconds' WHERE id=${connectionId}`
              const grants = yield* loadGrants
              const members = yield* sql<{
                readonly room: string
                readonly principal: string
                readonly clientId: string
                readonly state: Schema.Json
              }>`SELECT p.room,c.principal,p.client_id AS "clientId",p.state FROM echo_presence p JOIN echo_connections c ON c.id=p.connection_id WHERE c.expires_at>now() AND p.expires_at>now()`
              const visible = Arr.filter(
                members,
                (member) =>
                  canRead(principal, Scope.make(member.room), grants) &&
                  canRead(Principal.make(member.principal), Scope.make(member.room), grants)
              )
              return { _tag: tag, epoch: (yield* engine).epoch, connectionId, presence: visible } satisfies Wire.Change
            })
          )
        const poke = snapshot("Poke")
        const keepalive = Stream.fromEffectRepeat(
          Effect.gen(function* () {
            yield* Effect.sleep("25 seconds")
            return yield* snapshot("Keepalive")
          })
        )
        // Listener failure ends the HTTP stream (including keepalives), forcing catchup on reconnect.
        return Stream.concat(
          Stream.fromEffect(poke),
          Stream.merge(
            Stream.mapEffect(Stream.fromQueue(queue), (notification) =>
              snapshot(notification.payload === "presence" ? "Keepalive" : "Poke")
            ),
            keepalive
          )
        )
      })
    ).pipe(Stream.mapError(() => new Wire.ServerError({ message: "Changes connection failed" })))
  return { execute, pull, get, ingestCatalog, importStreams, resetEpoch, changes, presence }
})

/** Mount this HTTP effect at the application's sync route; authentication runs for every request. */
export const httpApp = <E, R>(
  server: Server,
  authenticate: (request: HttpServerRequest.HttpServerRequest) => Effect.Effect<Principal, E, R>
) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const principal = yield* authenticate(request)
    const handlers = Wire.EchoRpcs.toLayer({
      Execute: (input) => server.execute(principal, input),
      Pull: (input) => server.pull(principal, input),
      Changes: () => server.changes(principal),
      Presence: (input) => server.presence(principal, input)
    })
    const app = yield* RpcServer.toHttpEffect(Wire.EchoRpcs).pipe(
      Effect.provide([handlers, RpcSerialization.layerNdjson])
    )
    return yield* app
  })
