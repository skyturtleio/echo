import * as PgClient from "@effect/sql-pg/PgClient"
import * as Arr from "effect/Array"
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
import { canRead, canWrite, privateScope, type Principal, Scope, ScopeForbidden } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"

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
const protect = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((error) =>
      Schema.is(Wire.ResyncRequired)(error) || Schema.is(Wire.ServerError)(error)
        ? error
        : new Wire.ServerError({ message: "Echo storage operation failed" })
    )
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

export const make = Effect.fn("Echo.Server.make")(function* (options: {
  readonly aggregates: ReadonlyArray<Aggregate.Any>
}) {
  const sql = yield* PgClient.PgClient
  const registry = new Map(Arr.map(options.aggregates, (a) => [a.tag, a]))
  if (registry.size !== options.aggregates.length)
    return yield* new Wire.ServerError({ message: "Duplicate aggregate tag" })
  const aggregate = (tag: string) => registry.get(tag) ?? fail(`Unknown aggregate ${tag}`)
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
            return { epoch: current.epoch, receipt: yield* Schema.decodeUnknownEffect(Wire.Receipt)(prior[0].receipt) }
          }
          const stored = yield* sql<Row>`SELECT aggregate,id,scope,seq,state FROM echo_streams`
          const planned = yield* Effect.gen(function* () {
            const rows: Array<Row> = []
            for (const row of stored)
              rows.push({ ...row, state: yield* decode(aggregate(row.aggregate).definition.state, row.state) })
            const events: Array<{ aggregate: string; id: string; seq: number; payload: unknown }> = []
            const touched = new Map<string, Row>()
            const rejected = (rejection: unknown): Wire.Receipt => ({ _tag: "Rejected", batchId: batch.id, rejection })
            for (const item of batch.commands) {
              const a = registry.get(item.aggregate)
              if (a === undefined) return yield* new Wire.ServerError({ message: "Unknown aggregate" })
              const raw = yield* Schema.decodeUnknownEffect(commandInput)(item.command)
              const c = Option.getOrUndefined(
                Arr.findFirst(a.definition.commands, (candidate) => candidate.tag === raw._tag)
              )
              if (c === undefined) return yield* new Wire.ServerError({ message: "Unknown command" })
              const reads = c.reads
              const command = yield* decode(c, item.command)
              const row = Option.getOrUndefined(
                Arr.findFirst(rows, (r) => r.aggregate === a.tag && r.id === command.target)
              )
              if (row && !canWrite(principal, Scope.make(row.scope))) {
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
                const visible = Arr.filter(rows, (r) => canRead(principal, Scope.make(r.scope)))
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
                try: () => handler(row?.state ?? a.definition.initial, command, { principal, now, read }),
                catch: () => new Wire.ServerError({ message: "Invalid domain decision or undeclared read" })
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
                if (scope === null || !canWrite(principal, scope) || (row && row.scope !== scope)) {
                  return {
                    receipt: rejected(
                      yield* encode(ScopeForbidden, new ScopeForbidden({ scope: privateScope(principal) }))
                    ),
                    events: [],
                    touched: []
                  }
                }
                const next: Row = { aggregate: a.tag, id: command.target, seq, scope, state }
                if (row) rows.splice(rows.indexOf(row), 1, next)
                else rows.push(next)
                touched.set(`${a.tag.length}:${a.tag}${command.target}`, next)
              }
            }
            const receipt: Wire.Receipt = {
              _tag: "Accepted",
              batchId: batch.id,
              seqs: Array.from(touched.values(), (r) => ({ aggregate: r.aggregate, id: r.id, seq: r.seq }))
            }
            return { receipt, events, touched: Array.from(touched.values()) }
          })
          for (const event of planned.events)
            yield* sql`INSERT INTO echo_events VALUES (${event.aggregate},${event.id},${event.seq},${sql.json(event.payload)},${batch.id},${principal},${DateTime.formatIso(now)})`
          for (const row of planned.touched)
            yield* sql`INSERT INTO echo_streams VALUES (${row.aggregate},${row.id},${row.scope},${row.seq},${sql.json(yield* encode(aggregate(row.aggregate).definition.state, row.state))}) ON CONFLICT(aggregate,id) DO UPDATE SET scope=EXCLUDED.scope,seq=EXCLUDED.seq,state=EXCLUDED.state`
          yield* sql`INSERT INTO echo_receipts VALUES (${principal},${request.clientId},${batch.id},${sql.json(batch)},${sql.json(planned.receipt)})`
          if (planned.touched.length > 0) yield* changed(Arr.map(planned.touched, (r) => r.scope))
          return { epoch: current.epoch, receipt: planned.receipt }
        })
      )
      epoch = result.epoch
      receipts.push(result.receipt)
    }
    if (request.batches.length === 0) {
      const current = yield* engine
      yield* checkEpoch(request.epoch, current.epoch)
      epoch = current.epoch
    }
    return { epoch, receipts }
  }, protect)
  const pull = Effect.fn("Echo.Pull")(function* (
    principal: Principal,
    request: Wire.PullRequest
  ): Effect.fn.Return<Wire.PullResponse, unknown> {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`
        const current = yield* engine
        yield* checkEpoch(request.epoch, current.epoch)
        const positions = yield* sql<{
          readonly scope: string
          readonly cursor: string
        }>`SELECT scope,cursor::text FROM echo_scopes WHERE scope=${privateScope(principal)} OR scope LIKE 'catalog:%'`
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
        return { epoch: current.epoch, scopes: result }
      })
    )
  }, protect)

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
  }, protect)
  const resetEpoch = transaction(
    Effect.gen(function* () {
      yield* sql`UPDATE echo_engine SET epoch=gen_random_uuid()::text,cursor=cursor+1`
      yield* sql`SELECT pg_notify('echo_changes','')`
      return (yield* engine).epoch
    })
  ).pipe(protect)
  const changes = () =>
    Stream.unwrap(
      Effect.gen(function* () {
        // LISTEN owns a separate physical connection, never a slot in the application query pool.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- dedicated driver construction; the enclosing stream owns its scope
        const listener = yield* PgClient.makeClient(sql.config).pipe(Effect.provide(Reactivity.layer))
        // LISTEN completes before the initial poke: writes in the connection gap cannot be lost.
        const queue = yield* listener.listen("echo_changes")
        const poke = Effect.map(engine, ({ epoch }): Wire.Change => ({ _tag: "Poke", epoch }))
        const keepalive = Stream.fromEffectRepeat(
          Effect.gen(function* () {
            yield* Effect.sleep("25 seconds")
            return { _tag: "Keepalive", epoch: (yield* engine).epoch } satisfies Wire.Change
          })
        )
        // Listener failure ends the HTTP stream (including keepalives), forcing catchup on reconnect.
        return Stream.concat(
          Stream.fromEffect(poke),
          Stream.merge(
            Stream.mapEffect(Stream.fromQueue(queue), () => poke),
            keepalive
          )
        )
      })
    ).pipe(Stream.mapError(() => new Wire.ServerError({ message: "Changes connection failed" })))
  return { execute, pull, ingestCatalog, resetEpoch, changes }
})

export type Server = Effect.Success<ReturnType<typeof make>>

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
      Changes: () => server.changes()
    })
    const app = yield* RpcServer.toHttpEffect(Wire.EchoRpcs).pipe(
      Effect.provide([handlers, RpcSerialization.layerNdjson])
    )
    return yield* app
  })
