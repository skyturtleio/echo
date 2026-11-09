import * as PgClient from "@effect/sql-pg/PgClient"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Str from "effect/String"
import { Reactivity } from "effect/reactivity"
import { Grant, Principal, privateScope } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"

const scopesCodec = Schema.fromJsonString(Schema.Array(Schema.String))
const decodeScopes = Schema.decodeUnknownOption(scopesCodec)
// oxlint-disable-next-line effecttsgo/schema-sync -- pure encoding of engine-owned string arrays; no unknown input or service requirements
const encodeScopes = Schema.encodeSync(scopesCodec)
const unavailable = () => new Wire.ServerError({ message: "Changes connection failed" })

/** A hint, never an event log. Oversized batches conservatively wake every reader. */
export const notification = (scopes: ReadonlyArray<string>): string => {
  const payload = encodeScopes(scopes)
  return scopes.length === 0 || scopes.length > 256 || new TextEncoder().encode(payload).length > 7000 ? "" : payload
}

interface Subscriber {
  readonly principal: Principal
  readonly wake: Queue.Queue<void, Wire.ServerError>
  poke: boolean
  frame: Wire.Change | undefined
}

/** One application-scoped listener and refresh worker, independent of HTTP reader count. */
export const make = Effect.fnUntraced(function* (sql: PgClient.PgClient) {
  const subscribers = new Map<string, Subscriber>()
  const wake = yield* Queue.sliding<void>(1)
  const started = yield* Deferred.make<void, Wire.ServerError>()
  let ready = false
  let generation = 0
  let all = false
  let maintenance = false
  let scopes = new Set<string>()

  const signal = (payload: string) => {
    if (payload !== "presence") {
      const decoded = decodeScopes(payload)
      if (Option.isNone(decoded)) all = true
      else if (!all) {
        for (const scope of decoded.value) scopes.add(scope)
        if (scopes.size > 256) all = true
      }
      if (all) scopes.clear()
    }
    Queue.offerUnsafe(wake, undefined)
  }
  const disconnect = Effect.gen(function* () {
    ready = false
    for (const subscriber of subscribers.values()) Queue.failCauseUnsafe(subscriber.wake, Cause.fail(unavailable()))
    subscribers.clear()
    // Also unblock construction if acquisition defects before the first successful LISTEN.
    yield* Deferred.fail(started, unavailable())
  })

  const refresh = Effect.gen(function* () {
    // Take dirty state before the transaction. Arrivals during SQL remain pending for the next pass.
    const readers = [...subscribers]
    const dirtyAll = all
    const dirtyScopes = scopes
    const renew = maintenance
    all = false
    maintenance = false
    scopes = new Set()
    if (readers.length === 0) return
    const snapshot = yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
        const ids = Arr.map(readers, ([id]) => id)
        const live = yield* renew
          ? sql<{
              readonly id: string
            }>`UPDATE echosync_connections SET expires_at=now()+interval '60 seconds' WHERE ${sql.in("id", ids)} AND expires_at>now() RETURNING id`
          : sql<{
              readonly id: string
            }>`SELECT id FROM echosync_connections WHERE ${sql.in("id", ids)} AND expires_at>now()`
        const grants = yield* Schema.decodeUnknownEffect(Schema.Array(Grant))(
          yield* sql`SELECT scope,principal,role FROM echosync_grants`
        )
        const members = yield* Schema.decodeUnknownEffect(Schema.Array(Wire.PresenceMember))(
          yield* sql`SELECT p.room,c.principal,p.client_id AS "clientId",p.state FROM echosync_presence p JOIN echosync_connections c ON c.id=p.connection_id WHERE c.expires_at>now() AND p.expires_at>now()`
        )
        const engine = yield* sql<{ readonly epoch: string }>`SELECT epoch FROM echosync_engine`
        const epoch = engine[0]?.epoch
        if (epoch === undefined) return yield* unavailable()
        return { live: new Set(Arr.map(live, (row) => row.id)), grants, members, epoch }
      })
    )
    const grants = new Map<string, Set<string>>()
    for (const grant of snapshot.grants) {
      let granted = grants.get(grant.principal)
      if (granted === undefined) {
        granted = new Set()
        grants.set(grant.principal, granted)
      }
      granted.add(grant.scope)
    }
    const readable = (principal: string, scope: string) =>
      scope === privateScope(Principal.make(principal)) ||
      Str.startsWith("catalog:")(scope) ||
      (!Str.startsWith("user:")(scope) && grants.get(principal)?.has(scope) === true)
    const members = Arr.filter(snapshot.members, (member) => readable(member.principal, member.room))
    const visible = new Map<Principal, ReadonlyArray<Wire.PresenceMember>>()
    for (const [id, subscriber] of readers) {
      if (subscribers.get(id) !== subscriber) continue
      if (!snapshot.live.has(id)) {
        Queue.failCauseUnsafe(subscriber.wake, Cause.fail(unavailable()))
        subscribers.delete(id)
        continue
      }
      let presence = visible.get(subscriber.principal)
      if (presence === undefined) {
        presence = Arr.filter(members, (member) => readable(subscriber.principal, member.room))
        visible.set(subscriber.principal, presence)
      }
      subscriber.poke ||= dirtyAll || Arr.some([...dirtyScopes], (scope) => readable(subscriber.principal, scope))
      subscriber.frame = { _tag: "Keepalive", epoch: snapshot.epoch, connectionId: id, presence }
      Queue.offerUnsafe(subscriber.wake, undefined)
    }
  }).pipe(Effect.timeout("15 seconds"))

  const session = Effect.scoped(
    Effect.gen(function* () {
      // Dedicated session; never reserve the application's query pool for LISTEN.
      const listener = yield* PgClient.makeClient({ ...sql.config, acquireForStream: false }).pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Reactivity has no scoped resources; the driver acquires into the surrounding listener-attempt Scope
        Effect.provide(Reactivity.layer)
      )
      const notifications = yield* listener.listen("echosync_changes")
      generation++
      ready = true
      yield* Deferred.succeed(started, undefined)
      yield* Effect.raceAllFirst([
        Stream.runForEach(Stream.fromQueue(notifications), (message) => Effect.sync(() => signal(message.payload))),
        Effect.forever(
          Effect.gen(function* () {
            yield* Queue.take(wake)
            yield* Effect.yieldNow
            yield* refresh
          })
        ),
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep("25 seconds")
            yield* Effect.sync(() => {
              maintenance = true
              signal("presence")
            })
          })
        )
      ])
    })
  ).pipe(Effect.ensuring(disconnect))
  const worker = yield* Effect.forever(session.pipe(Effect.catch(() => Effect.sleep("1 second")))).pipe(
    Effect.forkScoped
  )
  // A failed/cancelled constructor must not leave a reconnect loop in the caller's live scope.
  yield* Deferred.await(started).pipe(Effect.onError(() => Fiber.interrupt(worker)))

  return (principal: Principal): Stream.Stream<Wire.Change, Wire.ServerError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        if (!ready) return yield* unavailable()
        const owner = generation
        const id = yield* Effect.acquireRelease(
          Effect.gen(function* () {
            yield* sql`DELETE FROM echosync_connections WHERE expires_at<now()`
            const rows = yield* sql<{
              readonly id: string
            }>`INSERT INTO echosync_connections SELECT gen_random_uuid()::text,${principal},now()+interval '60 seconds' RETURNING id`
            const created = rows[0]?.id
            if (created === undefined) return yield* unavailable()
            return created
          }).pipe(Effect.timeout("15 seconds")),
          (released) =>
            Effect.gen(function* () {
              subscribers.delete(released)
              yield* sql`DELETE FROM echosync_connections WHERE id=${released}`
              yield* sql`SELECT pg_notify('echosync_changes','presence')`
            }).pipe(Effect.timeout("5 seconds"), Effect.ignore)
        )
        if (!ready || owner !== generation) return yield* unavailable()
        const subscriber: Subscriber = {
          principal,
          wake: yield* Queue.sliding<void, Wire.ServerError>(1),
          poke: true,
          frame: undefined
        }
        subscribers.set(id, subscriber)
        signal("presence")
        return Stream.fromQueue(subscriber.wake).pipe(
          Stream.mapEffect(() =>
            Effect.gen(function* () {
              const frame = subscriber.frame
              if (frame === undefined) return yield* unavailable()
              const tag = subscriber.poke ? "Poke" : "Keepalive"
              subscriber.poke = false
              return { ...frame, _tag: tag } satisfies Wire.Change
            })
          )
        )
      })
    ).pipe(Stream.mapError(() => unavailable()))
})
