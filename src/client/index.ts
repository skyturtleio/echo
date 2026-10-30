import * as Arr from "effect/Array"
import * as Clock from "effect/Clock"
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { FetchHttpClient, HttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import type * as Aggregate from "../domain/Aggregate.ts"
import type { Principal } from "../domain/Scope.ts"
import * as Protocol from "../protocol.ts"
import * as Storage from "./BrowserStorage.ts"
import * as Model from "./Model.ts"

export { ProfileBusy, StorageError } from "./BrowserStorage.ts"
export type { Read } from "./Model.ts"
export class ClientClosed extends Data.TaggedError("ClientClosed") {}
export class ProfileMismatch extends Data.TaggedError("ProfileMismatch") {}
export class ClientError extends Data.TaggedError("ClientError")<{ readonly cause: unknown }> {}
export class TransportError extends Data.TaggedError("TransportError")<{
  readonly reason: "offline" | "auth" | "error"
  readonly cause: unknown
}> {}

export interface Transport {
  readonly execute: (
    request: Protocol.ExecuteRequest
  ) => Effect.Effect<Protocol.ExecuteResponse, TransportError | Protocol.ResyncRequired>
  readonly pull: (
    request: Protocol.PullRequest
  ) => Effect.Effect<Protocol.PullResponse, TransportError | Protocol.ResyncRequired>
  readonly changes: Stream.Stream<Protocol.Change, TransportError>
}
export interface Options {
  /** Stable namespace including authority and domain identity, not just a user-facing label. */
  readonly profile: string
  readonly principal: Principal
  readonly aggregates: ReadonlyArray<Aggregate.Any>
  readonly transport: Transport
  /** Defaults to true. Auth failure stops automation until close/reopen; sync remains a one-shot attempt. */
  readonly autoConnect?: boolean
}
export interface Status {
  readonly connection: "idle" | "syncing" | "offline" | "error" | "auth"
  readonly pending: number
  readonly lastSynced: number | null
  readonly error: unknown
}
export type Notice = Model.Intent
export type Outcome<A extends Aggregate.Any = Aggregate.Any> =
  | { readonly _tag: "Pending"; readonly id: string }
  | { readonly _tag: "Rejected"; readonly rejection: Model.DomainRejection<A> }

const failure = (cause: unknown): TransportError => {
  const unauthorized = Schema.is(
    Schema.Struct({
      reason: Schema.Struct({
        cause: Schema.Struct({ response: Schema.Struct({ status: Schema.Literals([401, 403]) }) })
      })
    })
  )(cause)
  return new TransportError({ reason: unauthorized ? "auth" : navigator.onLine ? "error" : "offline", cause })
}

/** Browser RPC transport; session credentials remain the server's source of identity. */
export const httpTransport = (url: string, requestInit: RequestInit = { credentials: "include" }) =>
  Effect.gen(function* () {
    const httpClient = Layer.effect(HttpClient.HttpClient)(
      Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk)
    ).pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)(requestInit)))
    const protocol = RpcClient.layerProtocolHttp({ url }).pipe(
      Layer.provide(RpcSerialization.layerNdjson),
      Layer.provide(httpClient)
    )
    // Browser transport entry point: its RPC and HTTP resources belong to the caller's scope.
    // oxlint-disable-next-line effecttsgo/strict-effect-provide
    const rpc = yield* RpcClient.make(Protocol.EchoRpcs).pipe(Effect.provide(protocol))
    return {
      execute: (request) =>
        rpc.Execute(request).pipe(
          Effect.timeout("30 seconds"),
          Effect.mapError((error) => (error._tag === "ResyncRequired" ? error : failure(error)))
        ),
      pull: (request) =>
        rpc.Pull(request).pipe(
          Effect.timeout("30 seconds"),
          Effect.mapError((error) => (error._tag === "ResyncRequired" ? error : failure(error)))
        ),
      changes: rpc.Changes({}).pipe(Stream.mapError(failure))
    } satisfies Transport
  })

const attempt = <A>(body: () => A) => Effect.try({ try: body, catch: (cause) => new ClientError({ cause }) })
const emptyReplica = (): Model.Replica => ({ epoch: null, cursors: {}, rows: [] })
// Length-prefix the aggregate so separators in either string cannot alias identities.
const streamKey = (aggregate: string, id: string) => `${aggregate.length}:${aggregate}${id}`
const covered = (intent: Model.Intent, replica: Model.Replica): boolean =>
  intent.receipt?._tag === "Accepted" &&
  Arr.every(intent.receipt.seqs, (ref) =>
    Arr.some(replica.rows, (row) => row.aggregate === ref.aggregate && row.id === ref.id && row.seq >= ref.seq)
  )

/** Hydrates before returning. All resources, connection work and ownership end with the caller's Scope. */
export const open = (options: Options) =>
  Effect.gen(function* () {
    const namespace = `${encodeURIComponent(options.profile)}:${encodeURIComponent(options.principal)}`
    yield* Storage.ownProfile(namespace)
    const journalDb = yield* Storage.openDatabase(`echo:journal:${namespace}`)
    const replicaDb = yield* Storage.openDatabase(`echo:replica:${namespace}`)
    const saved = yield* Storage.load(journalDb)
    const clientId = yield* Storage.randomId
    let journal: Model.Journal = yield* saved === undefined
      ? Effect.succeed({
          principal: options.principal,
          clientId,
          epoch: null,
          intents: []
        })
      : Schema.decodeUnknownEffect(Model.Journal)(saved)
    if (journal.principal !== options.principal) return yield* Effect.fail(new ProfileMismatch())
    yield* Storage.save(journalDb, journal)
    const cached = yield* Storage.load(replicaDb)
    let replica = yield* saved === undefined || cached === undefined
      ? Effect.succeed(emptyReplica())
      : Schema.decodeUnknownEffect(Model.Replica)(cached)
    if (replica.epoch !== journal.epoch) replica = emptyReplica()
    yield* Storage.save(replicaDb, replica)
    let closed = false
    const stopped = yield* Deferred.make<void>()
    const live = <A>(stream: Stream.Stream<A>): Stream.Stream<A> =>
      Stream.suspend(() => (closed ? Stream.empty : stream.pipe(Stream.interruptWhen(Deferred.await(stopped)))))
    const mutex = yield* Semaphore.make(1)
    const syncMutex = yield* Semaphore.make(1)
    const requested = yield* SubscriptionRef.make(0)
    const state = yield* SubscriptionRef.make<ReadonlyArray<Model.Row>>([])
    const notices = yield* SubscriptionRef.make<ReadonlyArray<Notice>>([])
    const status = yield* SubscriptionRef.make<Status>({
      connection: "idle",
      pending: 0,
      lastSynced: null,
      error: null
    })
    const guard = Effect.suspend(() => (closed ? Effect.fail(new ClientClosed()) : Effect.void))
    const locked = <A, E>(effect: Effect.Effect<A, E>) =>
      mutex.withPermit(Effect.uninterruptible(Effect.andThen(guard, effect)))
    const writeJournal = (next: Model.Journal) =>
      Effect.gen(function* () {
        yield* Storage.save(journalDb, next)
        journal = next
      })
    const publish = (rejectUnsent = false) =>
      Effect.gen(function* () {
        let rows = replica.rows
        const intents: Array<Model.Intent> = []
        for (const intent of journal.intents) {
          if (intent.phase === "Rejected" || intent.phase === "Unknown") {
            intents.push(intent)
            continue
          }
          if (covered(intent, replica)) continue
          const decision = yield* attempt(() => Model.decide(options.aggregates, rows, options.principal, intent))
          if (decision.rejection === null) {
            rows = decision.rows
            intents.push(intent)
          } else if (rejectUnsent && intent.phase === "Pending") {
            intents.push({ ...intent, phase: "Rejected", rejection: decision.rejection })
          } else intents.push(intent)
        }
        // Rows derive solely from already durable intents and the current confirmed replica.
        // A failed retirement/notice write must not leave subsequent decisions on stale hot state.
        yield* SubscriptionRef.set(state, rows)
        if (!Equal.equals(journal.intents, intents)) yield* writeJournal({ ...journal, intents })
        yield* SubscriptionRef.set(
          notices,
          Arr.filter(
            intents,
            (intent) => !intent.dismissed && (intent.phase === "Rejected" || intent.phase === "Unknown")
          )
        )
        yield* SubscriptionRef.update(status, (s) => ({
          ...s,
          pending: Arr.filter(intents, (i) => i.phase !== "Rejected" && i.phase !== "Unknown").length
        }))
      })
    yield* publish()
    yield* Effect.addFinalizer(() =>
      mutex.withPermit(
        Effect.gen(function* () {
          closed = true
          yield* Deferred.succeed(stopped, undefined)
        })
      )
    )

    const changeEpoch = (epoch: string) =>
      locked(
        Effect.gen(function* () {
          if (journal.epoch === epoch && replica.epoch === epoch) return
          yield* writeJournal({
            ...journal,
            epoch,
            intents: Arr.map(journal.intents, (intent) =>
              intent.phase === "Sent" || intent.phase === "Accepted"
                ? { ...intent, phase: "Unknown", rejection: { _tag: "AuthorityChanged" } }
                : intent
            )
          })
          const next = emptyReplica()
          // Invalidate hot authority before fallible cache I/O; never send old cursors with the new epoch.
          replica = next
          yield* publish()
          yield* Storage.save(replicaDb, next)
        })
      )
    const pull = Effect.gen(function* () {
      const request = yield* locked(
        Effect.sync(() => ({ epoch: journal.epoch, cursors: replica.epoch === journal.epoch ? replica.cursors : {} }))
      )
      const response = yield* options.transport.pull(request)
      if (request.epoch !== null && response.epoch !== request.epoch) {
        return yield* new Protocol.ResyncRequired({ epoch: response.epoch })
      }
      yield* locked(
        Effect.gen(function* () {
          const scopes = new Set(Arr.map(response.scopes, (scope) => scope.scope))
          // Map insertion order matches filter-then-push: replacements move to the end.
          const rows = new Map<string, Model.Row>()
          const members = new Map<string, Set<string>>()
          const remove = (key: string) => {
            const row = rows.get(key)
            if (row === undefined) return
            members.get(row.scope)?.delete(key)
            rows.delete(key)
          }
          const append = (key: string, row: Model.Row) => {
            rows.set(key, row)
            let scopeMembers = members.get(row.scope)
            if (scopeMembers === undefined) {
              scopeMembers = new Set()
              members.set(row.scope, scopeMembers)
            }
            scopeMembers.add(key)
          }
          for (const row of replica.rows) {
            if (scopes.has(row.scope)) append(streamKey(row.aggregate, row.id), row)
          }
          const cursors: Record<string, number> = {}
          for (const scope of response.scopes) {
            if (scope.reset) {
              for (const key of members.get(scope.scope) ?? []) remove(key)
            }
            for (const entry of scope.entries) {
              const key = streamKey(entry.aggregate, entry.id)
              if (entry._tag === "Upsert" || rows.get(key)?.scope === scope.scope) remove(key)
              if (entry._tag === "Upsert") {
                const aggregate = Option.getOrUndefined(
                  Arr.findFirst(options.aggregates, (a) => a.tag === entry.aggregate)
                )
                if (aggregate === undefined)
                  return yield* Effect.fail(new ClientError({ cause: `Unknown aggregate ${entry.aggregate}` }))
                yield* attempt(() => Model.decode(aggregate.definition.state, entry.state))
                append(key, {
                  aggregate: entry.aggregate,
                  id: entry.id,
                  scope: scope.scope,
                  seq: entry.seq,
                  state: entry.state
                })
              }
            }
            cursors[scope.scope] = scope.cursor
          }
          // Journal epoch first: a crash here invalidates the old replica on the next open.
          yield* writeJournal({ ...journal, epoch: response.epoch })
          const next = { epoch: response.epoch, rows: [...rows.values()], cursors }
          yield* Storage.save(replicaDb, next)
          replica = next
          yield* publish(true)
        })
      )
    })
    const send = (intent: Model.Intent) =>
      Effect.gen(function* () {
        const request = yield* locked(
          Effect.gen(function* () {
            yield* writeJournal({
              ...journal,
              intents: Arr.map(journal.intents, (i) =>
                i.id === intent.id ? { ...i, phase: "Sent", attemptEpoch: journal.epoch } : i
              )
            })
            return {
              epoch: journal.epoch,
              clientId: journal.clientId,
              batches: [{ id: intent.id, commands: [{ aggregate: intent.aggregate, command: intent.command }] }]
            }
          })
        )
        const response = yield* options.transport.execute(request)
        if (request.epoch !== null && response.epoch !== request.epoch) {
          return yield* new Protocol.ResyncRequired({ epoch: response.epoch })
        }
        yield* locked(
          Effect.gen(function* () {
            const receipt = Option.getOrUndefined(Arr.findFirst(response.receipts, (r) => r.batchId === intent.id))
            if (receipt === undefined) return yield* Effect.fail(new ClientError({ cause: "Missing receipt" }))
            yield* writeJournal({
              ...journal,
              epoch: response.epoch,
              intents: Arr.map(journal.intents, (i) =>
                i.id === intent.id
                  ? {
                      ...i,
                      phase: receipt._tag,
                      receipt,
                      rejection: receipt._tag === "Rejected" ? receipt.rejection : null
                    }
                  : i
              )
            })
            // Rejections are visible even if the following pull fails; accepted predictions remain until covered.
            yield* publish()
          })
        )
      })
    const sync = syncMutex
      .withPermit(
        Effect.gen(function* () {
          yield* guard
          yield* SubscriptionRef.update(status, (s): Status => ({ ...s, connection: "syncing", error: null }))
          // Recover lost receipts BEFORE applying any newer snapshot: otherwise a non-idempotent prediction doubles.
          const sent = yield* locked(Effect.sync(() => Arr.filter(journal.intents, (i) => i.phase === "Sent")))
          for (const intent of sent) yield* send(intent)
          yield* pull
          while (true) {
            const intent = yield* locked(
              Effect.sync(() => Option.getOrUndefined(Arr.findFirst(journal.intents, (i) => i.phase === "Pending")))
            )
            if (intent === undefined) break
            yield* send(intent)
            yield* pull
          }
          const now = yield* Clock.currentTimeMillis
          yield* SubscriptionRef.update(status, (s): Status => ({
            ...s,
            connection: "idle",
            lastSynced: now,
            error: null
          }))
        })
      )
      .pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            if (Schema.is(ProtocolEpoch)(error)) yield* changeEpoch(error.epoch)
            yield* SubscriptionRef.update(status, (s): Status => ({
              ...s,
              connection: error._tag === "TransportError" ? error.reason : "error",
              error
            }))
            return yield* Effect.fail(error)
          })
        )
      )

    function execute<A extends Aggregate.Any>(aggregate: A, command: Aggregate.CommandInstance<A>) {
      return locked(
        Effect.gen(function* () {
          // A prior sync may have durably advanced the replica before journal reconciliation failed.
          // Recover from that state before deciding another local command, rather than using stale hot rows.
          yield* publish()
          const commandClass = Option.getOrUndefined(
            Arr.findFirst(aggregate.definition.commands, (c) => c.tag === command._tag)
          )
          if (commandClass === undefined) return yield* Effect.fail(new ClientError({ cause: "Unknown command" }))
          const intent: Model.Intent = {
            id: yield* Storage.randomId,
            aggregate: aggregate.tag,
            command: yield* attempt(() => Model.encode(commandClass, command)),
            now: yield* Clock.currentTimeMillis,
            attemptEpoch: null,
            dismissed: false,
            phase: "Pending",
            receipt: null,
            rejection: null
          }
          const decision = yield* attempt(() =>
            Model.decide(options.aggregates, SubscriptionRef.getUnsafe(state), options.principal, intent)
          )
          if (decision.rejection !== null)
            return {
              _tag: "Rejected",
              rejection: yield* attempt(() => Model.decodeRejection(aggregate, decision.rejection))
            } satisfies Outcome<A>
          yield* writeJournal({ ...journal, intents: [...journal.intents, intent] })
          yield* publish()
          yield* SubscriptionRef.update(requested, (n) => n + 1)
          return { _tag: "Pending", id: intent.id } satisfies Outcome
        })
      )
    }
    function query<A>(select: (read: Model.Read) => A): A {
      if (closed) throw new ClientClosed()
      return select(Model.reader(SubscriptionRef.getUnsafe(state), options.principal))
    }
    function subscribe<A>(select: (read: Model.Read) => A): Stream.Stream<A> {
      return live(
        SubscriptionRef.changes(state).pipe(
          Stream.map((rows) => select(Model.reader(rows, options.principal))),
          Stream.changesWith((a, b) => Equal.equals(a, b))
        )
      )
    }
    const resetReplica = syncMutex.withPermit(
      locked(
        Effect.gen(function* () {
          const next = emptyReplica()
          yield* Storage.save(replicaDb, next)
          replica = next
          yield* publish()
        })
      )
    )
    const dismiss = (id: string) =>
      locked(
        Effect.gen(function* () {
          // Unknown identities remain retained. Dismissal never turns uncertainty into permission to retry.
          yield* writeJournal({
            ...journal,
            intents: Arr.map(journal.intents, (i) =>
              i.id === id && (i.phase === "Unknown" || i.phase === "Rejected") ? { ...i, dismissed: true } : i
            )
          })
          yield* publish()
        })
      )
    /** Exactly one connection loop belongs to this profile resource. Local execute never waits for it. */
    const connect = Effect.forever(
      Effect.gen(function* () {
        yield* Stream.merge(
          SubscriptionRef.changes(requested).pipe(Stream.map(() => undefined)),
          options.transport.changes.pipe(
            Stream.timeout("55 seconds"),
            Stream.filter((change) => change._tag === "Poke" || change.epoch !== journal.epoch),
            Stream.map(() => undefined)
          ),
          { haltStrategy: "either" }
        ).pipe(
          Stream.runForEach(() => sync),
          Effect.catch((error) =>
            Effect.gen(function* () {
              yield* SubscriptionRef.update(status, (s): Status => ({
                ...s,
                connection: error._tag === "TransportError" ? error.reason : "error",
                error
              }))
              if (error._tag === "TransportError" && error.reason === "auth") return yield* error
            })
          )
        )
        yield* Effect.sleep("1 second")
      })
    ).pipe(Effect.catchTag("TransportError", () => Effect.void))
    if (options.autoConnect !== false) yield* Effect.forkScoped(connect)
    return {
      execute,
      query,
      subscribe,
      sync,
      resetReplica,
      dismiss,
      status: live(SubscriptionRef.changes(status)),
      notices: live(SubscriptionRef.changes(notices))
    }
  })

const ProtocolEpoch = Schema.Struct({ _tag: Schema.Literal("ResyncRequired"), epoch: Schema.String })
export type Client = Effect.Success<ReturnType<typeof open>>
