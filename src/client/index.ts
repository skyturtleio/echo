/* BroadcastChannel.postMessage has no targetOrigin parameter. */
/* oxlint-disable unicorn/require-post-message-target-origin */
import * as Arr from "effect/Array"
import * as Clock from "effect/Clock"
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Record from "effect/Record"
import * as Schema from "effect/Schema"
import type * as EffectScope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { FetchHttpClient, HttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import type * as Aggregate from "../domain/Aggregate.ts"
import type { Principal, Scope } from "../domain/Scope.ts"
import * as Protocol from "../protocol.ts"
import * as Storage from "./BrowserStorage.ts"
import * as Diagnostics from "./Diagnostics.ts"
import * as Model from "./Model.ts"

export { ProfileBusy, StorageError } from "./BrowserStorage.ts"
export type { Diagnostics, DiagnosticOperation } from "./Diagnostics.ts"
export type { Read } from "./Model.ts"
export class ClientClosed extends Data.TaggedError("ClientClosed") {}
export class PendingIntents extends Data.TaggedError("PendingIntents")<{ readonly count: number }> {}
export class ProfileMismatch extends Data.TaggedError("ProfileMismatch") {}
export class ClientError extends Data.TaggedError("ClientError")<{ readonly cause: unknown }> {}
export class TransportError extends Data.TaggedError("TransportError")<{
  readonly reason: "offline" | "auth" | "error" | "oversized"
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
  readonly presence?: (request: Protocol.PresenceRequest) => Effect.Effect<void, TransportError>
}
export interface Options {
  /** Stable namespace including authority and domain identity, not just a user-facing label. */
  readonly profile: string
  readonly principal: Principal
  readonly aggregates: ReadonlyArray<Aggregate.Any>
  readonly transport: Transport
  /** Defaults to true. Auth/oversized failures stop automation until close/reopen; sync stays one-shot. */
  readonly autoConnect?: boolean
}
export interface Status {
  readonly connection: "idle" | "syncing" | "offline" | "error" | "auth" | "oversized" | "reset"
  readonly pending: number
  readonly lastSynced: number | null
  readonly error: unknown
}
export type Notice = Model.Intent
export type Outcome<A extends Aggregate.Any = Aggregate.Any> =
  | { readonly _tag: "Pending"; readonly id: string }
  | { readonly _tag: "Rejected"; readonly rejection: Model.DomainRejection<A> }

export interface CommandEntry {
  readonly aggregate: Aggregate.Any
  readonly command: { readonly _tag: string; readonly target: string }
}
export const command = <A extends Aggregate.Any>(aggregate: A, value: Aggregate.CommandInstance<A>): CommandEntry => ({
  aggregate,
  command: value
})
export type BatchOutcome =
  | { readonly _tag: "Pending"; readonly id: string }
  | { readonly _tag: "Rejected"; readonly rejection: unknown }

export type PresenceMember = Protocol.PresenceMember
/** Null is reserved for leaving a room on the wire. */
export type PresenceState = Exclude<Schema.Json, null>
export interface PresenceRoom {
  readonly members: Stream.Stream<ReadonlyArray<PresenceMember>>
  readonly track: (state: PresenceState) => Effect.Effect<void, ClientClosed | ClientError | TransportError>
  readonly leave: Effect.Effect<void, TransportError>
}

const failure = (cause: unknown): TransportError => {
  const oversized = Schema.is(
    Schema.TaggedStruct("RpcClientError", {
      reason: Schema.TaggedStruct("RpcClientDefect", {
        cause: Schema.TaggedStruct("MaxBufferSizeExceeded", { maxBufferSize: Schema.Number })
      })
    })
  )(cause)
  const unauthorized = Schema.is(
    Schema.Struct({
      reason: Schema.Struct({
        cause: Schema.Struct({ response: Schema.Struct({ status: Schema.Literals([401, 403]) }) })
      })
    })
  )(cause)
  return new TransportError({
    reason: oversized ? "oversized" : unauthorized ? "auth" : navigator.onLine ? "error" : "offline",
    cause
  })
}

/** Browser RPC transport; session credentials remain the server's source of identity. */
export const httpTransport = (url: string, requestInit: RequestInit = { credentials: "include" }) =>
  Effect.gen(function* () {
    const httpClient = Layer.effect(HttpClient.HttpClient)(
      Effect.map(HttpClient.HttpClient, HttpClient.filterStatusOk)
    ).pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)(requestInit)))
    const protocol = RpcClient.layerProtocolHttp({ url }).pipe(
      // NDJSON measures UTF-16 code units per response frame, not bytes or total heap.
      // Server request parsing keeps Effect's smaller default limit.
      Layer.provide(RpcSerialization.layerNdjsonWith({ maxBufferSize: 64 * 1024 * 1024 })),
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
      changes: rpc.Changes({}).pipe(Stream.mapError(failure)),
      presence: (request) => rpc.Presence(request).pipe(Effect.timeout("30 seconds"), Effect.mapError(failure))
    } satisfies Transport
  })

const attempt = <A>(body: () => A) => Effect.try({ try: body, catch: (cause) => new ClientError({ cause }) })
const emptyReplica = (): Model.Replica => ({ epoch: null, revision: 0, grants: [], cursors: {}, rows: [] })
// Length-prefix the aggregate so separators in either string cannot alias identities.
const streamKey = (aggregate: string, id: string) => `${aggregate.length}:${aggregate}${id}`
const covered = (intent: Model.Intent, replica: Model.Replica): boolean =>
  intent.receipt?._tag === "Accepted" && replica.epoch !== null && replica.revision >= intent.receipt.revision

const profileNamespace = (options: { readonly profile: string; readonly principal: Principal }) =>
  `${encodeURIComponent(options.profile)}:${encodeURIComponent(options.principal)}`

export const resetProfile = Effect.fn("Echo.resetProfile")(function* (options: {
  readonly profile: string
  readonly principal: Principal
  readonly discardPending?: boolean
}) {
  const namespace = profileNamespace(options)
  yield* Storage.withLock(
    `${namespace}:sync`,
    Storage.withLock(
      namespace,
      Effect.gen(function* () {
        const journalDb = yield* Storage.openDatabase(`echo:journal:${namespace}`)
        const replicaDb = yield* Storage.openDatabase(`echo:replica:${namespace}`)
        const saved = yield* Storage.load(journalDb)
        const journal = saved === undefined ? undefined : yield* Schema.decodeUnknownEffect(Model.Journal)(saved)
        const count = Arr.filter(journal?.intents ?? [], (intent) => intent.phase !== "Rejected").length
        if (count > 0 && options.discardPending !== true) return yield* new PendingIntents({ count })
        // Erasing identity first fences stale peers even if replica erasure fails.
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            yield* Storage.save(journalDb, undefined)
            const channel = new BroadcastChannel(`echo:${namespace}`)
            // Peers check durable identity rather than withdrawing a newly opened generation.
            channel.postMessage("replica")
            channel.close()
          })
        )
        yield* Storage.save(replicaDb, undefined)
      })
    )
  )
})

/** Hydrates before returning. All resources, connection work and ownership end with the caller's Scope. */
export const open = (options: Options) =>
  Effect.gen(function* () {
    const diagnostics = Diagnostics.make()
    const saveJournal = (db: IDBDatabase, value: Model.Journal) =>
      diagnostics.measure("journal", Storage.save(db, value), () => value.intents.length)
    const saveReplica = (db: IDBDatabase, value: Model.Replica) =>
      diagnostics.measure("replica", Storage.save(db, value), () => value.rows.length)
    const namespace = profileNamespace(options)
    const channel = yield* Effect.acquireRelease(
      Effect.sync(() => new BroadcastChannel(`echo:${namespace}`)),
      (value) => Effect.sync(() => value.close())
    )
    const journalDb = yield* Storage.openDatabase(`echo:journal:${namespace}`)
    const replicaDb = yield* Storage.openDatabase(`echo:replica:${namespace}`)
    const hydrated = yield* Storage.withLock(
      namespace,
      Effect.gen(function* () {
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
        yield* saveJournal(journalDb, journal)
        const cached = yield* Storage.load(replicaDb)
        let replica = yield* saved === undefined || cached === undefined
          ? Effect.succeed(emptyReplica())
          : Schema.decodeUnknownEffect(Model.Replica)(cached)
        const invalidCache = replica.epoch !== journal.epoch
        if (invalidCache) replica = emptyReplica()
        if (saved === undefined || cached === undefined || invalidCache) yield* saveReplica(replicaDb, replica)
        return { journal, replica }
      })
    )
    let journal = hydrated.journal
    let replica = hydrated.replica
    let closed = false
    const stopped = yield* Deferred.make<void>()
    const resourceStopped = yield* Deferred.make<void>()
    const live = <A>(stream: Stream.Stream<A>): Stream.Stream<A> =>
      Stream.suspend(() => (closed ? Stream.empty : stream.pipe(Stream.interruptWhen(Deferred.await(resourceStopped)))))
    const mutex = yield* Semaphore.make(1)
    const syncMutex = yield* Semaphore.make(1)
    const requested = yield* SubscriptionRef.make(0)
    let isNetworkOwner = false
    const frame = yield* SubscriptionRef.make<Protocol.Change>({ _tag: "Keepalive", epoch: "" })
    const state = yield* SubscriptionRef.make<ReadonlyArray<Model.Row>>([])
    let read = Model.makeReader(options.principal, replica.grants)
    const snapshotReaders = new WeakMap<ReadonlyArray<Model.Row>, ReturnType<typeof Model.makeReader>>()
    let publishedJournal: Model.Journal | undefined
    let publishedReplica: Model.Replica | undefined
    let rejectPending = false
    const notices = yield* SubscriptionRef.make<ReadonlyArray<Notice>>([])
    const status = yield* SubscriptionRef.make<Status>({
      connection: "idle",
      pending: 0,
      lastSynced: null,
      error: null
    })
    const guard = Effect.suspend(() => (closed ? Effect.fail(new ClientClosed()) : Effect.void))
    const withdraw = Effect.gen(function* () {
      closed = true
      yield* SubscriptionRef.set(state, [])
      yield* SubscriptionRef.set(notices, [])
      yield* SubscriptionRef.set(frame, { _tag: "Keepalive", epoch: "" })
      yield* SubscriptionRef.set(status, { connection: "reset", pending: 0, lastSynced: null, error: null })
      yield* Deferred.succeed(stopped, undefined)
    })
    const refresh = Effect.gen(function* () {
      const saved = yield* Storage.load(journalDb)
      if (saved === undefined) {
        yield* withdraw
        return yield* new ClientClosed()
      }
      const nextJournal = yield* Schema.decodeUnknownEffect(Model.Journal)(saved)
      if (nextJournal.clientId !== journal.clientId) {
        yield* withdraw
        return yield* new ClientClosed()
      }
      const cached = yield* Storage.load(replicaDb)
      const nextReplica =
        cached === undefined ? emptyReplica() : yield* Schema.decodeUnknownEffect(Model.Replica)(cached)
      if (!Equal.equals(journal, nextJournal)) journal = nextJournal
      if (!Equal.equals(replica, nextReplica))
        replica = nextReplica.epoch === journal.epoch ? nextReplica : emptyReplica()
    })
    const locked = <A, E>(effect: Effect.Effect<A, E>) =>
      mutex.withPermit(
        Storage.withLock(
          namespace,
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* guard
              yield* refresh
              return yield* effect
            })
          )
        )
      )
    const writeJournal = (next: Model.Journal) =>
      Effect.gen(function* () {
        if (Equal.equals(journal, next)) return
        yield* saveJournal(journalDb, next)
        journal = next
        channel.postMessage("changed")
      })
    const publish = (rejectUnsent = false, prediction?: ReadonlyArray<Model.Row>) =>
      Effect.gen(function* () {
        rejectPending ||= rejectUnsent
        if (!rejectPending && publishedJournal === journal && publishedReplica === replica) return
        let rows = prediction ?? replica.rows
        const intents: Array<Model.Intent> = []
        for (const intent of journal.intents) {
          if (prediction !== undefined || intent.phase === "Rejected" || intent.phase === "Unknown") {
            intents.push(intent)
            continue
          }
          if (covered(intent, replica)) continue
          const decision = yield* attempt(() =>
            Model.decide(options.aggregates, rows, options.principal, intent, replica.grants)
          )
          if (decision.rejection === null) {
            rows = decision.rows
            intents.push(intent)
          } else if (rejectPending && intent.phase === "Pending") {
            intents.push({ ...intent, phase: "Rejected", rejection: decision.rejection })
          } else intents.push(intent)
        }
        // Rows derive solely from already durable intents and the current confirmed replica.
        // A failed retirement/notice write must not leave subsequent decisions on stale hot state.
        const previous = SubscriptionRef.getUnsafe(state)
        const grantsChanged = publishedReplica?.grants !== replica.grants
        if (grantsChanged) read = Model.makeReader(options.principal, replica.grants)
        if (
          grantsChanged ||
          previous.length !== rows.length ||
          !Arr.every(rows, (row, i) => Equal.equals(row, previous[i]))
        ) {
          rows = [...rows]
          snapshotReaders.set(rows, read)
          yield* SubscriptionRef.set(state, rows)
        }
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
        // Failed reconciliation must retry, including authoritative rejection persistence.
        publishedJournal = journal
        publishedReplica = replica
        rejectPending = false
      }).pipe((effect) => diagnostics.measure("rebase", effect, () => SubscriptionRef.getUnsafe(state).length))
    yield* locked(publish())
    yield* Effect.addFinalizer(() =>
      mutex.withPermit(
        Effect.gen(function* () {
          closed = true
          yield* Deferred.succeed(stopped, undefined)
          yield* Deferred.succeed(resourceStopped, undefined)
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
          yield* saveReplica(replicaDb, next).pipe(
            Effect.onError(() =>
              Effect.sync(() => {
                publishedReplica = undefined
              })
            )
          )
        })
      )
    const pull = Effect.gen(function* () {
      const request = yield* locked(
        Effect.sync(() => ({ epoch: journal.epoch, cursors: replica.epoch === journal.epoch ? replica.cursors : {} }))
      )
      const response = yield* diagnostics.measure("pull", options.transport.pull(request), (reply) =>
        Arr.reduce(reply.scopes, 0, (count, scope) => count + scope.entries.length)
      )
      if (request.epoch !== null && response.epoch !== request.epoch) {
        return yield* new Protocol.ResyncRequired({ epoch: response.epoch })
      }
      yield* locked(
        Effect.gen(function* () {
          const scopes = new Set(Arr.map(response.scopes, (scope) => scope.scope))
          if (
            response.epoch === journal.epoch &&
            response.epoch === replica.epoch &&
            response.revision === replica.revision &&
            Equal.equals(response.grants, replica.grants) &&
            scopes.size === response.scopes.length &&
            scopes.size === Record.keys(replica.cursors).length &&
            Arr.every(
              response.scopes,
              (scope) => !scope.reset && scope.entries.length === 0 && replica.cursors[scope.scope] === scope.cursor
            )
          ) {
            yield* publish(true)
            return
          }
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
          const next = {
            epoch: response.epoch,
            revision: response.revision,
            grants: response.grants,
            rows: [...rows.values()],
            cursors
          }
          yield* saveReplica(replicaDb, next)
          replica = next
          channel.postMessage("replica")
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
              batches: [
                {
                  id: intent.id,
                  commands: intent.commands,
                  ...(intent.action === undefined ? {} : { action: intent.action })
                }
              ]
            }
          })
        )
        const response = yield* diagnostics.measure(
          "execute",
          options.transport.execute(request),
          (reply) => reply.receipts.length
        )
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
        Storage.withLock(
          `${namespace}:sync`,
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

    function enqueue(
      entries: ReadonlyArray<CommandEntry>,
      action?: { readonly name: string; readonly input: unknown }
    ) {
      return locked(
        Effect.gen(function* () {
          // A prior sync may have durably advanced the replica before journal reconciliation failed.
          // Recover from that state before deciding another local command, rather than using stale hot rows.
          yield* publish()
          const commands = yield* attempt(() =>
            Arr.map(entries, ({ aggregate, command: value }) => {
              const schema = Option.getOrUndefined(
                Arr.findFirst(aggregate.definition.commands, (c) => c.tag === value._tag)
              )
              if (schema === undefined) throw new Error("Unknown command")
              return { aggregate: aggregate.tag, command: Model.encode(schema, value) }
            })
          )
          const intent: Model.Intent = {
            id: yield* Storage.randomId,
            commands,
            ...(action === undefined ? {} : { action }),
            now: yield* Clock.currentTimeMillis,
            attemptEpoch: null,
            dismissed: false,
            phase: "Pending",
            receipt: null,
            rejection: null
          }
          const decision = yield* attempt(() =>
            Model.decide(
              options.aggregates,
              SubscriptionRef.getUnsafe(state),
              options.principal,
              intent,
              replica.grants
            )
          )
          if (decision.rejection !== null)
            return {
              _tag: "Rejected",
              rejection: decision.rejection
            } satisfies BatchOutcome
          yield* writeJournal({ ...journal, intents: [...journal.intents, intent] })
          // The mutex and successful recovery above make this the durable journal's exact next prediction.
          yield* publish(false, decision.rows)
          yield* SubscriptionRef.update(requested, (n) => n + 1)
          return { _tag: "Pending", id: intent.id } satisfies BatchOutcome
        })
      )
    }
    function execute<A extends Aggregate.Any>(aggregate: A, value: Aggregate.CommandInstance<A>) {
      return Effect.gen(function* () {
        const result = yield* enqueue([command(aggregate, value)])
        return result._tag === "Pending"
          ? result
          : ({
              _tag: "Rejected",
              rejection: yield* attempt(() => Model.decodeRejection(aggregate, result.rejection))
            } satisfies Outcome<A>)
      })
    }
    function query<A>(select: (read: Model.Read) => A): A {
      if (closed) throw new ClientClosed()
      return select(read(SubscriptionRef.getUnsafe(state)))
    }
    function subscribe<A>(select: (read: Model.Read) => A): Stream.Stream<A> {
      return live(
        SubscriptionRef.changes(state).pipe(
          Stream.map((rows) => select((snapshotReaders.get(rows) ?? read)(rows))),
          Stream.changesWith((a, b) => Equal.equals(a, b))
        )
      )
    }
    const resetReplica = syncMutex.withPermit(
      Storage.withLock(
        `${namespace}:sync`,
        locked(
          Effect.gen(function* () {
            const next = emptyReplica()
            yield* saveReplica(replicaDb, next)
            replica = next
            yield* publish()
            channel.postMessage("changed")
          })
        )
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
    // Browser connectivity is only a retry hint; the transport still decides whether the attempt works.
    const retry = Effect.callback<void>((resume) => {
      const online = () => {
        globalThis.removeEventListener("online", online)
        resume(Effect.void)
      }
      globalThis.addEventListener("online", online)
      return Effect.sync(() => globalThis.removeEventListener("online", online))
    }).pipe(Effect.race(Effect.sleep("1 second")))
    /** Exactly one connection loop belongs to this profile resource. Local execute never waits for it. */
    const connect = Effect.forever(
      Effect.gen(function* () {
        yield* Stream.merge(
          SubscriptionRef.changes(requested).pipe(Stream.map(() => undefined)),
          options.transport.changes.pipe(
            Stream.timeout("55 seconds"),
            Stream.tap((change) =>
              Effect.gen(function* () {
                yield* SubscriptionRef.set(frame, change)
                channel.postMessage(change)
              })
            ),
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
              if (error._tag === "TransportError" && (error.reason === "auth" || error.reason === "oversized"))
                return yield* error
            })
          )
        )
        yield* retry
      })
    ).pipe(Effect.catchTag("TransportError", () => Effect.void))
    yield* Stream.fromEventListener<MessageEvent>(channel, "message").pipe(
      Stream.runForEach((event) => {
        const value: unknown = event.data
        if (Schema.is(Protocol.Change)(value)) return SubscriptionRef.set(frame, value)
        if (value === "hello")
          return Effect.sync(() => {
            if (isNetworkOwner) channel.postMessage(SubscriptionRef.getUnsafe(frame))
          })
        return locked(
          Effect.gen(function* () {
            yield* publish()
            if (value !== "replica") yield* SubscriptionRef.update(requested, (n) => n + 1)
          })
        )
      }),
      Effect.race(Deferred.await(stopped)),
      Effect.catch(() => Effect.void),
      Effect.forkScoped
    )
    if (options.autoConnect !== false)
      yield* Effect.forkScoped(
        Storage.withLock(
          `${namespace}:leader`,
          Effect.gen(function* () {
            isNetworkOwner = true
            yield* connect
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                isNetworkOwner = false
              })
            )
          )
        ).pipe(Effect.race(Deferred.await(stopped)))
      )
    channel.postMessage("hello")
    const presence = Effect.fn("Echo.Client.presence")(function* (
      room: Scope,
      initial: PresenceState
    ): Effect.fn.Return<PresenceRoom, ClientClosed | ClientError, EffectScope.Scope> {
      yield* guard
      if (initial === null) return yield* new ClientError({ cause: "Null presence state is reserved for leave" })
      const sendPresence = options.transport.presence
      if (sendPresence === undefined) return yield* new ClientError({ cause: "Transport does not support presence" })
      const clientId = yield* Storage.randomId
      let current = initial
      let left = false
      let connectionId: string | undefined
      const roomStopped = yield* Deferred.make<void>()
      const track = (value: PresenceState) =>
        Effect.gen(function* () {
          yield* guard
          if (value === null) return yield* new ClientError({ cause: "Null presence state is reserved for leave" })
          if (left || connectionId === undefined) return yield* new ClientError({ cause: "Presence is disconnected" })
          current = value
          yield* sendPresence({ room, clientId, connectionId, state: value })
        })
      const leave = Effect.gen(function* () {
        if (left) return
        left = true
        yield* Deferred.succeed(roomStopped, undefined)
        if (connectionId !== undefined) yield* sendPresence({ room, clientId, connectionId, state: null })
      })
      yield* Effect.addFinalizer(() => leave.pipe(Effect.catch(() => Effect.void)))
      yield* SubscriptionRef.changes(frame).pipe(
        Stream.runForEach((change) =>
          Effect.gen(function* () {
            if (!left && !closed && change.connectionId !== undefined && change.connectionId !== connectionId) {
              connectionId = change.connectionId
              yield* sendPresence({ room, clientId, connectionId, state: current }).pipe(
                Effect.catch(() => Effect.void)
              )
            }
          })
        ),
        Effect.race(Deferred.await(stopped)),
        Effect.race(Deferred.await(roomStopped)),
        Effect.forkScoped
      )
      yield* Effect.forever(
        Effect.gen(function* () {
          yield* Effect.sleep("20 seconds")
          if (!left && connectionId !== undefined) yield* track(current).pipe(Effect.catch(() => Effect.void))
        })
      ).pipe(Effect.race(Deferred.await(stopped)), Effect.race(Deferred.await(roomStopped)), Effect.forkScoped)
      return {
        track,
        leave,
        members: live(
          SubscriptionRef.changes(frame).pipe(
            Stream.map((change) => Arr.filter(change.presence ?? [], (member) => member.room === room)),
            Stream.changesWith((a, b) => Equal.equals(a, b))
          )
        )
      }
    })
    return {
      execute,
      presence,
      batch: (entries: ReadonlyArray<CommandEntry>) => enqueue(entries),
      action: (name: string, input: unknown) => enqueue([], { name, input }),
      pending: locked(Effect.sync(() => Arr.filter(journal.intents, (intent) => intent.phase !== "Rejected"))),
      requestSync: Effect.andThen(
        guard,
        Effect.sync(() => {
          channel.postMessage("sync")
        })
      ).pipe(Effect.andThen(SubscriptionRef.update(requested, (n) => n + 1))),
      query,
      subscribe,
      sync,
      resetReplica,
      dismiss,
      /** In-memory snapshot only; age uses the device clock and durable intent creation times. */
      diagnostics: Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis
        let count = 0
        let oldest: number | null = null
        for (const intent of journal.intents) {
          if (intent.phase === "Rejected" || intent.phase === "Unknown") continue
          count++
          oldest = oldest === null ? intent.now : Math.min(oldest, intent.now)
        }
        return {
          ...diagnostics.snapshot(),
          pending: { count, oldestAgeMs: oldest === null ? null : Math.max(0, now - oldest) }
        } satisfies Diagnostics.Diagnostics
      }),
      status: live(SubscriptionRef.changes(status)),
      notices: live(SubscriptionRef.changes(notices))
    }
  })

const ProtocolEpoch = Schema.Struct({ _tag: Schema.Literal("ResyncRequired"), epoch: Schema.String })
export type Client = Effect.Success<ReturnType<typeof open>>
