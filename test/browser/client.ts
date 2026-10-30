import * as Arr from "effect/Array"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Record from "effect/Record"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Str from "effect/String"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { FetchHttpClient, HttpClient } from "effect/http"
import { DefineTag, Note, NoteId, Tag, TagId, WriteNote } from "../../examples/fixture/domain/src/index.ts"
import * as Domain from "../../src/domain/index.ts"
import * as Client from "../../src/client/index.ts"
import { ResyncRequired, type Change, type ExecuteRequest, type PullResponse } from "../../src/protocol.ts"

// Non-idempotent fixture: repeating a prediction changes 3 to 6, unlike a set-value command.
const Id = Schema.String
const State = Schema.NullOr(Schema.Struct({ value: Schema.Number, scope: Domain.Scope.Scope }))
class Added extends Domain.Event("Added", { amount: Schema.Number, scope: Domain.Scope.Scope }) {}
class Limit extends Domain.Rejection("Limit", {}) {}
class Add extends Domain.Command("Add", {
  target: Id,
  input: { amount: Schema.Number },
  events: [Added],
  rejects: [Limit],
  reads: []
}) {}
class Counter extends Domain.Aggregate("Counter", {
  id: Id,
  state: State,
  initial: null,
  commands: [Add],
  indexes: Domain.Index(Id, State, { by_value: ["value"] }),
  scope: (state) => state?.scope ?? null,
  decide: {
    Add: (state, command, ctx) =>
      (state?.value ?? 0) + command.amount > 10
        ? new Limit()
        : [new Added({ amount: command.amount, scope: Domain.Scope.privateScope(ctx.principal) })]
  },
  evolve: { Added: (state, event) => ({ value: (state?.value ?? 0) + event.amount, scope: event.scope }) }
}) {}

const principal = Domain.Scope.Principal.make("browser-fixture")
const offline = new Client.TransportError({ reason: "offline", cause: "injected network outage" })
const disconnected: Client.Transport = {
  execute: () => Effect.fail(offline),
  pull: () => Effect.fail(offline),
  changes: Stream.never
}
const options = (profile: string, transport = disconnected) => ({
  profile,
  principal,
  aggregates: [Counter],
  transport,
  autoConnect: false
})
const add = (client: Client.Client, amount: number) => client.execute(Counter, new Add({ target: "one", amount }))
const value = (client: Client.Client) => client.query((read) => read.get(Counter, "one")?.value ?? 0)
const check = (condition: boolean, message: string) =>
  Effect.sync(() => {
    if (!condition) throw new Error(message)
  })
const snapshot = (amount: number, seq = 1, epoch = "one"): PullResponse => ({
  epoch,
  scopes: [
    {
      scope: `user:${principal}`,
      cursor: seq,
      reset: true,
      entries:
        amount === 0
          ? []
          : [
              {
                _tag: "Upsert",
                aggregate: "Counter",
                id: "one",
                seq,
                state: { value: amount, scope: `user:${principal}` }
              }
            ]
    }
  ]
})

// INV-CLIENT-1 catches publishing before the journal commit and deleting the journal on cache reset.
const prepareReload = (profile: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const client = yield* Client.open(options(profile)).pipe(Effect.provideService(Scope.Scope, scope))
    // A redundant fallible write after committing the command must not turn success into an ambiguous failure.
    const original = IDBDatabase.prototype.transaction
    let journalWrites = 0
    Object.defineProperty(IDBDatabase.prototype, "transaction", {
      configurable: true,
      writable: true,
      value: function (this: IDBDatabase, ...args: Parameters<typeof original>) {
        const tx = original.call(this, ...args)
        if (Str.startsWith("echo:journal:")(this.name) && args[1] === "readwrite" && ++journalWrites === 2)
          queueMicrotask(() => tx.abort())
        return tx
      }
    })
    yield* Effect.gen(function* () {
      const accepted = yield* add(client, 3)
      yield* check(accepted._tag === "Pending", "durable append was reported as failed")
      const rejected = yield* add(client, 8)
      yield* check(
        rejected._tag === "Rejected" && Schema.is(Limit)(rejected.rejection),
        "next command used stale effective state or lost typed rejection"
      )
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          IDBDatabase.prototype.transaction = original
        })
      )
    )
    yield* check(value(client) === 3, "optimistic state was not immediately available")
    // Deliberately do not close: the browser navigation kills this resource, like an abrupt page reload.
    return "PASS INV-CLIENT-1 prepare: durable offline +3"
  })
const verifyReload = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const client = yield* Client.open(options(profile))
      yield* check(value(client) === 3, "reload lost pending work")
      yield* client.resetReplica
      yield* check(value(client) === 3, "cache reset erased journal")
      return "PASS INV-CLIENT-1 reload: offline value 3, independent replica reset preserved value 3"
    })
  )

// INV-CLIENT-2 catches pulling a committed operation before its lost receipt is resolved (3 becomes 6).
const lostReceipt = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      let committed = false
      let lose = true
      const requests: Array<ExecuteRequest> = []
      const transport: Client.Transport = {
        changes: Stream.never,
        pull: () => Effect.succeed(snapshot(committed ? 3 : 0, committed ? 1 : 0)),
        execute: (request) =>
          Effect.gen(function* () {
            requests.push(request)
            committed = true
            if (lose) {
              lose = false
              return yield* Effect.fail(offline)
            }
            return {
              epoch: "one",
              receipts: Arr.map(request.batches, (batch) => ({
                _tag: "Accepted",
                batchId: batch.id,
                seqs: [{ aggregate: "Counter", id: "one", seq: 1 }]
              }))
            }
          })
      }
      const beforeReload = yield* Scope.make()
      const first = yield* Client.open(options(profile, transport)).pipe(
        Effect.provideService(Scope.Scope, beforeReload)
      )
      yield* add(first, 3)
      yield* Effect.ignore(first.sync)
      yield* check(value(first) === 3, "lost receipt changed optimistic value")
      yield* Scope.close(beforeReload, Exit.void)
      const client = yield* Client.open(options(profile, transport))
      yield* check(value(client) === 3, "reopening lost uncertain intent")
      yield* client.sync
      yield* check(value(client) === 3, "recovered receipt double-applied prediction")
      yield* check(
        requests.length === 2 &&
          requests[0]?.clientId === requests[1]?.clientId &&
          requests[0]?.batches[0]?.id === requests[1]?.batches[0]?.id,
        "retry changed intent identity"
      )
      const status = yield* Stream.runHead(client.status)
      yield* check(status._tag === "Some" && status.value.pending === 0, "covered receipt not retired")
      return "PASS INV-CLIENT-2 lost reply: same client/batch retried, authoritative value 3 (not 6), pending 0"
    })
  )

// INV-CLIENT-3 catches discarding an unsent rejection, replaying it, and resending uncertainty across epochs.
const rebaseAndEpoch = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      let remote = 0
      let epoch = "one"
      let sends = 0
      let reusedCursor = false
      const transport: Client.Transport = {
        changes: Stream.never,
        pull: (request) =>
          Effect.suspend(() => {
            if (request.epoch === "two" && Record.keys(request.cursors).length !== 0) reusedCursor = true
            return request.epoch !== null && request.epoch !== epoch
              ? Effect.fail(new ResyncRequired({ epoch }))
              : Effect.succeed(snapshot(remote, remote === 0 ? 0 : 1, epoch))
          }),
        execute: (request) =>
          Effect.gen(function* () {
            if (request.epoch !== epoch) return yield* Effect.fail(new ResyncRequired({ epoch }))
            sends++
            return yield* Effect.fail(offline)
          })
      }
      const client = yield* Client.open(options(profile, transport))
      yield* add(client, 3)
      remote = 8
      yield* client.sync
      yield* check(value(client) === 8 && sends === 0, "unsent conflict was sent or still predicted")
      const notices = yield* Stream.runHead(client.notices)
      yield* check(
        notices._tag === "Some" &&
          notices.value[0]?.phase === "Rejected" &&
          Schema.is(Schema.Struct({ _tag: Schema.Literal("Limit") }))(notices.value[0].rejection),
        "rebase rejection not preserved"
      )
      yield* add(client, 1)
      yield* Effect.ignore(client.sync)
      yield* add(client, 1) // Definitely unsent work must remain usable when the epoch reset's cache write fails.
      epoch = "two"
      // Browser platform fault injection: abort exactly the replica reset transaction, not the journal.
      const original = IDBDatabase.prototype.transaction
      let abortReplica = true
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          Object.defineProperty(IDBDatabase.prototype, "transaction", {
            configurable: true,
            writable: true,
            value: function (this: IDBDatabase, ...args: Parameters<typeof original>) {
              const tx = original.call(this, ...args)
              if (abortReplica && Str.startsWith("echo:replica:")(this.name) && args[1] === "readwrite") {
                abortReplica = false
                queueMicrotask(() => tx.abort())
              }
              return tx
            }
          })
        }),
        () =>
          Effect.sync(() => {
            IDBDatabase.prototype.transaction = original
          })
      )
      // Execute's expected epoch is refused before the operation can run under the new authority.
      yield* Effect.ignore(client.sync)
      yield* check(value(client) === 1, "failed cache reset hid unsent work")
      const refused = yield* add(client, 10)
      yield* check(
        refused._tag === "Rejected" && Schema.is(Limit)(refused.rejection),
        "write after cache failure decided against empty state"
      )
      yield* Effect.ignore(client.sync)
      yield* check(!reusedCursor && !abortReplica, "retry reused old authority cursors after cache failure")
      const uncertain = yield* Stream.runHead(client.notices)
      yield* check(
        uncertain._tag === "Some" &&
          Arr.some(
            uncertain.value,
            (notice) =>
              notice.phase === "Unknown" && Schema.is(Schema.Struct({ amount: Schema.Literal(1) }))(notice.command)
          ),
        "epoch lost uncertain command"
      )
      yield* check(value(client) === 9 && sends === 2, "epoch replayed uncertain command or dropped unsent work")
      const changes = yield* SubscriptionRef.make<Change>({ _tag: "Keepalive", epoch: "three" })
      let authority = "three"
      const automatic = yield* Client.open({
        ...options(`${profile}-keepalive`, {
          ...disconnected,
          changes: SubscriptionRef.changes(changes),
          pull: (request) =>
            request.epoch !== null && request.epoch !== authority
              ? Effect.fail(new ResyncRequired({ epoch: authority }))
              : Effect.succeed(snapshot(authority === "three" ? 3 : 8, 1, authority))
        }),
        autoConnect: true
      })
      yield* automatic
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((n) => n === 3),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      authority = "four"
      yield* SubscriptionRef.set(changes, { _tag: "Keepalive", epoch: authority } satisfies Change)
      yield* automatic
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((n) => n === 8),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      return "PASS INV-CLIENT-3 rebase/epoch: rejection retained, uncertainty quarantined, failed replica reset retries safely, changed Keepalive resyncs"
    })
  )

// INV-CLIENT-4 catches lock release before resource close and user/profile cross-contamination.
const lifetime = (profile: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const started = yield* Deferred.make<void>()
    let starts = 0
    let stops = 0
    const transport: Client.Transport = {
      ...disconnected,
      pull: () => Effect.never,
      changes: Stream.unwrap(
        Effect.acquireRelease(
          Effect.gen(function* () {
            starts++
            yield* Deferred.succeed(started, undefined)
          }),
          () =>
            Effect.sync(() => {
              stops++
            })
        ).pipe(Effect.as(Stream.never))
      )
    }
    const first = yield* Client.open({ ...options(profile, transport), autoConnect: true }).pipe(
      Effect.provideService(Scope.Scope, scope)
    )
    yield* Deferred.await(started).pipe(Effect.timeout("2 seconds"))
    const consumers = yield* Effect.all(
      [
        Stream.runDrain(first.status),
        Stream.runDrain(first.notices),
        Stream.runDrain(first.subscribe((read) => read.scan(Counter)))
      ],
      { concurrency: "unbounded" }
    ).pipe(Effect.forkChild)
    yield* add(first, 2)
    const busy = yield* Effect.scoped(Client.open(options(profile))).pipe(Effect.flip)
    yield* check(busy._tag === "ProfileBusy", "second opener did not fail ProfileBusy")
    yield* Effect.scoped(
      Effect.gen(function* () {
        const other = yield* Client.open({
          ...options(profile),
          principal: Domain.Scope.Principal.make("other-browser-user")
        })
        yield* check(value(other) === 0, "principal reused another journal")
      })
    )
    yield* Scope.close(scope, Exit.void)
    yield* Fiber.join(consumers).pipe(Effect.timeout("2 seconds"))
    yield* check(starts === 1 && stops === 1, "automatic connection was duplicated or not stopped")
    const closed = yield* add(first, 1).pipe(Effect.flip)
    yield* check(closed._tag === "ClientClosed", "closed client accepted a command")
    yield* Effect.scoped(
      Effect.gen(function* () {
        const reopened = yield* Client.open(options(profile))
        yield* check(value(reopened) === 2, "new owner could not recover old journal")
      })
    )
    return "PASS INV-CLIENT-4 lifetime: one automatic connection starts/stops; streams end; busy/isolation/closed writes/reopen 2"
  })

// INV-CLIENT-5 catches a network-held local-write lock and unnecessary query emissions.
const queries = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const client = yield* Client.open(options(profile, { ...disconnected, pull: () => Effect.never }))
      const values: Array<number> = []
      yield* client
        .subscribe((read) => read.scan(Counter).length)
        .pipe(
          Stream.runForEach((n) =>
            Effect.sync(() => {
              values.push(n)
            })
          ),
          Effect.forkScoped
        )
      yield* Effect.yieldNow
      const syncing = yield* client.sync.pipe(Effect.forkScoped)
      yield* add(client, 2).pipe(Effect.timeout("2 seconds"))
      yield* Effect.yieldNow
      yield* add(client, 1).pipe(Effect.timeout("2 seconds"))
      yield* Effect.yieldNow
      yield* check(value(client) === 3, "hung network blocked local writes")
      yield* check(values.length === 2 && values[0] === 0 && values[1] === 1, "unchanged selector emitted")
      yield* check(
        client.query((read) => read.index(Counter.by_value, 3)).length === 1,
        "typed index missed effective state"
      )
      yield* Fiber.interrupt(syncing)
      return "PASS INV-CLIENT-5 queries: network stalled, local value 3, count emissions [0,1], effective index hit"
    })
  )

// INV-SYNC-1 runs against actual HTTP RPC and disposable Postgres, including a browser navigation.
const prepareServerReload = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const http = yield* Client.httpTransport(`${location.origin}/sync`)
      const online = yield* Client.open({
        profile: `${profile}-writer`,
        principal,
        aggregates: [Tag, Note],
        transport: http,
        autoConnect: false
      })
      const tagId = TagId.make(profile)
      yield* online.execute(Tag, new DefineTag({ target: tagId, name: profile }))
      yield* online.sync
      return "PASS INV-SYNC-1 cached real-server tag"
    })
  )
const pendingServerWrite = (profile: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const client = yield* Client.open({
      profile: `${profile}-writer`,
      principal,
      aggregates: [Tag, Note],
      transport: yield* Client.httpTransport(`${location.origin}/sync`).pipe(Effect.provideService(Scope.Scope, scope)),
      autoConnect: false
    }).pipe(Effect.provideService(Scope.Scope, scope))
    const outcome = yield* client.execute(
      Note,
      new WriteNote({ target: NoteId.make(profile), tagId: TagId.make(profile), text: "durable browser note" })
    )
    yield* check(outcome._tag === "Pending", "offline real-server fixture write failed local decision")
    return "PASS INV-SYNC-1 prepared: real server tag cached, offline note durable"
  })

const verifyServerReload = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const http = yield* Client.httpTransport(`${location.origin}/sync`)
      const client = yield* Client.open({
        profile: `${profile}-writer`,
        principal,
        aggregates: [Tag, Note],
        transport: http,
        autoConnect: false
      })
      yield* check(
        client.query((read) => read.get(Note, NoteId.make(profile)))?.text === "durable browser note",
        "real-server offline reload lost note"
      )
      if (!navigator.onLine) return "PASS INV-SYNC-1 physically offline reload hydrated pending note"
      const other = yield* Client.open({
        profile: `${profile}-reader`,
        principal,
        aggregates: [Tag, Note],
        transport: http
      })
      yield* other.sync
      yield* check(
        other.query((read) => read.get(Note, NoteId.make(profile))) === null,
        "note reached authority before reconnect"
      )
      yield* client.sync
      yield* other
        .subscribe((read) => read.get(Note, NoteId.make(profile)))
        .pipe(
          Stream.filter((note) => note?.text === "durable browser note"),
          Stream.runHead,
          Effect.timeout("5 seconds")
        )
      yield* check(
        other.query((read) => read.get(Note, NoteId.make(profile)))?.text === "durable browser note",
        "independent device did not converge through PG"
      )
      // Race after Pull but before Execute: only the server can reject this locally accepted create.
      let race = true
      const racing: Client.Transport = {
        ...http,
        execute: (request) =>
          Effect.gen(function* () {
            if (race) {
              race = false
              yield* http.execute({
                epoch: request.epoch,
                clientId: `${profile}-racer`,
                batches: [
                  {
                    id: "race",
                    commands: [
                      {
                        aggregate: "Tag",
                        command: { _tag: "DefineTag", target: `${profile}-winner`, name: `${profile}-race` }
                      }
                    ]
                  }
                ]
              })
            }
            return yield* http.execute(request)
          })
      }
      const loser = yield* Client.open({
        profile: `${profile}-loser`,
        principal,
        aggregates: [Tag, Note],
        transport: racing,
        autoConnect: false
      })
      yield* loser.sync
      const optimistic = yield* loser.execute(
        Tag,
        new DefineTag({ target: TagId.make(`${profile}-loser`), name: `${profile}-race` })
      )
      yield* check(optimistic._tag === "Pending", "race did not start with local acceptance")
      yield* loser.sync
      const notices = yield* Stream.runHead(loser.notices)
      yield* check(
        notices._tag === "Some" &&
          notices.value[0]?.receipt?._tag === "Rejected" &&
          Schema.is(Schema.TaggedStruct("DuplicateName", { existing: Schema.String }))(notices.value[0].rejection),
        "authoritative rejection not retained"
      )
      const unauthorized = yield* Client.httpTransport(`${location.origin}/unauthorized`)
      const auth = yield* unauthorized.pull({ epoch: null, cursors: {} }).pipe(Effect.flip)
      yield* check(
        auth._tag === "TransportError" && auth.reason === "auth",
        "HTTP 401 not distinguished from offline/error"
      )
      const counts: Array<string> = []
      // Standalone browser scenario entry point; this client only reads the fixture's request counter.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      const counterHttp = yield* HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer))
      const requestCount = counterHttp.get(`${location.origin}/auth-count`).pipe(
        Effect.flatMap((response) => response.json),
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Int))
      )
      // Catch retrying either a failed sync or a failed Changes stream. Each denial is real HTTP 401.
      for (const denied of ["pull", "execute", "changes"]) {
        const authProfile = `${profile}-auth-${denied}`
        const target = TagId.make(authProfile)
        const authOptions = { profile: authProfile, principal, aggregates: [Tag, Note] }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const seed = yield* Client.open({ ...authOptions, transport: http, autoConnect: false })
            if (denied !== "changes") yield* seed.execute(Tag, new DefineTag({ target, name: authProfile }))
          })
        )
        const blockedScope = yield* Scope.make()
        const blocked = yield* Client.open({
          ...authOptions,
          transport: {
            pull: denied === "pull" ? unauthorized.pull : http.pull,
            execute: denied === "execute" ? unauthorized.execute : http.execute,
            changes: denied === "changes" ? unauthorized.changes : Stream.never
          }
        }).pipe(Effect.provideService(Scope.Scope, blockedScope))
        yield* Effect.gen(function* () {
          yield* blocked.status.pipe(
            Stream.filter((s) => s.connection === "auth"),
            Stream.runHead,
            Effect.timeout("5 seconds")
          )
          const before = yield* requestCount
          // New local work must not implicitly resume an auth-blocked connection.
          if (denied === "changes") yield* blocked.execute(Tag, new DefineTag({ target, name: authProfile }))
          yield* blocked.execute(Tag, new DefineTag({ target: TagId.make(`${target}-later`), name: `${target}-later` }))
          yield* Effect.sleep("3500 millis")
          const after = yield* requestCount
          yield* check(after === before, `${denied} auth retried automatically: ${before} -> ${after} in 3500ms`)
          const status = yield* Stream.runHead(blocked.status)
          yield* check(
            status._tag === "Some" && status.value.connection === "auth" && status.value.pending === 2,
            `${denied} auth lost pending work or status`
          )
          counts.push(`${denied} ${before}->${after}/3500ms`)
          if (denied !== "changes") {
            const retry = yield* blocked.sync.pipe(Effect.flip)
            yield* check(retry._tag === "TransportError" && retry.reason === "auth", "explicit retry lost auth error")
            const explicit = yield* requestCount
            yield* Effect.sleep("1500 millis")
            yield* check(
              explicit === after + 1 && (yield* requestCount) === explicit,
              "explicit sync restarted automatic auth retries"
            )
          }
        }).pipe(Effect.ensuring(Scope.close(blockedScope, Exit.void)))
        // Restoring authenticated transport and reopening deliberately starts automation again.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const resumed = yield* Client.open({ ...authOptions, transport: http })
            yield* check(
              resumed.query((read) => read.get(Tag, target))?.name === authProfile,
              "auth close/reopen lost durable intent"
            )
            yield* resumed.status.pipe(
              Stream.filter((s) => s.connection === "idle" && s.pending === 0 && s.lastSynced !== null),
              Stream.runHead,
              Effect.timeout("5 seconds")
            )
            yield* other.sync
            yield* check(
              other.query((read) => read.get(Tag, target))?.name === authProfile &&
                other.query((read) => read.get(Tag, TagId.make(`${target}-later`)))?.name === `${target}-later`,
              "authenticated reopen did not commit retained intents"
            )
          })
        )
      }
      return `PASS INV-SYNC-1 actual HTTP/PG: offline reload converges; rejection retained; auth stops (${Arr.join(counts, ", ")}); 2 durable intents per case resume on authenticated reopen`
    })
  )

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)
Object.assign(globalThis, {
  echoScenarios: {
    prepareServerReload: (profile: string) => run(prepareServerReload(profile)),
    pendingServerWrite: (profile: string) => run(pendingServerWrite(profile)),
    verifyServerReload: (profile: string) => run(verifyServerReload(profile)),
    prepareReload: (profile: string) => run(prepareReload(profile)),
    verifyReload: (profile: string) => run(verifyReload(profile)),
    lostReceipt: (profile: string) => run(lostReceipt(profile)),
    rebaseAndEpoch: (profile: string) => run(rebaseAndEpoch(profile)),
    lifetime: (profile: string) => run(lifetime(profile)),
    queries: (profile: string) => run(queries(profile))
  }
})
