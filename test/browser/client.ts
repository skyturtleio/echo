import * as Arr from "effect/Array"
import * as Clock from "effect/Clock"
import * as DateTime from "effect/DateTime"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
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
import {
  ResyncRequired,
  type Change,
  type ExecuteRequest,
  type PullResponse,
  type Receipt
} from "../../src/protocol.ts"

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

const ReferenceState = Schema.NullOr(
  Schema.Struct({
    scope: Domain.Scope.Scope,
    key: Schema.Struct({ group: Schema.String }),
    at: Schema.DateTimeUtc
  })
)
class Reference extends Domain.Aggregate("Reference", {
  id: Id,
  state: ReferenceState,
  initial: null,
  commands: [],
  indexes: Domain.Index(Id, ReferenceState, { by_key: ["key"] }),
  scope: (state) => state?.scope ?? null,
  decide: {},
  evolve: {}
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
  revision: seq,
  grants: [],
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
                revision: 1,
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
              notice.phase === "Unknown" &&
              Schema.is(Schema.Struct({ amount: Schema.Literal(1) }))(notice.commands[0]?.command)
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
      // A failed journal reconciliation must not mark the new replica as fully published.
      for (const accepted of [false, true]) {
        let remoteValue = 0
        const recovery = yield* Client.open(
          options(`${profile}-recovery-${accepted}`, {
            changes: Stream.never,
            pull: () => Effect.succeed(snapshot(remoteValue, remoteValue === 0 ? 0 : 1)),
            execute: (request) =>
              Effect.sync(() => {
                remoteValue = 3
                return {
                  epoch: "one",
                  receipts: Arr.map(
                    request.batches,
                    (batch) =>
                      ({
                        _tag: "Accepted",
                        batchId: batch.id,
                        revision: 1,
                        seqs: [{ aggregate: "Counter", id: "one", seq: 1 }]
                      }) satisfies Receipt
                  )
                }
              })
          })
        )
        yield* add(recovery, 3)
        if (!accepted) remoteValue = 8
        const transaction = IDBDatabase.prototype.transaction
        let armed = false
        let aborted = false
        Object.defineProperty(IDBDatabase.prototype, "transaction", {
          configurable: true,
          writable: true,
          value: function (this: IDBDatabase, ...args: Parameters<typeof transaction>) {
            const tx = transaction.call(this, ...args)
            if (args[1] === "readwrite" && remoteValue !== 0) {
              if (Str.startsWith("echo:replica:")(this.name)) armed = true
              else if (armed && !aborted && Str.startsWith("echo:journal:")(this.name)) {
                aborted = true
                queueMicrotask(() => tx.abort())
              }
            }
            return tx
          }
        })
        yield* recovery.sync.pipe(
          Effect.ignore,
          Effect.ensuring(
            Effect.sync(() => {
              IDBDatabase.prototype.transaction = transaction
            })
          )
        )
        yield* check(aborted && value(recovery) === remoteValue, "failed journal reconciliation hid durable replica")
        yield* add(recovery, 1)
        yield* check(value(recovery) === remoteValue + 1, "recovery duplicated a covered prediction or used stale rows")
        const recoveredNotices = yield* Stream.runHead(recovery.notices)
        yield* check(
          accepted || (recoveredNotices._tag === "Some" && recoveredNotices.value[0]?.phase === "Rejected"),
          "failed authoritative rejection was forgotten by local recovery"
        )
      }
      // Full-size generic catalog, not private app data. Check every row independently of reconciliation.
      const catalog = "catalog:fixture"
      const other = "catalog:other"
      const count = 84_780
      const upsert = (
        id: string,
        amount: number,
        scope = catalog
      ): PullResponse["scopes"][number]["entries"][number] => ({
        _tag: "Upsert",
        aggregate: "Counter",
        id,
        seq: 1,
        state: { value: amount, scope }
      })
      let reply: PullResponse = {
        epoch: "catalog",
        revision: 1,
        grants: [],
        scopes: [
          {
            scope: catalog,
            cursor: 1,
            reset: true,
            entries: Array.from({ length: count }, (_, i) => upsert(`row:${i}`, i))
          }
        ]
      }
      let cursors: Readonly<Record<string, number>> = {}
      const catalogClient = yield* Client.open({
        ...options(`${profile}-catalog`, {
          ...disconnected,
          pull: (request) => {
            cursors = request.cursors
            return Effect.succeed(reply)
          }
        }),
        aggregates: [Counter, Tag]
      })
      const start = performance.now()
      yield* catalogClient.sync
      const elapsed = performance.now() - start
      const loaded = catalogClient.query((read) => read.scan(Counter))
      yield* check(
        loaded.length === count &&
          Arr.every(loaded, (row, i) => row.id === `row:${i}` && row.state?.value === i && row.state.scope === catalog),
        "catalog lost, reordered or corrupted rows"
      )
      // The former per-entry scan takes ~47s here; leave ample headroom over indexed sync (~0.6s).
      yield* check(elapsed < 5_000, `catalog reconciliation exceeded 5s: ${elapsed.toFixed(1)}ms`)
      // Replacements append, same ids in different aggregates coexist, and stale Gone/reset
      // from the old scope must not delete a stream that moved to the other scope.
      reply = {
        epoch: "catalog",
        revision: 2,
        grants: [],
        scopes: [
          {
            scope: catalog,
            cursor: 2,
            reset: true,
            entries: [
              upsert("a", 1),
              upsert("b", 2),
              upsert("c", 3),
              {
                _tag: "Upsert",
                aggregate: "Tag",
                id: "a",
                seq: 1,
                state: { name: "same id", removed: false, scope: catalog }
              }
            ]
          },
          { scope: other, cursor: 1, reset: false, entries: [upsert("b", 20, other)] },
          {
            scope: catalog,
            cursor: 3,
            reset: false,
            entries: [
              { _tag: "Gone", aggregate: "Counter", id: "b" },
              upsert("a", 10),
              upsert("a", 11),
              { _tag: "Gone", aggregate: "Counter", id: "c" },
              { _tag: "Gone", aggregate: "Counter", id: "c" },
              upsert("c", 30)
            ]
          }
        ]
      }
      yield* catalogClient.sync
      const reconciled = catalogClient.query((read) => read.scan(Counter))
      yield* check(
        reconciled.length === 3 &&
          reconciled[0]?.id === "b" &&
          reconciled[0].state?.value === 20 &&
          reconciled[1]?.id === "a" &&
          reconciled[1].state?.value === 11 &&
          reconciled[2]?.id === "c" &&
          reconciled[2].state?.value === 30 &&
          catalogClient.query((read) => read.get(Tag, TagId.make("a")))?.name === "same id",
        "Pull conflated identities, lost moved rows, or changed replacement order"
      )
      reply = {
        epoch: "catalog",
        revision: 3,
        grants: [],
        scopes: [
          // Move twice before resetting the old scope in this same Pull: rebuilding the
          // index on a later sync would hide stale membership left behind by replacement.
          { scope: catalog, cursor: 3, reset: false, entries: [upsert("b", 22)] },
          { scope: other, cursor: 1, reset: false, entries: [upsert("b", 20, other)] },
          { scope: catalog, cursor: 4, reset: true, entries: [upsert("d", 4)] },
          { scope: other, cursor: 1, reset: false, entries: [] },
          { scope: catalog, cursor: 5, reset: true, entries: [] }
        ]
      }
      yield* catalogClient.sync
      yield* check(
        catalogClient.query((read) => read.scan(Counter)).length === 1 &&
          catalogClient.query((read) => read.get(Counter, "b"))?.value === 20,
        "reset deleted another scope or retained an earlier block"
      )
      reply = {
        epoch: "catalog",
        revision: 4,
        grants: [],
        scopes: [
          {
            scope: other,
            cursor: 2,
            reset: false,
            entries: [
              upsert("b", 21, other),
              { _tag: "Upsert", aggregate: "Counter", id: "bad", seq: 1, state: "invalid" },
              { _tag: "Gone", aggregate: "Counter", id: "bad" }
            ]
          }
        ]
      }
      const invalid = yield* catalogClient.sync.pipe(Effect.flip)
      yield* check(
        invalid._tag === "ClientError" &&
          cursors[catalog] === 5 &&
          catalogClient.query((read) => read.get(Counter, "b"))?.value === 20,
        "invalid overwritten entry escaped validation or partially published"
      )
      reply = {
        epoch: "catalog",
        revision: 5,
        grants: [],
        scopes: [{ scope: catalog, cursor: 6, reset: false, entries: [] }]
      }
      yield* catalogClient.sync
      yield* check(
        cursors[other] === 1 && catalogClient.query((read) => read.scan(Counter)).length === 0,
        "failed Pull advanced cursor or absent scope retained rows"
      )
      return `PASS INV-CLIENT-3 rebase/epoch and ordered scope reconciliation: ${count} catalog rows verified; sync ${elapsed.toFixed(1)}ms`
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
    yield* Effect.scoped(
      Effect.gen(function* () {
        const peer = yield* Client.open(options(profile))
        yield* check(value(peer) === 2, "second tab lost durable effective state")
      })
    )
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
      const catalog = "catalog:query"
      let response: PullResponse = {
        epoch: "query",
        revision: 1,
        grants: [],
        scopes: [
          {
            scope: catalog,
            cursor: 1,
            reset: true,
            entries: Arr.map(["visible", "hidden"], (id) => ({
              _tag: "Upsert",
              aggregate: "Reference",
              id,
              seq: 1,
              // The persisted scope is deliberately misleading for the second row.
              state: {
                scope: id === "visible" ? catalog : "user:someone-else",
                key: { group: "first" },
                at: "2026-10-30T00:00:00.000Z"
              }
            }))
          }
        ]
      }
      const references = yield* Client.open({
        ...options(`${profile}-references`, {
          ...disconnected,
          pull: () => Effect.succeed(response)
        }),
        aggregates: [Counter, Reference]
      })
      yield* references.sync
      const before = references.query((read) => read)
      const first = before.get(Reference, "visible")
      yield* check(
        first !== null &&
          DateTime.toPartsUtc(first.at).year === 2026 &&
          before.get(Reference, "hidden") === null &&
          before.scan(Reference).length === 1 &&
          before.index(Reference.by_key, { group: "first" }).length === 1,
        "cached reads changed decoded scope, structural index equality or DateTime behavior"
      )
      const emitted: Array<ReadonlyArray<{ readonly id: string; readonly state: typeof ReferenceState.Type }>> = []
      yield* references
        .subscribe((read) => read.scan(Reference))
        .pipe(
          Stream.runForEach((rows) =>
            Effect.sync(() => {
              emitted.push(rows)
            })
          ),
          Effect.forkScoped
        )
      yield* Effect.yieldNow
      response = {
        epoch: "query",
        revision: 1,
        grants: [],
        scopes: [{ scope: catalog, cursor: 1, reset: false, entries: [] }]
      }
      const savedBefore = Arr.filter(
        (yield* references.diagnostics).operations,
        (op) => op.operation === "replica" || op.operation === "journal"
      ).length
      yield* references.sync
      yield* check(
        Arr.filter(
          (yield* references.diagnostics).operations,
          (op) => op.operation === "replica" || op.operation === "journal"
        ).length === savedBefore,
        "unchanged pull rewrote durable snapshots"
      )
      response = {
        epoch: "query",
        revision: 2,
        grants: [],
        scopes: [
          {
            scope: catalog,
            cursor: 2,
            reset: false,
            entries: [
              {
                _tag: "Upsert",
                aggregate: "Reference",
                id: "visible",
                seq: 2,
                state: { scope: catalog, key: { group: "second" }, at: "2027-01-01T00:00:00.000Z" }
              }
            ]
          }
        ]
      }
      yield* references.sync
      yield* Effect.yieldNow
      yield* check(
        before.get(Reference, "visible")?.key.group === "first" &&
          references.query((read) => read.get(Reference, "visible"))?.key.group === "second" &&
          references.query((read) => read.index(Reference.by_key, { group: "first" })).length === 0 &&
          references.query((read) => read.index(Reference.by_key, { group: "second" })).length === 1 &&
          emitted.length === 2 &&
          emitted[0]?.[0]?.state?.key.group === "first" &&
          emitted[1]?.[0]?.state?.key.group === "second",
        "snapshot cache returned stale state/index or mutated a retained snapshot"
      )
      yield* references.resetReplica
      yield* check(
        references.query((read) => read.scan(Reference)).length === 0 && before.scan(Reference).length === 1,
        "reset reused cached rows or changed a retained snapshot"
      )
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
          globalThis.dispatchEvent(new Event("online"))
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
      // Browser resource accounting catches successful callbacks leaking listeners despite no further traffic.
      const listeners = new Set<EventListenerOrEventListenerObject>()
      const originalAdd = globalThis.addEventListener
      const originalRemove = globalThis.removeEventListener
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          Object.defineProperty(globalThis, "addEventListener", {
            configurable: true,
            writable: true,
            value: (
              type: string,
              listener: EventListenerOrEventListenerObject | null,
              listenerOptions?: boolean | AddEventListenerOptions
            ) => {
              if (type === "online" && listener !== null) listeners.add(listener)
              if (listener !== null) originalAdd.call(globalThis, type, listener, listenerOptions)
            }
          })
          Object.defineProperty(globalThis, "removeEventListener", {
            configurable: true,
            writable: true,
            value: (
              type: string,
              listener: EventListenerOrEventListenerObject | null,
              listenerOptions?: boolean | EventListenerOptions
            ) => {
              if (type === "online" && listener !== null) listeners.delete(listener)
              if (listener !== null) originalRemove.call(globalThis, type, listener, listenerOptions)
            }
          })
        }),
        () =>
          Effect.sync(() => {
            for (const listener of listeners) originalRemove.call(globalThis, "online", listener)
            globalThis.addEventListener = originalAdd
            globalThis.removeEventListener = originalRemove
          })
      )
      let hintElapsed = 0
      for (const mode of ["online", "timeout", "close"]) {
        const retryScope = yield* Scope.make()
        let attempts = 0
        const retried = yield* Deferred.make<void>()
        const retrying = yield* Client.open({
          ...options(`${profile}-hint-${mode}`, {
            ...disconnected,
            pull: () =>
              Effect.gen(function* () {
                if (++attempts === 1) return yield* offline
                yield* Deferred.succeed(retried, undefined)
                return snapshot(0)
              })
          }),
          autoConnect: true
        }).pipe(Effect.provideService(Scope.Scope, retryScope))
        yield* retrying.status.pipe(
          Stream.filter((s) => s.connection === "offline"),
          Stream.runHead,
          Effect.timeout("2 seconds")
        )
        yield* Effect.sleep("50 millis")
        yield* check(listeners.size === 1, "reconnect did not install exactly one online listener")
        const hintStart = performance.now()
        if (mode === "online") globalThis.dispatchEvent(new Event("online"))
        if (mode !== "close") {
          yield* Deferred.await(retried).pipe(Effect.timeout(mode === "online" ? "500 millis" : "2 seconds"))
          if (mode === "online") hintElapsed = performance.now() - hintStart
        }
        yield* Scope.close(retryScope, Exit.void)
        yield* check(listeners.size === 0, `${mode} retry retained its browser listener`)
        const closedAttempts = attempts
        globalThis.dispatchEvent(new Event("online"))
        yield* Effect.sleep("50 millis")
        yield* check(attempts === closedAttempts, "online event restarted a closed client")
      }
      return `PASS INV-SYNC-1 actual HTTP/PG: offline reload converges; rejection retained; auth stops (${Arr.join(counts, ", ")}); 2 durable intents per case resume on authenticated reopen; online hint ${hintElapsed.toFixed(1)}ms; closed client stays stopped`
    })
  )

// INV-SYNC-1: catches the default 16Mi NDJSON limit and publishing only an in-memory catalog.
const largeCatalog = (profile: string, reload: boolean) =>
  Effect.scoped(
    Effect.gen(function* () {
      const http = yield* Client.httpTransport(`${location.origin}/sync`)
      let seenCursors: Readonly<Record<string, number>> = {}
      let received = -1
      const transport: Client.Transport = {
        ...http,
        pull: (request) => {
          seenCursors = request.cursors
          return http.pull(request).pipe(
            Effect.tap((reply) =>
              Effect.sync(() => {
                received = Arr.reduce(reply.scopes, 0, (n, s) => n + s.entries.length)
              })
            )
          )
        }
      }
      const client = yield* Client.open({ profile, principal, aggregates: [Tag, Note], transport, autoConnect: false })
      const verify = () => {
        const rows = client.query((read) =>
          Arr.filter(read.scan(Tag), (row) => row.state?.scope === "catalog:transport")
        )
        return (
          rows.length === 84_780 &&
          Arr.every(rows, (row) => {
            const i = Number(Str.slice("catalog:".length)(row.id))
            return (
              Number.isInteger(i) &&
              i >= 0 &&
              i < 84_780 &&
              row.state?.name === `${i}:${Str.repeat(380)("x")}` &&
              !row.state.removed
            )
          }) &&
          new Set(Arr.map(rows, (row) => row.id)).size === 84_780
        )
      }
      if (reload) yield* check(verify(), "offline navigation did not hydrate all durable catalog rows")
      const start = performance.now()
      yield* client.sync
      yield* check(verify(), "large HTTP catalog failed complete state validation")
      const elapsed = performance.now() - start
      if (reload)
        yield* check(
          received === 0 && (seenCursors["catalog:transport"] ?? 0) > 0,
          "durable cursor redownloaded catalog"
        )
      // Keep the same transport deadline; this is not a relaxed fixture timeout.
      yield* check(elapsed < 5_000, `large catalog sync exceeded 5s: ${elapsed.toFixed(1)}ms`)
      return `PASS INV-SYNC-1 ${reload ? "durable navigation/reopen, unchanged Pull" : "HTTP/PG catalog"}: 84780 exact rows, ${received} received, ${elapsed.toFixed(1)}ms`
    })
  )

// Same INV-SYNC-1 scenario: deterministic frame rejection must not become a download loop.
const oversizedCatalog = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const http = yield* Client.httpTransport(`${location.origin}/sync`)
      const oversized = yield* Client.httpTransport(`${location.origin}/oversized`)
      const base = { profile, principal, aggregates: [Tag, Note] }
      const target = TagId.make(`${profile}-pending`)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const seed = yield* Client.open({ ...base, transport: disconnected, autoConnect: false })
          yield* seed.execute(Tag, new DefineTag({ target, name: target }))
        })
      )
      // Standalone scenario's HTTP request counter, independent of the transport under test.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      const counterHttp = yield* HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer))
      const count = counterHttp.get(`${location.origin}/oversized-count`).pipe(
        Effect.flatMap((response) => response.json),
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Int))
      )
      let cursors: Readonly<Record<string, number>> = {}
      const before = yield* count
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.open({
            ...base,
            transport: {
              ...oversized,
              changes: Stream.never,
              pull: (request) => {
                cursors = request.cursors
                return oversized.pull(request)
              }
            }
          })
          const status = yield* client.status.pipe(
            Stream.filter((s) => s.connection === "oversized"),
            Stream.runHead,
            Effect.timeout("5 seconds")
          )
          const frameError = Schema.Struct({
            cause: Schema.TaggedStruct("RpcClientError", {
              reason: Schema.TaggedStruct("RpcClientDefect", {
                cause: Schema.TaggedStruct("MaxBufferSizeExceeded", { maxBufferSize: Schema.Literal(67_108_864) })
              })
            })
          })
          yield* check(
            status._tag === "Some" && Schema.is(frameError)(status.value.error),
            "oversized status lost parser cause/limit"
          )
          yield* client.execute(Tag, new DefineTag({ target: TagId.make(`${target}-later`), name: `${target}-later` }))
          globalThis.dispatchEvent(new Event("online"))
          yield* Effect.sleep("3500 millis")
          yield* check((yield* count) === before + 1, "oversized response automatically redownloaded")
          const retry = yield* client.sync.pipe(Effect.flip)
          yield* check(
            retry._tag === "TransportError" && retry.reason === "oversized",
            "explicit retry lost oversized error"
          )
          yield* Effect.sleep("1500 millis")
          yield* check((yield* count) === before + 2, "explicit sync restarted automatic oversized retries")
        })
      )
      let recoveredCursors: Readonly<Record<string, number>> = {}
      const reopened = yield* Client.open({
        ...base,
        autoConnect: false,
        transport: {
          ...http,
          pull: (request) => {
            recoveredCursors = request.cursors
            return Effect.fail(offline)
          }
        }
      })
      const rows = reopened.query((read) => read.scan(Tag))
      yield* check(
        Arr.filter(rows, (row) => row.state?.scope === "catalog:transport").length === 84_780 &&
          reopened.query((read) => read.get(Tag, target))?.name === target &&
          reopened.query((read) => read.get(Tag, TagId.make(`${target}-later`)))?.name === `${target}-later`,
        "oversized failure changed durable catalog or lost pending writes"
      )
      yield* Effect.ignore(reopened.sync)
      yield* check(
        Record.keys(recoveredCursors).length === Record.keys(cursors).length &&
          Arr.every(Record.keys(cursors), (key) => recoveredCursors[key] === cursors[key]) &&
          (cursors["catalog:transport"] ?? 0) > 0,
        "oversized failure advanced or deleted durable cursors"
      )
      return `PASS INV-SYNC-1 oversized: nested MaxBufferSizeExceeded/67108864; requests ${before}->${before + 1}/3500ms; explicit only +1/1500ms; 84780 durable rows/cursors and 2 pending intents preserved`
    })
  )

// INV-CLIENT-6 catches payload/error leakage, reset ages, unbounded history and counting initial calls as retries.
const diagnostics = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const secret = `${profile}-diagnostic-private-payload`
      const firstScope = yield* Scope.make()
      const first = yield* Client.open({ ...options(profile), aggregates: [Tag] }).pipe(
        Effect.provideService(Scope.Scope, firstScope)
      )
      yield* first.execute(Tag, new DefineTag({ target: TagId.make(profile), name: secret }))
      yield* Effect.ignore(first.sync)
      const before = yield* first.diagnostics
      yield* check(
        before.pending.count === 1 && before.failedOperationFollowups.pull === 0,
        "initial offline attempt counted as retry"
      )
      const clock = yield* Clock.Clock
      const backwards = yield* first.diagnostics.pipe(
        Effect.provideService(Clock.Clock, {
          ...clock,
          currentTimeMillis: Effect.succeed(0)
        })
      )
      yield* check(backwards.pending.oldestAgeMs === 0, "backward device clock produced negative pending age")
      yield* Scope.close(firstScope, Exit.void)
      yield* Effect.sleep("30 millis")
      const http = yield* Client.httpTransport(`${location.origin}/sync`)
      let failPull = true
      let failExecute = true
      let pullCount: number | null = null
      const transport: Client.Transport = {
        changes: Stream.never,
        pull: (request) =>
          Effect.gen(function* () {
            yield* Effect.sleep("20 millis")
            if (failPull)
              return yield* new Client.TransportError({
                reason: "offline",
                cause: { secret, url: "https://private/?token=secret" }
              })
            const response = yield* http.pull(request)
            pullCount = Arr.reduce(response.scopes, 0, (n, scope) => n + scope.entries.length)
            return response
          }),
        execute: (request) =>
          Effect.gen(function* () {
            yield* Effect.sleep("20 millis")
            if (failExecute) return yield* new Client.TransportError({ reason: "auth", cause: { secret } })
            return yield* http.execute(request)
          })
      }
      const client = yield* Client.open({ ...options(profile, transport), aggregates: [Note, Tag] })
      const reopened = yield* client.diagnostics
      yield* check(
        reopened.pending.count === 1 && (reopened.pending.oldestAgeMs ?? 0) >= (before.pending.oldestAgeMs ?? 0) + 20,
        "pending age reset on reopen"
      )
      yield* check(reopened.failedOperationFollowups.pull === 0, "session counters survived reopen")
      for (let i = 0; i < 105; i++) yield* Effect.ignore(client.sync)
      const bounded = yield* client.diagnostics
      yield* check(
        bounded.operations.length === 100 && bounded.failedOperationFollowups.pull === 104,
        "history unbounded or followups counted initial attempt"
      )
      yield* check(
        Arr.every(bounded.operations, (op) => op.followsFailure),
        "retained followup flags lost"
      )
      yield* check(
        Arr.every(
          bounded.operations,
          (op) => op.operation === "pull" && op.failure === "offline" && op.count === null && op.durationMs >= 15
        ),
        "failed pull timing/category/count incorrect"
      )
      failPull = false
      yield* Effect.ignore(client.sync)
      const denied = yield* client.diagnostics
      yield* check(
        denied.operations.at(-1)?.failure === "auth" && denied.failedOperationFollowups.execute === 0,
        "auth failure or initial execute count incorrect"
      )
      failExecute = false
      yield* client.sync
      const synced = yield* client.diagnostics
      yield* check(
        synced.pending.count === 0 &&
          synced.pending.oldestAgeMs === null &&
          synced.failedOperationFollowups.execute === 1 &&
          synced.failedOperationFollowups.pull === 105,
        "sync retirement/followups incorrect"
      )
      const execute = Option.getOrUndefined(Arr.findLast(synced.operations, (op) => op.operation === "execute"))
      const pull = Option.getOrUndefined(Arr.findLast(synced.operations, (op) => op.operation === "pull"))
      yield* check(
        execute?.count === 1 &&
          execute.failure === null &&
          execute.durationMs >= 15 &&
          pull?.count === pullCount &&
          pull?.failure === null,
        "real RPC response counts/timing incorrect"
      )
      yield* check(
        Arr.some(synced.operations, (op) => op.operation === "journal" && op.count === 0) &&
          Arr.some(synced.operations, (op) => op.operation === "replica" && (op.count ?? 0) > 0) &&
          Arr.some(synced.operations, (op) => op.operation === "rebase" && (op.count ?? 0) > 0),
        "persistence/rebase counts absent"
      )
      yield* check(
        Arr.every(
          synced.operations,
          (op) =>
            op.bytes === null &&
            Number.isFinite(op.durationMs) &&
            op.durationMs >= 0 &&
            Arr.join(Arr.sort(Record.keys(op), Str.Order), ",") ===
              "bytes,count,durationMs,failure,followsFailure,operation"
        ),
        "export contains extra fields or fabricated bytes"
      )
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(synced)
      yield* check(
        !Str.includes(secret)(encoded) &&
          !Str.includes(profile)(encoded) &&
          !Str.includes(principal)(encoded) &&
          !Str.includes("https://")(encoded),
        "diagnostics leaked private data"
      )
      // Returned objects are detached, even when a JS consumer disregards readonly types.
      if (synced.operations[0]) Object.assign(synced.operations[0], { count: 999999 })
      const unchanged = yield* client.diagnostics
      yield* check(
        (yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(unchanged)) === encoded,
        "snapshot read mutated history or triggered work"
      )
      // A broken diagnostic clock must neither block durable writes nor replace transport errors.
      for (const failAtStart of [true, false]) {
        let reads = 0
        const faultyClock = {
          ...clock,
          monotonicTimeNanos: Effect.suspend(() =>
            failAtStart || ++reads % 2 === 0 ? Effect.die(secret) : Effect.succeed(0n)
          )
        }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const isolated = yield* Client.open(options(`${profile}-clock-${failAtStart}`))
            yield* add(isolated, 3)
            const error = yield* Effect.flip(isolated.sync)
            yield* check(error === offline && value(isolated) === 3, "instrumentation changed operation outcome")
          }).pipe(Effect.provideService(Clock.Clock, faultyClock))
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const restored = yield* Client.open(options(`${profile}-clock-${failAtStart}`))
            yield* check(value(restored) === 3, "instrumentation lost or duplicated durable work")
          })
        )
      }
      return `PASS INV-CLIENT-6: 100 retained; 105 pull/1 execute failed-operation followups; durable pending age; real RPC receipts/entries; no private fields; detached read-only snapshot`
    })
  )

const batches = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const client = yield* Client.open(options(profile))
      const rejected = yield* client.batch([
        Client.command(Counter, new Add({ target: "one", amount: 4 })),
        Client.command(Counter, new Add({ target: "one", amount: 8 }))
      ])
      yield* check(
        rejected._tag === "Rejected" && value(client) === 0 && (yield* client.pending).length === 0,
        "batch published partial prediction"
      )
      const accepted = yield* client.batch([
        Client.command(Counter, new Add({ target: "one", amount: 2 })),
        Client.command(Counter, new Add({ target: "one", amount: 3 }))
      ])
      yield* check(
        accepted._tag === "Pending" && value(client) === 5 && (yield* client.pending).length === 1,
        "batch did not journal one ordered prediction"
      )
      yield* client.action("membership-transition", { destination: "private" })
      const pending = yield* client.pending
      yield* check(
        value(client) === 5 && pending.length === 2 && pending[1]?.action?.name === "membership-transition",
        "action predicted or lost identity"
      )
      const peer = yield* Client.open(options(profile))
      yield* check(value(peer) === 5 && (yield* peer.pending).length === 2, "journal hydration lost action or batch")
      return "PASS INV-CLIENT-7 atomic rollback/ordered prediction; one durable batch; unpredicted action survives reopen"
    })
  )

const ownership = (profile: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const first = yield* Client.open(options(profile))
      const second = yield* Client.open(options(profile))
      yield* Effect.all([add(first, 2), add(second, 3)], { concurrency: "unbounded" })
      yield* second
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((amount) => amount === 5),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      yield* check((yield* first.pending).length === 2, "concurrent tabs lost a durable intent")
      const refused = yield* Client.resetProfile({ profile, principal }).pipe(Effect.flip)
      yield* check(
        refused._tag === "PendingIntents" && refused.count === 2,
        "reset did not count peer's unresolved intents"
      )
      const resetStatus = yield* second.status.pipe(
        Stream.filter((status) => status.connection === "reset"),
        Stream.runHead,
        Effect.forkScoped
      )
      const withdrawn = yield* second
        .subscribe((read) => read.scan(Counter))
        .pipe(
          Stream.filter((rows) => rows.length === 0),
          Stream.runHead,
          Effect.forkScoped
        )
      const transaction = IDBDatabase.prototype.transaction
      Object.defineProperty(IDBDatabase.prototype, "transaction", {
        configurable: true,
        writable: true,
        value: function (this: IDBDatabase, ...args: Parameters<typeof transaction>) {
          const tx = transaction.call(this, ...args)
          if (Str.startsWith("echo:replica:")(this.name) && args[1] === "readwrite") queueMicrotask(() => tx.abort())
          return tx
        }
      })
      yield* Client.resetProfile({ profile, principal, discardPending: true }).pipe(
        Effect.flip,
        Effect.ensuring(
          Effect.sync(() => {
            IDBDatabase.prototype.transaction = transaction
          })
        )
      )
      yield* Fiber.join(resetStatus).pipe(Effect.timeout("3 seconds"))
      yield* Fiber.join(withdrawn).pipe(Effect.timeout("3 seconds"))
      yield* check(
        (yield* add(first, 1).pipe(Effect.flip))._tag === "ClientClosed",
        "stale peer repopulated reset journal"
      )
      const fresh = yield* Client.open(options(profile))
      yield* check(value(fresh) === 0 && (yield* fresh.pending).length === 0, "reset retained data")

      const ownerScope = yield* Scope.make()
      const followerScope = yield* Scope.make()
      let active = 0
      let max = 0
      let amount = 4
      let blocked = false
      const pulling = yield* Deferred.make<void>()
      const releasePull = yield* Deferred.make<void>()
      const pokes = yield* SubscriptionRef.make<Change>({ _tag: "Poke", epoch: "one" })
      const transport: Client.Transport = {
        ...disconnected,
        pull: () =>
          Effect.gen(function* () {
            if (blocked) {
              yield* Deferred.succeed(pulling, undefined)
              yield* Deferred.await(releasePull)
            }
            return snapshot(amount)
          }),
        changes: Stream.unwrap(
          Effect.acquireRelease(
            Effect.sync(() => {
              active++
              max = Math.max(max, active)
            }),
            () =>
              Effect.sync(() => {
                active--
              })
          ).pipe(Effect.as(SubscriptionRef.changes(pokes)))
        )
      }
      const owner = yield* Client.open({ ...options(`${profile}-network`, transport), autoConnect: true }).pipe(
        Effect.provideService(Scope.Scope, ownerScope)
      )
      const follower = yield* Client.open({ ...options(`${profile}-network`, transport), autoConnect: true }).pipe(
        Effect.provideService(Scope.Scope, followerScope)
      )
      yield* follower
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((n) => n === 4),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      blocked = true
      const inflight = yield* owner.sync.pipe(Effect.forkScoped)
      yield* Deferred.await(pulling).pipe(Effect.timeout("3 seconds"))
      let resetFinished = false
      const reset = yield* follower.resetReplica.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            resetFinished = true
          })
        ),
        Effect.forkScoped
      )
      yield* Effect.sleep("50 millis")
      yield* check(!resetFinished, "follower reset raced an in-flight owner Pull")
      blocked = false
      yield* Deferred.succeed(releasePull, undefined)
      yield* Fiber.join(inflight)
      yield* Fiber.join(reset)
      amount = 7
      yield* SubscriptionRef.update(pokes, (change) => ({ ...change }))
      yield* owner.requestSync
      yield* follower
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((n) => n === 7),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      yield* Scope.close(followerScope, Exit.void).pipe(Effect.timeout("3 seconds"))
      const successor = yield* Client.open({ ...options(`${profile}-network`, transport), autoConnect: true })
      yield* Scope.close(ownerScope, Exit.void).pipe(Effect.timeout("3 seconds"))
      amount = 8
      yield* successor.requestSync
      yield* successor
        .subscribe((read) => read.get(Counter, "one")?.value)
        .pipe(
          Stream.filter((n) => n === 8),
          Stream.runHead,
          Effect.timeout("3 seconds")
        )
      yield* check(max === 1, "competing network owners")
      return "PASS INV-CLIENT-8 concurrent journals, replica broadcasts, follower close, owner takeover, pending refusal and reset fencing"
    })
  )

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)
Object.assign(globalThis, {
  echoScenarios: {
    batches: (profile: string) => run(batches(profile)),
    ownership: (profile: string) => run(ownership(profile)),
    diagnostics: (profile: string) => run(diagnostics(profile)),
    largeCatalog: (profile: string, reload = false) => run(largeCatalog(profile, reload)),
    oversizedCatalog: (profile: string) => run(oversizedCatalog(profile)),
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
