# Echo — an aggregate-based, offline-first sync engine for Effect v4 + FoldKit

Status: design, 2026-10-29; cut to its five jobs 2026-10-30 (§2 item 13). This repository
(`skyturtleio/echo`, package `echo`) replaces `skyturtleio/echo-sync`, whose code and SPEC
are the previous design and are not references (§5). The vocabulary is `@alchemy.run/domain`'s.

This document has five jobs and nothing else: the **model** (§1, what the engine is), the
**decisions** (§2, why, with what each was chosen over), the **invariants catalogue** (§3, what
a rule-abiding engine promises, one scenario test each), the **phases** (§4) and the **working
rules** (§5). App rules are not described here; they are `Story` tests in each app's domain
package.

## 1. Model

**Who decides.** The server is authoritative and the device keeps a copy of the server's state.
Every rule is a pure `decide(state, command, ctx) → events | rejection` that runs on **both**
sides: the device runs it to answer immediately and offline, the server runs it to give the
final answer, and the device **rebases** its pending intents whenever confirmed state moves.
Device-decides logs (Effect `eventlog`, LiveStore) cannot refuse a write, and online-first
engines cannot write before their first sync; this design is the remaining quadrant.

**Domain package.** An app is a domain package (one block per file, a folder per aggregate,
importing only `effect` and `echo/domain`) plus thin server and client wiring. The blocks:

| Block | Shape |
|---|---|
| `Event` | `Schema.TaggedClass`; a fact, past tense, carries its computed outcome (`at` is in the event, `evolve` never calls `now()`) |
| `Rejection` | `Schema.TaggedError`; a *named* reason a command was refused, part of the contract, matched by the UI, never a string |
| `Command` | `Schema.TaggedClass` with `target` (stream id), `input`, the `events` it may produce, the rejections it may `reject` with, the other streams or indexes `decide` may `read` (§2 item 2), and later `authority: "both" \| "server"` |
| `Aggregate` | `id`, `state` schema (`null` while the stream has no events), `initial`, `commands`, `scope(state)`, `indexes`, `decide` and `evolve` as one handler per tag (§2 item 7) |
| `Index` | `Domain.Index(id, state, spec)`, field lists over state, bound to the aggregate and exposed as statics (§2 item 9) |
| `Scope` | `Principal`, `Scope` (opaque id), `privateScope`, `catalogScope`, the pre-grants rule, and `ScopeForbidden` (§2 item 8) |
| `Story` | the domain test: *given* events (one stream, or several seeded with `Story.stream(Aggregate, id, events)`), *when* command, *as* principal, *then* events or rejection; pure runner in `echo/domain`, vitest binding in `echo/domain/testing` (§2 item 10) |
| `Policy`, `Batch`, versioning | server-side reaction to events; several commands committed atomically under one id; `(name, version)` on events, state and commands with upcasters. Not built yet (§4) |

`decide` is pure and sees only its stream's state, the command and `ctx = { principal, now,
read }`; `evolve` is total; events are business-named; nothing in `evolve` reads a clock or
makes an id. `ctx.now` is the server's clock on the server and the device's on the device; the
confirmed event's `at` is what everyone converges on, so no rule orders by `now`.

**Write path.** Every write is a **batch** of commands, each addressed to one **stream**
(aggregate instance). The device decides the batch against *confirmed state ⊕ pending
batches*, appends it to a durable **journal**, updates in-memory **effective state** and
re-emits affected queries, without consulting connectivity. The server decides the same batch
in one `SERIALIZABLE` transaction, appends the events to each stream, writes a **receipt**
keyed by batch id (accepted *or* rejected), and publishes to the **scope log** of each touched
scope. Devices pull scope logs by cursor; on every confirmed change the leader rebases:
confirmed state advances, pending batches are re-decided in journal order (a batch that now
rejects becomes a notice), effective state is recomputed, queries re-emit. Because `decide` is
pure and pending batches are few, full replay is the whole algorithm; no merge logic lives in
the engine.

**Server (Postgres, one schema).**

| Table | Key | Holds |
|---|---|---|
| `streams` | `(aggregate, id)` | `scope`, `seq`, `state`, `state_version`: a cache of `fold(evolve, events)` |
| `events` | `(aggregate, id, seq)` | `name`, `version`, `payload`, `batch_id`, `principal`, `at`; append-only |
| `receipts` | `(client_id, batch_id)` | `Accepted {seqs}` \| `Rejected {rejection}` \| expired; `expires_at` |
| `scope_log` | `(scope, position)` | `Upsert {stream, seq, state}` \| `Event {stream, seq, event}` \| `Gone {stream}` |
| `grants` | `(scope, principal)` | `reader \| writer \| owner`, derived from the `Scope` aggregate's events |
| `policy_outbox` | `(event ref)` | events awaiting policies, at-least-once |
| `engine` | singleton | `epoch` |

`Execute { clientId, batches }`, per batch, one serializable transaction retried on
serialization failure: resolve the principal from the session (an app-provided layer; nothing
in the body is trusted for identity); return the existing receipt if `(clientId, batchId)` has
one; for each command load the stream, require a writer grant on `scope(state)` (or the
creation scope of a new stream) and that `authority` allows the server, run `decide`, on a
rejection abort the batch and write a `Rejected` receipt, else `evolve`, bump `seq`, append;
then write `streams`, write `Gone`+`Upsert` when the scope changed or the events otherwise,
write the `Accepted` receipt, queue for policies, poke connected devices holding a touched
scope.

`Pull { cursors }` returns, for every scope the principal holds, entries after the cursor; an
unseen scope starts at 0. The scope log is compacted: below a moving watermark only the latest
`Upsert` per stream survives, so a fresh device gets snapshot + tail, never history, and a
device whose cursor fell below the watermark re-pulls from 0 with its journal untouched.

**Transport** is plain HTTP over `effect/rpc`: `Execute` and `Pull` are request/response;
`Changes` is one long-lived streaming response per leader carrying only pokes and presence,
never data, with an inert server keepalive about every 25 s and client reconnect after about
twice that. Correctness lives in cursors and `Pull`; the stream only shortens latency. No
WebSockets, no client polling (§2 item 6).

**Authority epoch.** `engine.epoch` changes on restore-from-backup or operator reset. Every
reply carries it; a device holding another epoch gets `ResyncRequired`, discards cache and
cursors, keeps its journal, re-pulls and rebases.

**Policies** run only on the server, from the outbox, at least once, issuing commands under
deterministic batch ids (`policy:<event ref>`) so redelivery is a no-op via receipts.
**Grants are an aggregate** (`Scope`, commands `Grant`/`Revoke`, `authority: "server"`), so
every change to who sees what is an event; apps never write grant rows.

**Client.** One **leader** per profile (a `SharedWorker` with election and a dedicated-worker
fallback) and any number of **tabs**. The leader owns a durable journal (OPFS SQLite
`journal.db`: batches with phase `Pending | Sent | Confirmed(seqs) | Rejected(r) | Unknown`,
`epoch`, notices), a disposable durable cache (`replica.db`: confirmed stream rows with
generated columns for indexes, scope cursors) and hot memory (effective rows, indexes, the
subscription set). Journal and cache are separate files on purpose: a reset, an epoch change or
a state-version bump may delete the cache; the journal is deleted only by a confirmed receipt
or the user's explicit consent (sign-out with pending batches asks). A command with
`authority: "server"` is journaled without prediction and shows as pending. Nothing on the
execute → re-emit path touches disk synchronously; the journal append is write-ahead.

**Outcomes as data.** A batch sent without an answer is `Unknown`; the leader retries under the
same `batchId` until the receipt says `Accepted | Rejected`, or the server says the receipt
expired, in which case the batch becomes an `Unknown` notice and is never re-sent under a new
id.

**Queries** are pure functions over a `Read` interface (`get(aggregate, id)`,
`index(aggregate, name).range(...)`, `scan(aggregate)`), registered in the domain package. The
leader runs them over effective rows, records what they touched, re-runs on change and streams
results to tabs with structural equality so unchanged results do not re-emit.

**Scopes.** A scope is an opaque id (`user:<id>`, a shared id, `catalog:<name>`). Every stream
is in exactly one scope at a time, given by `Aggregate.scope(state)`. A grant on a scope
delivers every stream in it; `writer` may command them; `owner` may also grant. No per-row ACL
and no field-level filter: a field some readers must not see is its own aggregate in its own
scope. A scope change is an ordinary event; the server turns it into `Gone` for the old
scope's readers and `Upsert` for the new scope's (a reader who gains a scope gets the current
state, never the history they were not entitled to, §2 item 3). The engine enforces grants
once, generically, before `decide` (`ScopeForbidden`); `decide` enforces what the principal
may *say*. Catalogs are scopes with a `*` reader grant and server-only writes: an import writes
`Upsert` entries plus one `CatalogReleased { release }` event per release, not a stream per
row (§2 item 4).

**Conflicts are rules.** Every conflict is one of four shapes, each expressed in the domain,
not the engine: an *exclusive claim* (`decide` rejects the second claimant; the loser rebases
and sees the rejection); *independent fields* (the command carries a change set, the event
records it, `evolve` merges; same-field edits are last-writer-wins by server order); a *stale
base* (opt-in `expectedSeq`, `decide` returns `Stale`); a *vanished target or lost access*
(`NotFound` / `ScopeForbidden` from the server, surfaced as notices). Uniqueness and in-use
checks are `ctx.read` over an index inside `decide`: authoritative on the server because the
transaction is serializable, optimistic on the device and corrected by the rebase (§2 item 2).

**Schema evolution.** Events are immutable and versioned: a shape change is a new version plus
an `upcast` registered next to it; `evolve` sees only the latest. State is a derived cache with
a version: the server rebuilds a stream whose `state_version` is older by replay on first touch
and in a background sweep; the device treats a mismatch as "discard cache, re-pull", keeping
the journal; a periodic `Snapshot` event bounds replay. Read models are rebuilt from state. No
app-level DDL runs against event or state data; the only DDL is engine schema and index
promotion. Journaled commands are versioned too: one with no upcaster becomes an `Unknown`
notice. The server rejects clients below a `CompatibilityFloor`.

**Presence** is ephemeral: a room is a scope; `join(room, state)` and `track(state)` are small
requests; the server fans out `{ principal, clientId, state }` to the room's members over the
same `Changes` stream and expires the entry when that stream ends. One connection per leader
whether or not the app uses presence.

**FoldKit integration.** Apps use the framework's real branded `Command.define`,
`Subscription.make` and `ManagedResource` constructors. `Client.open` hydrates offline and
owns a scoped connection loop by default (`autoConnect: false` selects manual sync); there
is no public `connect` method or `echo/foldkit` adapter. Closing the client releases storage,
connection and profile lock and ends query/status/notice streams. Apps still gate late messages
by readiness and resource generation.

**Non-goals for v1:** collaborative text (CRDT) fields, per-row ACLs inside a scope, partial
replication of a scope, multi-region servers, transactions beyond one Postgres.

## 2. Decisions

Each item names what was taken, when, why, and what it was chosen over. A change to engine
behaviour adds or amends an item here in the same branch.

1. **The server keeps an events table, not state only** (2026-10-29). Its log is the audit
   trail, the replay substrate for state-version migrations and the feed for policies. Cost: a
   periodic `Snapshot` event per stream. Rejected: state-only rows.
2. **Cross-stream checks are `ctx.read` in `decide`, declared per command** (`reads: [...]`,
   2026-10-29), not Alchemy's rule of an aggregate per protected value. On the server the read
   is inside the SERIALIZABLE transaction, so two concurrent creates of one unique value cannot
   both commit (SSI aborts one; its retry sees the other); on the device it reads effective
   state and the rebase re-decides. Rejected: a stream per unique value with `Claim`/`Release`
   in the same batch, and counter aggregates for in-use checks; pure and shardable, but they
   exist because Durable Objects cannot share a transaction, and one Postgres already gives the
   guarantee. If a check ever needs to be bulletproof across concurrent creates, that answer
   still applies.
3. **A reader who gains a scope receives snapshots, not history** (2026-10-29). Those events
   happened while the reader was not entitled to them, and snapshot-only lets the scope log
   compact. Reversible, since the server keeps every event. Rejected: delivering past events on
   join.
4. **Catalogs are state-only rows plus one `CatalogReleased` event per release** (2026-10-29).
   Tens of thousands of reference rows nobody commands against do not get a stream each.
   Rejected: a stream per catalog row.
5. **The optimistic result is computed in the leader only** (2026-10-29); the tab asks over a
   `MessagePort`. Rejected for v1: tab-side `predict`, a second state copy per tab; cheap to add
   later because the functions are pure.
6. **Transport is plain HTTP with one streaming response per leader** (2026-10-29), no
   WebSockets, no client polling; the only periodic traffic is the server's keepalive on the
   open stream. Rejected: WebSockets (upgrade handshake, proxy and hibernation concerns,
   ping/pong anyway; nothing needs the client-to-server direction at stream latency) and
   polling (latency equals the interval, most requests return nothing).
7. **`decide` and `evolve` are objects with one handler per tag** (Phase 0, 2026-10-29), not
   one function dispatching with `Match.valueTags`. The tags are known from `commands` and
   their `events`, so the checker reports a missing or misspelt handler, types each handler's
   command and its return, and the runner and engine look a handler up without evaluating a
   match. Rejected: `Match.valueTags`, which needs the union assembled at the call site and
   gives one return type for all branches.
8. **`ScopeForbidden`, `Principal`, `Scope` and the pre-grants rule live in `echo/domain`**
   (Phase 0, 2026-10-29), not in an engine module. Writing the ownership story showed the check
   is the same for every aggregate and depends only on `scope(state)` and the principal's
   grants, so it is the engine's job; a story asserts the rejection, so the domain package must
   name it, and domain code never imports the engine. The story runner applies the same check
   before `decide`, as the engine will. The pre-grants rule (own private scope: read and write;
   `catalog:*`: read) is what exists until the grants aggregate of phase 3b. Rejected: an
   `own()` check repeated in every handler (the previous apps' shape); an `Engine` namespace
   the stories import from.
9. **Indexes are declared with `Domain.Index(id, state, spec)` and bound by `Aggregate.make`**,
   which exposes them as statics (Phase 0, 2026-10-29). A command must be able to name an index
   of its own aggregate in `reads`, and the aggregate imports its commands, so an index declared
   only inside the aggregate would be an import cycle. Rejected: `indexes: {...}` inline only,
   which would make own-aggregate `reads` strings or late-bound thunks.
10. **The story runner is pure and lives in `echo/domain`; `echo/domain/testing` binds it to
    vitest** (Phase 0, 2026-10-29). `given` is a list (the target stream) or seeds
    `Story.stream(Aggregate, id, events)`; seeded streams must belong to the aggregate under
    test or to a declared read; `ctx.read` answers only declared reads and only streams the
    principal may see; an undeclared read fails the story with a `StoryError` naming it, so
    `reads` is checked by the stories before any engine enforces it. Rejected: a runner that
    reads any seeded stream (hides a missing `reads`), and one that depends on vitest (an app's
    domain package would import a test runner). Amended 2026-10-30: seeds name the aggregate
    class instead of a record keyed `"Aggregate/id"`. An index is bound to its aggregate when
    the aggregate's module loads (item 9), so with string keys a story that seeded streams
    reached only through another aggregate's index needed a side-effect import of that
    aggregate; naming the class types the seeded events and loads the module. An index whose
    aggregate has not loaded reads as empty.
11. **Own building blocks, Alchemy-shaped, not a dependency on `@alchemy.run/domain`**
    (2026-10-29, built in Phase 0). The package (`2.0.0-beta.84` at alchemy 383d8e8) peer-depends
    on `alchemy`, the IaC package, at runtime; its kernel hands `decide` exactly one stream's
    state and its `HandleInput.context` is plain JSON, so `reads` (item 2) would be an extension
    of the kernel, not of a platform. Vocabulary, file layout, `decide`/`evolve`, `Envelope`,
    `RequestContext`, `Policy` and `Story` are kept as Alchemy has them, so a domain that never
    uses `reads` is portable by an import rename. Rejected: being an Alchemy platform (a beta
    API and the `alchemy` dependency in a browser worker, and no `reads`). Revisit if a domain
    ever has to run on Alchemy's platform.
12. **Domain packages before any engine code** (2026-10-29, done). Aggregate boundaries are the
    risky part and stories make them cheap to try; the engine's additions exist only because
    the apps need them, and the list of what the stories needed *is* the required `ctx` surface
    (§4, Phase 0). Rejected: building the engine against a toy domain first.
13. **This document holds the model, decisions, invariants, phases and rules, and nothing
    else** (2026-10-30). The first version also carried a DDD primer, a survey of other
    engines, worked examples in two private apps' vocabulary, a requirements table per app and
    inlined copies of the Phase 0 code and stories. The examples drifted from the code within a
    day, and the app names had to be purged from the public history twice. App rules are
    `Story` tests, engine behaviour is the invariants catalogue, and the reference for the
    blocks is `src/domain`. Rejected: a separate `docs/invariants.md` (one more file for the
    same "what the engine promises" question; the catalogue is a section here, §3) and a prose
    spec of app rules (the stories are it).
14. **The stand-in under `examples/` is a fixture, not an app** (2026-10-30). Two aggregates,
    `Note` and `Tag`, and one story per engine surface a domain can touch (§4, Phase 0); no
    rule beyond what a surface needs. The real design pressure comes from the apps' domains,
    written against a pinned commit in their own repositories, and their friction is the input
    to the next phase. Rejected: a realistic stand-in app (the first version was a timesheet
    with three aggregates). It grew business rules nobody needed, those rules were reviewed as
    if they were a product, and a fixture is far easier to grow than to shrink once the engine
    phases are built against it.

15. **First dogfood server uses serialized writes and snapshot pulls** (2026-10-30).
    A transactional singleton lock orders SERIALIZABLE writes and cursor advancement; retry
    serialization failures. This trades write throughput for a small, correct first slice,
    rather than using sequence allocations that can commit out of cursor order. Pull returns
    the complete visible scope list and replaces a changed scope with current state; unchanged
    cursors return no rows. Receipts are retained indefinitely and bound to authenticated
    principal, client id and batch id. Catalog imports are state-only with a release record.
    Sharing, policies, presence, history-tail compaction and receipt expiry are deferred, not
    stubbed. Catalog and private scopes are the only supported scopes; commands cannot move
    streams between scopes. Epoch mismatch refuses execution before checking receipts.
    The singleton is a global write bottleneck, including receipt-only transactions. Per-scope
    cursors avoid re-downloading unchanged catalogs after private writes. LISTEN uses a separate
    physical connection per Changes stream so it cannot exhaust the application query pool;
    apps must bound authenticated connections to their database connection budget.
16. **The first browser profile uses IndexedDB and an exclusive Web Lock** (2026-10-30).
    Separate journal and replica databases preserve the journal across cache reset. A second
    leader gets ProfileBusy; SharedWorker tab multiplexing and OPFS SQLite remain deferred.
    Accepted intents remain durable until replica sequences cover their receipt. Sent intents
    crossing an epoch become Unknown notices rather than risking duplicate execution after
    restore. Rejected: pretending multi-tab leadership or receipt recovery across epochs works.
    The initial client exposes one command per intent and equality-index queries, recomputing
    selectors over full replay; atomic client batches, range indexes and dependency tracking
    remain future work. Browser eviction or user-cleared site data can remove both databases;
    this separation protects against engine cache reset, not deletion of origin storage.
    Journal decode failure never authorizes deleting it. There is no destructive discard API.
17. **Connection lifecycle belongs to the client, not a framework adapter** (2026-10-30).
    Scoped `open` hydrates before starting its default connection loop; manual-sync mode is
    explicit. Rejected: one-line Effect/Stream wrappers and fabricated framework objects,
    which omit FoldKit brands and Scene metadata. Client close releases its resources and
    terminates its public streams; app resource generations guard already-delivered messages.

## 3. Invariants catalogue

The behaviours a rule-abiding engine promises, as a numbered list `INV-<AREA>-<n>`, one or
two sentences each, with exactly one scenario test per invariant named after the failure it
catches and citing the id. The catalogue for a phase is written here as part of designing that
phase, before its code; a behaviour not listed is not promised, and an entry without its test
is a known gap and is marked as such. Scenarios are story-shaped where they can be: seed events
on the server, act from a device, assert state after sync or rebase.

Areas: `SERVER` (execute, receipts, scope log, grants, policies, epoch), `CLIENT` (leader,
journal, replica, effective state, rebase, outcomes, queries), `SYNC` (end to end across
devices), `SCHEMA` (versions, upcasters, floor), `PRESENCE`.

- **INV-SERVER-1:** Accepted and rejected batches are atomic and deduplicated under the
  authenticated principal, client id and batch id; retrying cannot append events twice.
- **INV-SERVER-2:** Concurrent decisions serialize, including declared index reads; two
  claims for the same visible unique value cannot both be accepted.
- **INV-SERVER-3:** Writes require the principal's private scope; declared reads see private
  and catalog rows only, hidden ids read as null, and undeclared reads fail.
- **INV-SERVER-4:** Pull snapshots and cursors share one transaction snapshot; reconnect
  cannot skip committed state. Epoch mismatches refuse writes and pulls.
- **INV-SERVER-5:** Catalog ingestion validates state and catalog scope, writes no per-row
  events, and atomically exposes a release to readers.
- **INV-SERVER-6:** HTTP RPC resolves identity outside request bodies and Changes wakes a
  connected reader after a commit; reconnect starts with a catchup poke.
- **INV-CLIENT-1:** Journal survives offline reload and replica reset.
- **INV-CLIENT-2:** Retries preserve intent identity; accepted intents remain until the
  durable replica covers every receipt sequence.
- **INV-CLIENT-3:** Authoritative state rebases intents in journal order and surfaces rejections;
  epoch transitions invalidate old cache state even if persisting the new cache fails, and a
  changed authority epoch on a keepalive triggers resynchronization without a separate poke.
- **INV-CLIENT-4:** An exclusive profile lifetime prevents competing leaders. Open hydrates
  offline and owns automatic connection startup; close ends streams and releases storage,
  connection and lock before another profile instance opens.
- **INV-CLIENT-5:** Typed queries expose effective state and suppress structurally unchanged emissions.
- **INV-SYNC-1:** A browser's offline journal survives reload, reconnects over authenticated
  HTTP RPC to Postgres, converges with an independent device and retains authoritative rejection notices.

## 4. Phases

0. **Domain building blocks and the fixture domain, no engine.** Done 2026-10-30: `echo/domain`
   (`src/domain`) and `examples/fixture/domain` (item 14): `Note` and `Tag`, five stories, one
   per engine surface a domain can touch (an accepted command and its scope stamp, a read by
   id across scopes, `ScopeForbidden`, a read over an own index, a read over another
   aggregate's index). What the single-user app's domain, written against this in its own
   repository, established: the `ctx` surface it needs is `principal`, `now`, `read` by id and
   `read` by index, nothing else; every rule is decidable on the device; the ownership check
   the previous apps repeated in every handler is engine logic (§2 item 8). No story needed
   `Policy`, `Batch`, `authority` or versioning, so they are not built. Catalogs are seeded in
   stories by replaying events with a `catalog:` scope; the server-written path is phase 1.
1. **Server:** streams, events, receipts, scope log with compaction, grants aggregate,
   policies, `Execute`/`Pull`/`Changes` over `effect/rpc`, epoch. `SERVER` invariants first,
   then integration tests against Postgres.
2. **Client:** leader election, journal, replica cache, effective state, rebase, queries,
   outcome phases. `CLIENT` and `SYNC` invariants; browser scenarios (a script driving Chromium
   through `agent-browser` against a real server, printing PASS/FAIL and evidence) for
   offline, rebase, rejection and epoch.
3. **Apps.** (a) FoldKit lifecycle integration and the single-user app's port in its own repository:
   catalogs, `reads`, cursor and aggregate queries, multi-device rebase, import of its current
   data. (b) The shared shape, through a stand-in under `examples/` written then and the shared
   app's port in its own repository: grants aggregate, `Batch`, `Policy`, `authority: "server"`
   commands, scope moves, presence. Phases 1 and 2 build these interfaces but may stub them.
4. **Migrations tooling:** upcasters, state-version rebuild sweep, compatibility floor, import.

## 5. Working rules

**Clean start.** This repository is not a branch of `echo-sync`; the old engine stays frozen
for the current apps until each is ported. Nothing was copied from it but the environment
scripts and toolchain configuration, which are generic. The old engine, its SPEC and the
apps' sync code are not references and are not read (`AGENTS.md`); the inputs are this
document, Alchemy's `domain` package and the pinned Effect source. The apps are private
products: their domain packages live in their own repositories, pin this one by commit, and
none of their names, rules or data appear here; the engine's phases are built against
stand-ins under `examples/`.

**Three artefacts, each with one job, written before the code they govern.** This document
says *why* (§2) and *what is promised* (§3). The domain packages say *what the apps do* as
`Story` tests; there is no prose version of an app rule. The engine's scenarios prove §3, one
per invariant.

**Tests are of two kinds only.** A `Story` for a domain rule, in the domain package, and a
scenario for an engine invariant, named after the failure it catches and citing its `INV` id.
No per-function unit tests, no snapshot tests, no test whose expected value was read off the
implementation. Before keeping a test, name the plausible wrong implementation that fails it;
if there is none, delete the test. `bun run check` rejects a test block whose name cites no
catalogued `INV` id and is not a `Story`. Scenario declarations use `it`, `test`, or their
direct modifiers (`it.effect`, `it.skip`, etc.) with a literal name, not aliased imports,
computed names or `each` factories; the small naming gate deliberately supports these forms.
