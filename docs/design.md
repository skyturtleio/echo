# Echo — an aggregate-based, offline-first sync engine for Effect + FoldKit

Status: design, 2026-10-29. Greenfield; this repository (`skyturtleio/echo`, package `echo`)
replaces `skyturtleio/echo-sync`, whose code and SPEC are the previous design and are not
references for this one (§16).
Sources were read at these revisions: `alchemy-run/alchemy` 383d8e8 (`packages/domain`,
`examples/cloudflare-domain`, the local-first and FoldKit docs), `yielded-dev/sync` f32c24a,
`mac-monet/effect-domain` e0ab708, Effect v4 `effect/eventlog` (4.0.5),
and the owner's two apps, a shared one and a single-user one (private products whose domains
stay in their own repositories; stand-ins under `examples/` take their place here, §12).

## 1. The problem, in Alchemy's own terms

Alchemy's local-first page sorts every approach by two questions: **who decides** whether a
write is accepted (the device or the server), and **whether the device keeps the server's
state** locally.

| | server state stays on the server | device keeps a copy of server state |
|---|---|---|
| **server decides** | optimistic update (Alchemy: `Aggregate.predict`) | **offline-capable, server-authoritative: this engine** |
| **device decides** | offline intents (Alchemy: queued commands) | device-decides logs: Effect `eventlog`, LiveStore |

Alchemy ships the top-left and bottom-left and says about the top-right: *"a local copy of
the server's aggregates … takes a sync engine that keeps the server's state apart from
pending changes and recomputes everything whenever the server's state moves.
`@alchemy.run/domain` doesn't include one."* That sentence is this engine's job description.

The bottom-right is tempting (Effect's own `eventlog` lives there) but fails the requirements:
there is no server-side `decide`, so nothing can refuse a write. The shared app needs "a chore
is claimed by one housemate" and "a household has no more members than seats" to hold even when
every housemate was offline; the single-user app needs "no two of my reference rows share a code" to hold
across its own devices. Those are server-authoritative rules.
`yielded-dev/sync` is top-left and cannot write before its first sync. So: aggregates with a
pure `decide` that runs on **both** sides, the server's answer is final, and the device
**rebases** its pending intents whenever confirmed state moves.

In the words of Hologram's [three meanings of local-first](https://hologram.page/blog/local-first-means-three-different-things)
(speed, offline, ownership): this engine is for **speed and offline with a server in charge**,
the same choice Hologram, Zero and Jazz make. The UI never waits for the network; ownership
(the device holds the only real copy, no server could say no) is explicitly not a goal. The
article's cost list maps onto this design one to one: each client gets a slice (§7 scopes),
old clients come back (§9 versioned commands and the compatibility floor), browser storage is
not forever (§6: the journal is the only thing that must survive; the cache is disposable),
and some actions should wait (`authority: "server"`).

## 2. DDD in five words, with examples from the shared app

You do not need DDD literature for this. You need five nouns and one habit. The examples are
from the shared app's stand-in (§12): a household of housemates sharing chores, grouped in rooms.

| Word | Meaning | Shared app |
|---|---|---|
| **Command** | A request, imperative, can be refused | `ClaimChore { choreId }` |
| **Event** | A fact, past tense, never refused, never edited | `ChoreClaimed { by, at }` |
| **Rejection** | A *named* reason a command was refused, part of the contract | `ClaimedByHousemate { by }` |
| **Aggregate** | The smallest unit of state one rule must protect, with an id; all its commands are decided in order against the same state | `Chore`, `Household`, `Room` |
| **View** (read model) | Anything computed from facts for reading; disposable, rebuildable | "Today", "Later", room lists |

The habit: `decide(state, command) → events | rejection` holds every rule and is pure;
`evolve(state, event) → state` applies a fact and is total (never fails). Events carry their
outcome (`ChoreClaimed.at` is in the event, `evolve` never calls `now()`), so replaying them is
deterministic.

Why this matters for *sync* specifically, not just for tidiness:

1. **The same pure `decide` runs on the device and the server.** "Offline conflict handling"
   stops being a merge algorithm bolted on by the engine and becomes your rule, replayed.
2. **The aggregate is the unit of ordering, idempotency, scope and conflict.** Two devices
   claiming the same chore is resolved by whichever command the server decides first; no
   vector clocks.
3. **Rejections are schemas.** The UI shows "your housemate claimed it first" because it matched
   `ClaimedByHousemate`, not because it parsed a string. Both apps already keep a table of
   rejection codes with explanations; here the compiler enforces it.
4. **Events are the audit log and the migration substrate** (§9).

Both apps already do most of this: every rule sits in one handler shared by device and server,
and the only server-only decisions are grant changes (create/join household, provisioning) and
uniqueness checks that are authoritative only inside the server transaction. v2 gives that
structure names and per-aggregate ordering; the rules themselves transfer almost verbatim.

## 3. The model in one paragraph

An app is a **domain package** (Alchemy's layout: one block per file, a folder per aggregate,
imports only `effect` and the engine's `domain` entry) plus thin server and client wiring. Every
write is a **batch** of commands, each addressed to one aggregate instance (a **stream**). The
device decides the batch provisionally against *confirmed state ⊕ pending batches*, appends it
to a durable **intent journal**, updates in-memory **effective state**, and re-emits affected
queries. The server decides the same batch in one serializable transaction, appends the
resulting events to each stream, writes a **receipt** keyed by the batch id (accepted *or*
rejected), and publishes the events to the **scope log** of every scope the touched streams
belong to. Devices pull scope logs by cursor; on every confirmed change the leader **rebases**:
confirmed state advances, pending batches are re-decided in order (dropping ones whose receipt
says rejected), effective state is recomputed and queries re-emit. Presence is a separate
ephemeral channel keyed by scope.

The sequence diagram in the thread reply shows this for two housemates claiming one chore.

## 4. Building blocks (the domain package)

Vocabulary and shapes are Alchemy's; signatures below are sketches to be verified against the
pinned Effect v4 source when implemented (`Schema.TaggedClass`, `Schema.TaggedError`,
`Context.Service` tags on the classes as Alchemy does).

```ts
// Chore/ChoreClaimed.event.ts
export class ChoreClaimed extends Domain.Event("ChoreClaimed", { by: UserId, at: Instant }, { version: 1 }) {}

// Chore/ClaimedByHousemate.rejection.ts
export class ClaimedByHousemate extends Domain.Rejection("ClaimedByHousemate", { by: UserId }) {}

// Chore/ClaimChore.command.ts
export class ClaimChore extends Domain.Command("ClaimChore", {
  target: ChoreId,
  events: [ChoreClaimed],
  rejects: [ClaimedByHousemate, NotShared, NotYourChore],
}) {}

// Chore/Chore.aggregate.ts
export class Chore extends Domain.Aggregate("Chore", {
  id: ChoreId,
  state: ChoreState,                       // Schema; versioned, see §9
  initial: ChoreState.absent,
  commands: [CreateChore, EditChore, ClaimChore, ReleaseClaim, CompleteChore, MoveChore, DeleteChore],
  scope: (state) => state.accessScopeId,  // which scope this stream is visible in (§7)
  indexes: { by_room: ["roomId"], by_day: ["scheduledDay"] },
  decide: (state, cmd, ctx) => Match.valueTags(cmd, {
    ClaimChore: () =>
      state.visibility !== "shared" ? new NotShared() :
      state.claimedBy !== null && state.claimedBy !== ctx.principal ? new ClaimedByHousemate({ by: state.claimedBy }) :
      [new ChoreClaimed({ by: ctx.principal, at: ctx.now })],
    ...
  }),
  evolve: (state, event) => Match.valueTags(event, {
    ChoreClaimed: ({ by }) => ({ ...state, claimedBy: by }),
    ...
  }),
}) {}
```

What is **kept from Alchemy** unchanged: `Event`, `Rejection`, `Command`
(`target/input/events/rejects/reply`), `Aggregate` (`decide/evolve/initial`), `Envelope`
(`id, stream, seq, at, commandId, correlationId`), `RequestContext` (a schema-encoded principal
that travels with the command into `decide`), `Policy` (server-side reaction to events), and
`Story` tests (*given* events, *when* command, *then* events or rejection), which run the same
code the device and server run and are the proof that "same rule everywhere" holds.

What is **added**, because Alchemy stops at the server boundary:

| Addition | Why |
|---|---|
| `Aggregate.scope(state)` | Visibility is a property of the stream, derived from state, so "move a chore to the household" is an ordinary event (§7). |
| `Aggregate.indexes` | The client replica and the server both build these over state fields; queries use them (§6). |
| `Command.authority: "both" \| "server"` | A few commands may run only on the server (grants, invites, provisioning). The device journals them without predicting. |
| `ctx.read` in `decide` | Read-only access to *other* streams' current state in the same batch, for uniqueness and in-use checks (§8). Alchemy would model each as its own aggregate; we allow reads because the server transaction is serializable and the device re-decides on every rebase anyway. |
| `Batch` | Several commands committed atomically with one id. The shared app's rank renormalisation touches many rows; its `changes.apply` is a batch. |
| `version` on events, state and commands, with upcasters | Alchemy has none; we need a non-painful migration story (§9). |
| Receipts for **rejected** batches | Alchemy stores none; yielded's lesson is that a retried command must get the same answer it got the first time. |
| Client-side `rebase` | The thing Alchemy says it doesn't include. |

Rules of the domain package, copied from Alchemy's skill because they are exactly the ones a
sync engine needs: `decide` is pure, `evolve` is total, events carry computed outcomes,
events are business-named (`ChoreClaimed`, not `ChoreUpdated`), no `now()` or random ids inside
`evolve`, cross-aggregate invariants get their own aggregate keyed by what they protect (or,
here, a `ctx.read` plus the server transaction).

## 5. Server

Postgres, one schema. Tables (sketch; `jsonb` payloads encoded with the Effect schemas):

| Table | Key | Holds |
|---|---|---|
| `streams` | `(aggregate, id)` | `scope`, `seq`, `state`, `state_version` — a cache of `fold(evolve, events)` |
| `events` | `(aggregate, id, seq)` | `name`, `version`, `payload`, `batch_id`, `principal`, `at` — append-only |
| `receipts` | `(client_id, batch_id)` | `Accepted {seqs}` or `Rejected {rejection}` or expired; `expires_at` |
| `scope_log` | `(scope, position)` | `Upsert {stream, seq, state}` \| `Event {stream, seq, event}` \| `Gone {stream}` |
| `grants` | `(scope, principal)` | `reader \| writer \| owner` |
| `policy_outbox` | `(event ref)` | events awaiting server-side policies, at-least-once |
| `engine` | singleton | `epoch` (authority generation, see below) |

**Execute** (`Rpc Execute { clientId, batches: [{ batchId, commands }] }`), per batch, one
`SERIALIZABLE` transaction, retried on serialization failure:

1. Resolve the principal from the session (an app-provided layer; Logto for both apps) into a
   `RequestContext`. Nothing from the client body is trusted for identity.
2. If `receipts(clientId, batchId)` exists, return it. Idempotent under retry, including for
   rejections.
3. For each command in order: load the stream (`state`, `seq`); require a `writer`/`owner`
   grant on `scope(state)` (or the command's declared creation scope for a new stream) and that
   `authority` allows the server; run `decide` with `ctx = { principal, now, read }`; on a
   rejection, abort the whole batch and write a `Rejected` receipt. On events: `evolve`, bump
   `seq`, append to `events`.
4. Write `streams`; if `scope(before) ≠ scope(after)` write `Gone` to the old scope's log and
   `Upsert` to the new one, else write the events to the scope's log. Write the `Accepted`
   receipt. Queue the events for policies.
5. Reply with the receipt and poke every connected device whose grants cover a touched scope.

**Pull** (`Rpc Pull { cursors: { [scope]: position } }` and a long-lived `Changes` stream for
pokes): for every scope the principal is granted, return entries after the cursor. A scope the
device has never seen starts at 0. The scope log is **compacted** like a log-compacted topic:
below a moving watermark only the latest `Upsert` per stream survives, so a fresh device (or a
catalog of tens of thousands of rows) gets *snapshot + tail*, never history. A device whose cursor fell
below the watermark re-pulls from 0; its journal is untouched.

**Transport.** Plain HTTP, no WebSockets: `Execute` and `Pull` are request/response, and
`Changes` is one long-lived streaming response per leader (the server-sent-events pattern;
over `effect/rpc` with NDJSON it is the same thing without the `EventSource` API, and it runs
in a worker). The stream carries only *pokes* ("something changed in a scope you hold") and
presence fan-out; it never carries data, so losing it loses nothing. Correctness lives in
cursors and `Pull`; the stream only shortens latency, which is what makes flaky networks
harmless: a dropped stream reconnects and its first poke triggers a pull from the cursor. One
stream per leader, not per tab, keeps HTTP/1.1's six-connection limit out of the picture.

The stream needs a liveness signal because a TCP connection that died silently behind a proxy
looks exactly like "nothing changed", and idle proxies close connections after about a
minute. The server therefore writes a tiny keepalive (an SSE comment line or a `Heartbeat`
element) every ~25 s on each open stream. This is the one periodic thing in the design and it
is deliberately inert: a server-side timer tick that writes a few bytes, no database access, no
client request, and it never wakes the sync loop. A client that has not seen a byte for about
twice that interval reconnects. There is no client polling anywhere.

**Authority generation** (from yielded): `engine.epoch` changes on restore-from-backup or any
operator reset. Every reply carries it; a device holding a different epoch gets
`ResyncRequired`, discards its confirmed cache and cursors, keeps its journal, re-pulls, and
rebases. This replaces today's `require_epoch` ritual with a protocol rule.

**Policies** run only on the server, from the outbox, at least once; they issue commands with
deterministic batch ids (`policy:<event ref>`) so a redelivery is a no-op via receipts. The shared app:
`HouseholdJoined` → `Scope.Grant` per shared scope; `HouseholdLeft` → `Scope.Revoke`.

**Grants are an aggregate** (`Scope`, commands `Grant`/`Revoke`, `authority: "server"`), so
every change to who can see what is itself an event in the log; the server derives `grants`
from it. Apps never write grant rows by hand.

Why Postgres makes this easier than Alchemy's Durable Object model: a batch may span aggregates
and still be one transaction, and receipts can live outside any single aggregate partition.

## 6. Client

One **leader** per profile (a `SharedWorker`, with election and a dedicated-worker fallback for
browsers without it) and any number of **tabs**. The engine is framework-free; the FoldKit
adapter (§11) is a translation of the tab API.

The leader owns three kinds of state:

| Kind | Where | Content |
|---|---|---|
| **Durable, must survive anything** | OPFS SQLite (`journal.db`) | `journal`: batches with phase `Pending \| Sent \| Confirmed(seqs) \| Rejected(r) \| Unknown`, plus `epoch`, `notices` |
| **Durable cache, disposable** | OPFS SQLite (`replica.db`) | `confirmed` stream rows `(aggregate, id, scope, seq, state, state_version)` with generated columns for `Aggregate.indexes`; scope cursors |
| **Hot** | leader memory | **effective** rows = `confirmed ⊕ replay(pending batches)`; in-memory indexes; the subscription set |

Journal and cache are separate files on purpose (yielded's split): resetting a profile, an
epoch change or a state-version bump may delete the cache; the journal is deleted only by a
confirmed receipt or by the user's explicit consent.

**Execute from a tab.** The tab sends the batch to the leader. The leader (a) decides it against
effective state (`decide` + `evolve`, pure, in memory), rejecting locally with the same
rejection the server would give for everything it can know; (b) on local acceptance, appends it
to the journal (ordered write-ahead; the UI is not blocked on the disk), updates effective rows
and re-emits every subscribed query whose dependencies changed; (c) pushes `Sent` batches to
the server in journal order whenever online. A command with `authority: "server"` skips (a) and
is journaled with no prediction; the tab sees it as pending.

Online or offline is never consulted on this path. The decision, the journal append and the
re-emit happen the same way with or without a network; connectivity only changes how soon
step (c) runs. When online, the receipt arrives tens of milliseconds later and almost always
agrees with the provisional decision, so nothing visible changes; a tab can observe
`Pending → Accepted` if it wants to show it. The engine never "checks with the server first"
except for `authority: "server"` commands, which is the article's "some actions should wait".

**Rebase.** Whenever the confirmed cache moves (a pull, a receipt, an epoch reset), the leader
rebuilds effective state for the touched streams: start from confirmed, re-run each pending
batch in journal order through `decide`/`evolve`, drop a batch that now rejects (it becomes a
notice with its rejection), and re-emit affected queries. Because `decide` is pure and pending
batches are few (tens, not thousands), full replay is the simplest correct algorithm; no
per-field merge logic lives in the engine.

**Outcomes as data** (from yielded): a batch whose request was sent but got no answer is
`Unknown`; the leader retries with the same `batchId` until the receipt says
`Accepted | Rejected`, or the server says the receipt expired, in which case the batch becomes
an `Unknown` notice and is never re-sent under a new id (the single-user app's "a new mutate
call is a new intent" rule, now engine policy).

**Queries.** A query is a pure function over a `Read` interface (`get(aggregate, id)`,
`index(aggregate, name).range(...)`, `scan(aggregate)`) returning a value; it is registered in
the domain package like Alchemy's `Aggregate.read`. The leader runs it over effective rows,
records which aggregates (later: which keys) it touched, re-runs it when any of them changes,
and streams results to tabs with structural equality so unchanged results do not re-emit.
The shared app's one whole-replica query that re-emits everything becomes several small queries
with cheap invalidation; the single-user app's cursor pagination, sums per period and search are
`index(...).range` plus aggregate helpers on the `Read` interface.

**Next frame.** A tab→leader→tab round trip over a `MessagePort` is well under a millisecond;
what blows a frame budget is disk. So the rule is: nothing on the execute→re-emit path touches
OPFS synchronously. Effective state and indexes are in memory, the journal append is
write-ahead and asynchronous, and queries run in memory. The replica is loaded into memory at
leader start (the single-user app's largest table is thousands of rows; its large reference
catalog is kept on disk and served by index queries only). The durability window is the few milliseconds between
the UI update and the journal flush; a crash in that window loses that one batch, which is the
same trade every local-first app makes. Tab-side `predict` (running `decide`/`evolve` in the
tab before the leader answers) is possible because the functions are pure, but is not needed
for v1.

**Profiles and sign-out.** The replica is keyed by profile (a hash of the principal), so two
accounts on one browser never share a cache. Sign-out with pending batches requires consent to
discard them (the shared app's sign-out consent gate), now engine behaviour.

## 7. Scopes, grants and visibility

A **scope** is an opaque id (`user:<id>`, `household:<id>`, `household:<id>:ro:<owner>`,
`catalog:projects`). Every stream is in exactly one scope at a time, given by
`Aggregate.scope(state)`. A principal with a grant on a scope receives every stream in it;
`writer` may send commands to those streams; `owner` may also grant. There is no per-row ACL
and no field-level filter: a field some readers must not see (the shared app's room notes in the
read-only scope) is its own aggregate in its own scope. This is exactly how both apps model
sharing today, so their scope functions port unchanged.

A scope change is a normal event (`ChoreMoved { to }`); the server turns it into `Gone` for the
old scope's readers and `Upsert` for the new scope's readers (§5), so a housemate who loses
access sees the row vanish on the next pull and a housemate who gains it gets a snapshot, never
the history they were not entitled to.

`decide` sees `ctx.principal` and may enforce ownership (`NotYourChore`) and self-claims; the
engine enforces grants. The split is the same as today: grants decide *whether you may talk
to this stream*, the rule decides *what you may say*.

Catalogs are scopes with a `*` reader grant and only server-authority writes (the single-user
app's reference catalogs). A catalog import writes `Upsert` entries directly
with one `CatalogReleased { release }` event per release rather than one event per row.
Every other stream (an entry, a member's own project, a chore) is ordinary: each command's events
are appended to that stream and delivered as events. A new catalog release reaches a device
that already holds the catalog as `Upsert` entries for the rows that changed in that release,
not as the whole catalog again; only a device with no cursor loads the full snapshot.

## 8. Conflicts are rules, not merge strategies

Every conflict that actually occurs in the two apps falls into one of four shapes, and each is
expressed in `decide` or `evolve`, not in the engine:

| Shape | Example | Expression |
|---|---|---|
| Exclusive claim | both housemates claim one chore offline | `decide` rejects the second with `ClaimedByHousemate`; the loser's device rebases, drops it, shows the rejection |
| Independent fields | phone edits an entry's notes, laptop edits its times | the command carries the *change set* (`AmendEntry { changes }`), the event records it, `evolve` merges; same-field edits are last-writer-wins by server order, visibly and deliberately |
| Stale base | "I edited what I saw, don't clobber a newer version" | opt-in: the command carries `expectedSeq`; `decide` returns `Stale` when `state.seq` moved; the app chooses which commands opt in |
| Target vanished / access lost | housemate deleted the chore; owner made it private | `NotFound` / `Forbidden` rejections from the server, surfaced as notices; the device already shows the server's version after rebase |

Uniqueness and in-use checks (duplicate project codes, "reassign entries before removing this
task") use `ctx.read` over the relevant index inside `decide`. The server's
serializable transaction makes the read authoritative; the device's read makes the optimistic
answer right in the common case and the rebase corrects it otherwise. If a check ever needs to
be bulletproof across concurrent *creates* of the same unique value, the Alchemy answer still
applies: a `Code` aggregate keyed by the value, claimed in the same batch.

`ctx.now` is the server's clock on the server and the device's clock on the device; the
confirmed event's `at` is what everyone converges on. Apps never use `now` for ordering.

## 9. Schema evolution without pain

Three layers, each with a different lifetime:

1. **Events are immutable and versioned.** An event class is `(name, version)`. Changing an
   event's shape means a new version plus an `upcast(v1) → v2` function registered next to it;
   `evolve` only ever sees the latest version. Old events are never rewritten.
2. **State is a derived cache with a version.** `Aggregate.state` may change freely; bump
   `stateVersion`. The server rebuilds a stream whose stored `state_version` is older by
   replaying its events on first touch (and in a background sweep). The device treats a
   version mismatch as "discard the cache, re-pull snapshot + tail" (§5); the journal is kept.
   Streams get a periodic `Snapshot` event so replay is bounded.
3. **Read models are rebuildable.** Queries are functions; indexes are regenerated from state.
   No app-level `ALTER TABLE` ever runs against event or state data; the only DDL is engine
   schema and index promotion (a generated column for a field that needs an index).

Commands in the journal are versioned too: a pending batch whose command version has no
upcaster becomes an `Unknown` notice instead of being sent under a shape the server no longer
accepts. The server rejects commands and events from a client whose domain package version is
older than a declared floor (`CompatibilityFloor`), a gap the previous engine left open.

What this gives the two apps: the shared app's "engine swap + backfill" migration becomes an import
that emits `Upsert`s and one `Imported` event per stream; the single-user app's many numeric
columns are fields of one state schema and gaining one more is a state-version bump with a
default, no backfill.

## 10. Presence

Ephemeral, never journaled, never persisted. A **room** is a scope; a tab `join(room, state)`s
and `track(state)`s; the server fans out `{ principal, clientId, state }` to the room's
members over the same long-lived `Changes` stream used for pokes, and expires on disconnect or
a missed heartbeat. State is an app schema (`HousematePresence { editingChoreIds }`). The
single-user app has no presence and pays nothing. This is the one part of `echo-sync` whose shape carries
over as is, with one transport change: today the leader opens a second long-lived stream per
joined room (`echo-sync` SPEC §10.2 `room(join)`), each with its own heartbeat and idle timeout. Here
`join` and `track` are ordinary small requests carrying `{ room, state, revision }`, the
server keys the member's entry to the leader's `Changes` stream, and the entry expires when
that stream ends. One long-lived connection per leader, whether or not the app uses presence;
an offline tab's `track` is a local no-op and its entry ages out on the housemate's side.

## 11. FoldKit adapter

`echo/foldkit` is a thin translation of the tab API and imports nothing from the framework;
`echo/core`, `client` and `server` stay UI-agnostic. Surface:

| | Shape | Backed by |
|---|---|---|
| `Echo.subscribe(query, args)` | `Subscription<Msg>` emitting `Settled<A>` (`Loading \| Value \| Failure`) | query stream, diffed |
| `Echo.execute(batch)` | `Command<Msg>` resolving to `Accepted \| Rejected(r) \| Pending` (server-authority) | execute |
| `Echo.status` | `Subscription` of `{ online, pending, lastError, epoch }` | leader status |
| `Echo.notices` / `Echo.dismiss` | retained rejections and unknown outcomes | journal |
| `Echo.presence(room, state)` / `Echo.track` | `Subscription` of members | presence |
| `Echo.resource` | `ManagedResource` wiring the leader for the app's lifetime | client |

The shared app's hand-written adapter runs to hundreds of lines because `echo-sync`'s rejection, reset
and exit-gate streams had to be mapped one by one; with typed rejections per command and the
outcome-as-data journal, the adapter above is the whole mapping.

## 12. v1 is defined by the two apps

Both apps are private products: each domain package lives in its own repository, pins this
one by commit, and none of their names, rules or data appear here. The engine's phases are
built against stand-ins with the same shapes. `examples/timesheet/domain` stands in for the
single-user app (one record aggregate, two reference aggregates, a catalog scope, no sharing).
`examples/chores/domain`, written in phase 3b, stands in for the shared app: a household of
housemates, chores claimed by one housemate and grouped in rooms, room notes only their owner
sees, private and shared scopes, presence. Its vocabulary is used for the sketches in §2 and §4.

| Requirement | Shared app | Single-user app | Engine feature |
|---|---|---|---|
| Aggregates | `Member`, `Household`, `Chore`, `Room`, `RoomNotes`, `ChoreRank` | one record aggregate, several reference aggregates; catalogs | §4 |
| Atomic multi-stream writes | rank renormalisation, create-chore-with-rank, leave-household | restore from backup | `Batch` |
| Server-only commands | create/join household, provisioning | provisioning, catalog release | `authority: "server"`, `Scope` aggregate, policies |
| Scope moves | private ↔ shared ↔ visible | none | `scope(state)`, `Gone`/`Upsert` |
| Exclusive-claim conflict | `ClaimedByHousemate` | none | rejection + rebase |
| Field-level merge across devices | shared chore edits | two devices editing one record | change-set events |
| Cross-stream checks in `decide` | room usable, household full | duplicates, in-use, reference validity | `ctx.read` |
| Reactive queries | today/later/room lists, counts | lists by date cursor, totals per period, search | `Read` + index queries |
| Large read-only data | none | a reference catalog of tens of thousands of rows | compacted scope log, on-disk index queries |
| Presence | housemate editing | none | §10 |
| Notices | rejected/unknown with explanation | recovery list | journal phases |
| Auth | Logto session → principal | Logto session → principal | `RequestContext` layer |

Non-goals for v1: collaborative text (CRDT) fields, per-row ACLs inside a scope, partial
replication of a scope (a device gets a whole scope or nothing), multi-region servers,
cross-aggregate transactions beyond one server (one Postgres).

## 13. What was borrowed from whom

| From | Taken | Left behind |
|---|---|---|
| Alchemy `@alchemy.run/domain` | the whole vocabulary and file layout; `decide/evolve`; `Envelope`; `RequestContext`; `Policy`; `Story` tests; "events carry outcomes"; "cross-aggregate invariant → own aggregate" | Cloudflare DO partitioning; no versioning; no client replica; no receipt for rejections |
| `yielded-dev/sync` | authority epoch + `ResyncRequired`; outcome-as-data (`Found/Unknown/Expired`, never retry under a new id); journal separate from cache; atomic `{expected, head, events, receipt}` commit | online-first; one state blob per source; no local queries |
| Effect `eventlog` / LiveStore | client-side rebase by replay; versioned event names; materialise into local SQLite | device-decides (no server rejection); LWW by UUIDv7 clock |
| `mac-monet/effect-domain` | `Settled` as the FoldKit model slot; hashed invocation keys for subscriptions | the GraphQL-style selection layer |
| `echo-sync` (the previous engine) | scope/grant model, presence rooms, Postgres + leader-worker topology as a known-good shape, the browser scenario harness as a testing approach | the table/mutation/`dataHash` design, the SPEC |

## 14. Decisions

Each open item names the recommended option first and the alternative it was chosen over,
with the reason. "Confirm" means the recommendation stands unless vetoed.

**Taken (2026-10-29):** the server keeps an **events table**, not state only. Reason: the
server is authoritative, so its log is the audit trail, the replay substrate for state-version
migrations (§9) and the feed for policies. Cost: a periodic `Snapshot` event per stream so
replay stays bounded.

**Taken (2026-10-29), with the alternative each was chosen over:**

2. **Cross-stream checks use `ctx.read` in `decide`**, declared per command
   (`reads: [Project.by_code]`), not Alchemy's rule of an aggregate per protected
   value. How `CreateProject` learns another project has this code, or
   `RemoveTask` that entries still reference it: `decide` reads other streams' current
   state through the indexes. On the server the read is inside the SERIALIZABLE transaction, so
   two concurrent creates of one code cannot both commit (Postgres SSI aborts one; its
   retry sees the other); on the device it reads effective state and the rebase re-decides.
   This is what both apps do today. Rejected: a `Code` stream keyed by
   `user:<id>/<value>` with `Claim`/`Release` in the same batch, and counter aggregates for
   in-use checks; pure and shardable, but it exists because Durable Objects cannot share a
   transaction, and one Postgres with no sharding plans already gives the guarantee.
3. **A reader who gains a scope receives snapshots, not history.** When B joins the household
   or A shares a chore, B gets the stream's current state (`Upsert`). Those events happened
   while B was not entitled to them, and snapshot-only lets the scope log compact. The server
   is authoritative and keeps every event, so this is reversible; an activity feed would be a
   per-scope event window later. Rejected: delivering past events on join.
4. **Catalogs are state-only rows plus one `CatalogReleased` event per release.** Tens of
   thousands of reference rows nobody commands against do not get a stream each; every other
   aggregate does. Rejected: a stream per catalog row (tens of thousands of events for no reader).
5. **The optimistic result is computed in the leader only.** One effective state exists;
   the tab asks over a `MessagePort` (sub-millisecond). Rejected for v1: tab-side `predict`,
   which saves the round trip at the cost of a second state copy per tab; cheap to add later
   because the functions are pure.
7. **Transport is plain HTTP with one streaming response per leader** (§5 Transport), no
   WebSockets, no client polling; the only periodic traffic is the server's inert keepalive on
   the open stream. Rejected: WebSockets (bidirectional and lower per-message overhead, but an
   upgrade handshake, proxy and hibernation concerns, and ping/pong anyway; nothing here needs
   the client-to-server direction at stream latency) and polling (the "request every 15 s"
   shape; latency equals the interval and most requests return nothing).

**Open:**

1. **Depend on `@alchemy.run/domain`, or ship our own blocks with the same shape?**
   Facts (alchemy 383d8e8): the package is `2.0.0-beta.84`, peer-depends on `effect ^4.0.5`
   *and* on `alchemy` (the IaC package; `Aggregate.ts` and `Saga.ts` import from it at
   runtime), and has an explicit platform seam (`Platform.ts`: `AggregateStore`, `ViewStore`,
   `HandleInput { id, state, version, command, commandId, context, now }`); the Cloudflare
   platform is a separate package. Two platforms exist today, `InMemory` (in the domain
   package, used by the story runner) and Cloudflare Durable Objects
   (`@alchemy.run/cloudflare/Domain`); nothing in the domain package requires Durable Objects,
   so a Postgres platform is an implementation of `AggregateStore` (`send`, `schedule`,
   `state`, `changes`, `tail`) that nobody has written. Its kernel hands `decide` exactly one
   stream's state, and `HandleInput.context` is plain JSON, so a platform cannot smuggle a
   reader in through it; `reads` would be an extension of the kernel, not of a platform.
   Alchemy's own live reads use a WebSocket where the platform offers `connect`, and fall back
   to streamed HTTP responses otherwise.
   Item 2 therefore closes the Alchemy door only for the commands that declare `reads`: a
   command without `reads` runs on either engine unchanged, and a domain that never uses
   `reads` is fully portable. Duplication arises only if the *same* domain must run on both
   engines; a different project with its own domain on Alchemy shares nothing with this one.
   - *Own blocks, Alchemy-shaped (drafted in §4).* No beta or IaC dependency in a browser
     worker; we own versioning and `reads`. Moving a domain to Alchemy later is an import
     rename for portable commands plus rewriting the `reads` ones as own aggregates.
   - *Be an Alchemy platform.* The domain package is pure Alchemy; our engine implements its
     `Platform` seam on Postgres plus the client replica, with the sync additions in a sidecar
     `Sync.aggregate(Chore, {...})`. Cost: a beta API, the `alchemy` peer dependency in the
     client bundle (unverified how much it drags in), and no `reads`.
   Recommended for v1: own blocks. Revisit if a domain ever has to run on Alchemy's platform.
6. **Sequencing: model the apps' domains before any engine code (Phase 0)?** Not a design
   fork, a proposal: aggregate boundaries are the risky part (is `ChoreRank` its own aggregate?
   is membership on `Household` or `Member`? is `RoomNotes` a stream?), `Story` tests make
   them cheap to try, and the engine's additions in §4 exist only because the apps need them.
   Alternative: build the engine against a toy domain first and port the apps after.

## 15. Phases

0. **Domain packages first, no engine.** Rewrite each app's rules as aggregates, commands,
   events and rejections with `Story` tests. This costs nothing in infrastructure and is where
   the DDD learning happens; if an aggregate boundary is wrong it shows here.

   A `Story` (Alchemy's name) is a test in three parts: *given* the events that already
   happened to a stream, *when* this command arrives, *then* these events are recorded or this
   rejection is returned. It folds `given` through `evolve`, runs `decide` once, and compares.
   Nothing else is involved, so the stories run in milliseconds with no database or browser:

   ```ts
   Story.make(Chore, "a shared chore claimed by a housemate cannot be claimed again", {
     given: [new ChoreCreated({ owner: A, scope: household }), new ChoreClaimed({ by: B, at })],
     when: new ClaimChore({ target: choreId }),
     as: A,
     then: new ClaimedByHousemate({ by: B }),
   })
   ```

   The stories do not fail because the engine is missing; they pass when the rule is right.
   What they tell us is different. First, whether a rule is expressible with that aggregate's
   own state plus what `ctx` provides; when it is not (claiming needs the principal, creating
   a chore in a room needs the room's state, leaving a household touches several streams), the
   missing piece is either an engine requirement (`ctx.principal`, `ctx.read`, `Batch`,
   `Policy`) or a boundary mistake (membership was on `Member` when `HouseholdFull` needs it on
   `Household`). The list of what the stories needed *is* the engine's required `ctx` surface.
   Second, they are the executable contract that "the device answers the way the server
   would": the same `decide` runs in both places, so a story is proof, not hope. Third, when
   the engine exists, the same stories become the fixtures for its own tests (push these
   `given` events through the server, send `when` from an offline device, assert `then`
   after rebase).

   **The single-user shape goes first** (owner's choice, 2026-10-29), through the stand-in
   domain `examples/timesheet/domain` (§12): entries booked on tasks within projects, each
   stream in its member's private scope, projects also in a server-written catalog. It has
   no sharing, no presence and no grant-changing commands, so Phase 0 and the first engine
   slice exercise the core (streams, journal, rebase, `reads`, indexes, catalogs, cursor
   queries) without the multi-principal parts; those arrive with the shared shape in phase 3b. What
   the timesheet stories look like, and what each one forces the engine to provide:

   ```ts
   // Entry/Entry.aggregate.ts — one stream per entry, scope = its member's private scope
   export class Entry extends Domain.Aggregate("Entry", {
     id: EntryId,
     state: EntryState, initial: EntryState.absent,
     commands: [LogEntry, AmendEntry, DeleteEntry],
     scope: (state) => state.scope,
     indexes: { by_day: ["scope", "day"], by_task: ["taskId"], by_project: ["projectId"] },
     decide: (state, cmd, ctx) => Match.valueTags(cmd, {
       LogEntry: ({ data }) => {
         const project = ctx.read(Project, data.projectId)                // reads: declared on the command
         if (project === null) return new UnknownProject({ id: data.projectId })
         if (!inOrder(data.startedAt, data.endedAt)) return new TimesOutOfOrder()
         return [new EntryLogged({ ...data, scope: privateScope(ctx.principal) })]
       },
       AmendEntry: ({ changes }) => {
         const next = { ...state, ...changes }                               // sparse patch, rule on the merged row
         if (!inOrder(next.startedAt, next.endedAt)) return new TimesOutOfOrder()
         return [new EntryAmended({ changes })]
       },
       DeleteEntry: () => [new EntryDeleted()],
     }),
     evolve: (state, event) => Match.valueTags(event, {
       EntryLogged: (e) => ({ ...e, deleted: false }),
       EntryAmended: ({ changes }) => ({ ...state, ...changes }),
       EntryDeleted: () => ({ ...state, deleted: true }),
     }),
   }) {}
   ```

   ```ts
   // Entry/Entry.story.ts
   Story.make(Entry, "an amendment that ends an entry before it starts is refused on the merged row", {
     given: [new EntryLogged({ ...mondayMorning, startedAt: t("10:00"), endedAt: t("10:45") })],
     when: new AmendEntry({ target: entryId, changes: { endedAt: t("09:50") } }),
     as: member,
     then: new TimesOutOfOrder(),
   })
   // → needs nothing beyond this stream's state. Confirms sparse patches are decided on the merged row.

   Story.make(Entry, "an entry against a project the member cannot see is refused", {
     given: { [`Project/${acme}`]: [new ProjectCreated({ code: "ACME", scope: privateScope(otherMember) })] },
     when: new LogEntry({ target: entryId, data: { ...mondayMorning, projectId: acme } }),
     as: member,
     then: new UnknownProject({ id: acme }),
   })
   // → forces: `reads` on LogEntry, `ctx.read` scoped to the streams the principal may see
   //   (so the catalog scope works and another member's private project does not), and a
   //   story runner whose `given` can seed other streams.

   Story.make(Project, "a second project with the same code is refused", {
     given: { [`Project/${acme}`]: [new ProjectCreated({ code: "ACME", scope: privateScope(member) })] },
     when: new CreateProject({ target: acmeAgain, data: { code: "acme", ... } }),
     as: member,
     then: new DuplicateCode({ existing: acme }),
   })
   // → forces: an index read (`by_code`) in `decide`, normalisation inside `decide`
   //   ("acme" matches "ACME"), and the server-side SSI guarantee of §14 item 2.

   Story.make(Task, "a task with entries cannot be removed", {
     given: { [`Task/${review}`]: [new TaskAdded({...})], [`Entry/${e1}`]: [new EntryLogged({ ...mondayMorning, taskId: review })] },
     when: new RemoveTask({ target: review }),
     as: member,
     then: new TaskInUse({ entries: 1 }),
   })
   // → forces: `reads` over the `Entry.by_task` index. Confirms decision 2: in-use is a
   //   read, not a counter aggregate the app must maintain.

   Story.make(Entry, "another member cannot amend my entry", {
     given: [new EntryLogged({ ...mondayMorning })],
     when: new AmendEntry({ target: entryId, changes: { notes: "x" } }),
     as: otherMember,
     then: new Engine.ScopeForbidden({ scope: privateScope(member) }),
   })
   // → the interesting one: this rule is the apps' `own()` check, copied into every handler
   //   today. The first draft of this story said `then: new NotYourEntry()` and put the check
   //   in `decide`; writing it showed the check is the same for every aggregate and depends
   //   only on `scope(state)` and the principal's grants, so it is the engine's job. The story
   //   stays, asserting the engine's generic rejection, and `own()` disappears from the domain.
   ```

   What this batch of stories tells us before any engine exists: the `ctx` surface this
   shape of app needs is `principal`, `now`, `read` by id and `read` by index, nothing else;
   every rule is decidable on the device; three of five rules need `reads`, which settles
   item 2 for this shape; and one rule the apps thought was domain logic is engine logic.
   Stories do not cover the queries (`entries.list` by day cursor, `entries.totals`); those
   are read models over the indexes and get their own tests in phase 2.
1. **Server:** streams, events, receipts, scope log with compaction, grants aggregate, policies,
   `Execute`/`Pull`/`Changes` over `effect/rpc`, epoch. Integration tests against Postgres.
2. **Client:** leader election, journal, replica cache, effective state, rebase, queries,
   outcome phases. Browser scenarios for offline/rebase/rejection/epoch, in the style of the
   `echo-sync` browser harness (a shell script driving Chromium through `agent-browser` against a real server, printing PASS/FAIL and evidence).
3. **Apps.** (a) FoldKit adapter and the single-user app's port, in its own repository:
   catalogs, `reads`, cursor and aggregate queries, multi-device rebase, migrations from the
   current data. (b) The shared shape, through `examples/chores/domain` and then the shared
   app's port in its own repository: adds the multi-principal parts, which are the only engine work
   the single-user shape does not need: grants aggregate, `Batch`, `Policy`,
   `authority: "server"` commands, scope moves and presence. Phase 1 and 2 build these
   interfaces but may stub them until 3b.
4. **Migrations tooling:** upcasters, state-version rebuild sweep, compatibility floor, import.

## 16. Working rules for the rewrite

**Clean start.** The rewrite is this repository, `skyturtleio/echo`, not a branch of
`echo-sync`. The old engine stays frozen there for the current apps until each is ported,
so nothing in it has to keep working for the new one. This repository started with this
document, a fresh `AGENTS.md`, the environment scripts (`.agents/setup`, `.agents/resume`,
`.amp/services.yaml`) and the toolchain configuration, which are generic. Nothing else was
copied; the browser harness is reproduced from its *approach* (a scenario script that drives
Chromium against a real server and prints evidence), not its code. `AGENTS.md` says in its
first lines that the old engine, its SPEC and the current apps' sync code are not references
and are not to be read; the only inputs are this document, Alchemy's `domain` package and the
pinned Effect source.

**Three documents, each with one job, written before the code they govern.** This design
document says *why* (decisions and rejected alternatives, §14). The domain packages say *what
the apps do* as `Story` tests; there is no separate prose spec for app rules, the stories are
it. The engine has an **invariants catalogue**: a numbered list (`INV-<AREA>-<n>`) of the
behaviours a rule-abiding engine must show, one or two sentences each, with exactly one named
scenario test per invariant. It is written as part of designing each phase, not derived from
code afterwards; a behaviour not in the catalogue is not promised, and a catalogue entry
without its test is a known gap, listed as such. `echo-sync`'s SPEC is the
opposite shape (3 200 lines derived from the code after the fact, 158 invariants, about 430
test blocks across 50 files, one of which names the invariant it covers) and is the thing
this rule exists to avoid.

**Tests are of two kinds only.** A `Story` for a domain rule, in the domain package, and a
scenario for an engine invariant, in the engine, named after the failure it catches and
citing its `INV` id in the test name. No per-function unit tests, no snapshot tests, no test
whose expected value was read off the implementation. A scenario is story-shaped where it
can be (seed events on the server, act from a device, assert state after sync or rebase); the
browser scenarios are the same shape with a real browser. `bun run check` fails on a test
block whose name does not cite an existing `INV` id or `Story`, the same kind of lightweight
syntactic gate that `echo-sync` used for its telemetry entry points; review still asks of every test
"which plausible wrong implementation fails this".
