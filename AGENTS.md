# Echo — agent guidance

Echo is an aggregate-based, offline-first sync engine for Effect v4 and FoldKit: the UI never
waits for the network, writes work offline, and the server is authoritative. Read
`docs/design.md` first; it is the model (§1), the decisions (§2, why and what was rejected),
the invariants catalogue (§3), the phase plan (§4) and the working rules (§5) summarised here.

## Not references

This repository replaces `skyturtleio/echo-sync`. That engine, its `docs/SPEC.md`, and the
sync code of the owner's two private apps written against it (a shared one and a single-user
one) are the previous design. Do not read them, clone them, or borrow from
them, even to "check how it was done"; if the design doc needs something they knew, say so in
your report and the owner decides. The apps' domain packages live in their own repositories
and pin this one by commit; nothing of a private app's domain (its names, rules or data) goes
into this public repository. The engine's own phases are built against the fixture domain in
`examples/fixture` (design §2 item 14): one story per engine surface, no rules beyond that.
The inputs for this work are `docs/design.md`, `@alchemy.run/domain` (vocabulary, file
layout, `decide`/`evolve`, `Story`; read its source, not memory), and the pinned Effect source
in `node_modules/effect/src/**` and `node_modules/@effect/*`.

## Repo shape

- The repository root is the package (`echo`), consumed as a git dependency pinned to a
  commit SHA; it ships TypeScript source, no build step, no npm publish.
- Planned entry points (design §1): `echo/domain` (the building blocks an app's
  domain package is written against: `Aggregate`, `Command`, `Event`, `Rejection`, `Story`,
  `Policy`, `Batch`), `echo/server` (Postgres, `Execute`/`Pull`/`Changes`), `echo/client`
  (leader worker, journal, replica cache, effective state, rebase, queries),
  `echo/foldkit` (a thin translation of the client tab API that imports nothing from the
  framework). `domain` and `client` code never imports `server`. No engine entry point
  imports a UI framework; FoldKit is a devDependency for examples only.
- Runtime: Bun. Tooling: TypeScript strict (`exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`), `oxlint`, `oxfmt`, `vitest` + `@effect/vitest`.
- Runtime dependencies are Effect packages only, exact-pinned to one version.

## Effect v4

- Verify every API against the pinned source before using it. Do not trust memory of v3 APIs.
- v4 import paths: `effect/sql`, `effect/rpc`, `effect/http`, `effect/workers`,
  `effect/testing`, `effect/Schema` (stable). `effect/schema` (lowercase) is a different,
  unstable barrel; lint forbids it.
- No `any`, no `!`, no `as` outside platform edges (and then with a comment).
- `decide` and `evolve` are pure functions, never Effects; `decide` sees only its stream's
  state, the command, and `ctx` (`principal`, `now`, declared `reads`).

## Tests

Two kinds exist, and nothing else (design §5):

- A `Story` for a domain rule: *given* events, *when* command (*as* principal), *then* events
  or rejection. Stories live with the aggregate they test. They are the specification of app
  rules; there is no prose version.
- A scenario for an engine invariant, named after the failure it catches and citing its
  `INV-<AREA>-<n>` id from the invariants catalogue (design §3, written per phase before the
  code). One scenario per invariant. Scenarios are story-shaped where they can be:
  seed events on the server, act from a device, assert state after sync or rebase.

No per-function unit tests, no snapshot tests, no test whose expected value was read off the
implementation. Before keeping a test, name the plausible wrong implementation that fails it;
if there is none, delete the test. From phase 1, `bun run check` rejects a test block whose
name cites no existing `INV` id and is not a `Story`.

## Workflow

- Work on a `ws/<name>` branch. Commit in logical steps.
- A change to engine behaviour is a design change: update `docs/design.md` (a §2 decision with
  date, reason and rejected alternative, and the §3 catalogue) in the same branch.
- Before merging, get an independent review of the diff (a reviewing subagent or a second
  model), address its findings, and list in your report the `INV` ids and stories you tested.
- To land: `git fetch`, rebase linearly onto `origin/main`, run `bun run check` and the
  Postgres and browser suites that exist for the touched area, then fast-forward push `main`.
  Never force-push `main`. No pull requests.
- Consumers pin a commit SHA, so an app picks up a change only by bumping its pin.

## Local environment

- `.agents/setup` installs locked dependencies and prepares a disposable PostgreSQL 18 on
  `127.0.0.1:5432` with trust auth; `.agents/resume` makes sure it is running via
  `.amp/services.yaml`. Any machine with Bun and a PostgreSQL reachable at `ECHO_PG_URL`
  works the same way.
- `bun run check` is the default gate (types, lint, format, unit tests). That database is
  throwaway (crash durability off): never expose it or put production data in it.
- Long-lived servers in an orb run as supervised services (`amp orb service start`), never as
  detached shell jobs.
