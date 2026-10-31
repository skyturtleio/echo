# sql-pg 4.1.1 interrupted acquisition

`sql-pg-4.1.1.patch` repairs an upstream connection-owner permit leak. Interrupting
`pinExclusive` after taking the permit but before installing cleanup can leave an
idle pooled connection unusable. Echo's public Pull and Execute both expose this
failure with a size-one pool. The patch restores scoped ownership without masking
transaction bodies or changing Echo's engine behavior.

The reviewed patch has SHA256
`a21407fe296b77c4631a5ab188908e386efc076325d80d9cd40091d5473927c6`.
It changes both `src/PgConnection.ts` and the executed `dist/PgConnection.js`.
Failed acquisition closes a child scope immediately; successful acquisition keeps
the permit until caller scope close, releasing the pinned view before the permit.

Bun applies this patch through this repository's `patchedDependencies`. **Consumers
must configure the same patch at their own install root**; a dependency's patch
configuration does not patch the application's shared driver automatically.
Do not substitute an unpatched registry driver while retaining Effect 4.1.1.

After a clean frozen install, the patched file SHA256 values are:

- Source: `819a39382c962325ab6b471b1b0e4da750f1d55f86f4b00b5b19b815b09bc5ba`
- Runtime: `6739629a8703c16af4060789ec3cf7b63bff74c65eb3f9d65a52a42990407919`

Validation includes interruption at each scheduler evaluation boundary followed by
an uncancelled query and public Pull, plus the PostgreSQL and browser invariant
suites. Upstream-only controls also cover contended acquisition, pipeline drain,
live caller scopes, repeated close, and acquisition timeout. These establish the
tested ownership behavior, not production incident frequency or performance.
Reassess this patch against the upstream fix before upgrading the driver again.
