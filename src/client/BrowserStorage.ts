import * as Arr from "effect/Array"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import { constVoid } from "effect/Function"
import * as Option from "effect/Option"
import * as Order from "effect/Order"
import * as Schema from "effect/Schema"
import { Replica, Row } from "./Model.ts"
import { Position, StreamRef } from "../protocol.ts"

export class StorageError extends Data.TaggedError("StorageError")<{ readonly cause: unknown }> {}
export class ProfileBusy extends Data.TaggedError("ProfileBusy")<{ readonly profile: string }> {}
export class CompatibilityError extends Data.TaggedError("CompatibilityError")<{
  readonly reason: "blocked" | "outdated" | "superseded" | "invalidGeneration" | "predictionUnavailable"
}> {}

// DOM failures are operational; thrown application errors must remain defects.
const storageFailure = (cause: unknown) =>
  cause instanceof DOMException ? Effect.fail(new StorageError({ cause })) : Effect.die(cause)

const Generation = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER - 3)
)

// Browser platform edge; use the same native cryptographic source as Web Locks/IndexedDB's host.
// oxlint-disable-next-line effecttsgo/crypto-random-uuid-in-effect -- browser identity uses the host cryptographic source
export const randomId = Effect.sync(() => crypto.randomUUID())

/** Browser platform edge: the lock is held until the owning Effect scope closes. */
export const ownProfile = (profile: string, wait = false) =>
  Effect.acquireRelease(
    Effect.callback<Effect.Effect<void>, StorageError | ProfileBusy>((resume, signal) => {
      if (globalThis.navigator?.locks === undefined) {
        resume(Effect.fail(new StorageError({ cause: "Web Locks unavailable" })))
        return
      }
      let release = constVoid
      // Web Locks requires a native pending Promise to retain ownership; this is not an Effect task.
      // oxlint-disable-next-line effecttsgo/new-promise -- Web Locks retains ownership until this native Promise resolves
      const held = new Promise<void>((resolve) => {
        release = resolve
      })
      const finished = navigator.locks
        .request(`echosync:${profile}`, wait ? { signal } : { ifAvailable: true }, (lock) => {
          if (lock === null) {
            resume(Effect.fail(new ProfileBusy({ profile })))
            return
          }
          resume(
            Effect.succeed(
              Effect.promise(() => {
                release()
                return finished
              })
            )
          )
          return held
        })
        .catch((cause: unknown) => resume(Effect.fail(new StorageError({ cause }))))
      return Effect.sync(() => {
        release()
      })
    }),
    (release) => release,
    { interruptible: true }
  ).pipe(Effect.asVoid)

/** Cross-tab serialization. A dead tab releases the browser-owned lock automatically. */
export const withLock = <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(Effect.andThen(ownProfile(name, true), effect))

export interface Database {
  readonly database: IDBDatabase
  superseded: boolean
  cache: { readonly token: unknown; readonly value: unknown } | undefined
  replica?:
    | {
        readonly token: unknown
        readonly value: Replica
        readonly orders: ReadonlyMap<string, number>
        readonly nextOrder: number
      }
    | undefined
}

const ReplicaHeader = Schema.Struct({
  _storage: Schema.Literal("rows"),
  epoch: Replica.fields.epoch,
  revision: Replica.fields.revision,
  grants: Replica.fields.grants,
  cursors: Replica.fields.cursors,
  nextOrder: Position
})
const OrderedRow = Schema.Struct({ order: Position, row: Row })
const StoredRow = Schema.Union([OrderedRow, Schema.Struct({ gone: StreamRef })])
const rowKey = (row: typeof StreamRef.Type) => `${row.aggregate.length}:${row.aggregate}${row.id}`

/** Separate databases deliberately prevent cache disposal from deleting pending commands.
 * Version 2 introduced change tokens; later versions fence incompatible app bundles. */
export const openDatabase = (name: string, compatibilityGeneration = 1) =>
  Effect.acquireRelease(
    Effect.callback<Database, StorageError | CompatibilityError>((resume) => {
      if (!Schema.is(Generation)(compatibilityGeneration)) {
        resume(Effect.fail(new CompatibilityError({ reason: "invalidGeneration" })))
        return
      }
      let abandoned = false
      let request: IDBOpenDBRequest
      try {
        request = indexedDB.open(name, 3 + compatibilityGeneration)
      } catch (cause) {
        resume(storageFailure(cause))
        return
      }
      request.addEventListener("upgradeneeded", () => {
        if (abandoned) {
          request.transaction?.abort()
          return
        }
        try {
          if (!request.result.objectStoreNames.contains("data")) request.result.createObjectStore("data")
          if (!request.result.objectStoreNames.contains("rows")) request.result.createObjectStore("rows")
        } catch (cause) {
          request.transaction?.abort()
          resume(storageFailure(cause))
        }
      })
      request.addEventListener("blocked", () => {
        abandoned = true
        resume(Effect.fail(new CompatibilityError({ reason: "blocked" })))
      })
      request.addEventListener("success", () => {
        if (abandoned) request.result.close()
        else {
          const db: Database = { database: request.result, superseded: false, cache: undefined }
          db.database.addEventListener("versionchange", () => {
            // Close synchronously: the upgrader may already hold the profile mutation lock.
            db.superseded = true
            db.database.close()
          })
          resume(Effect.succeed(db))
        }
      })
      request.addEventListener("error", () =>
        resume(
          Effect.fail(
            request.error?.name === "VersionError"
              ? new CompatibilityError({ reason: "outdated" })
              : new StorageError({ cause: request.error })
          )
        )
      )
      return Effect.sync(() => {
        abandoned = true
      })
    }),
    (db) => Effect.sync(() => db.database.close()),
    { interruptible: true }
  )

export const awaitSuperseded = (db: Database) =>
  Effect.callback<void>((resume) => {
    const changed = () => resume(Effect.void)
    db.database.addEventListener("versionchange", changed)
    if (db.superseded) changed()
    return Effect.sync(() => db.database.removeEventListener("versionchange", changed))
  })

/** None acknowledges a caller's committed token; Some(undefined) is an absent snapshot.
 * Token and snapshot are read in one transaction; acknowledgment never caches caller-owned data. */
export const loadIfChanged = (db: Database, acknowledgedToken?: string) =>
  Effect.callback<Option.Option<unknown>, StorageError | CompatibilityError>((resume) => {
    if (db.superseded) {
      resume(Effect.fail(new CompatibilityError({ reason: "superseded" })))
      return
    }
    let tx: IDBTransaction
    let store: IDBObjectStore
    let request: IDBRequest<unknown>
    try {
      tx = db.database.transaction(["data", "rows"], "readonly")
      store = tx.objectStore("data")
      request = store.get("token")
    } catch (cause) {
      resume(storageFailure(cause))
      return
    }
    let value: unknown
    let storedRows: unknown
    let baseRows: unknown
    let unchanged = false
    request.addEventListener("success", () => {
      if (acknowledgedToken !== undefined && acknowledgedToken === request.result) unchanged = true
      else if (db.cache !== undefined && db.cache.token === request.result) value = db.cache.value
      else {
        try {
          const snapshot = store.get("snapshot")
          snapshot.addEventListener("success", () => {
            value = snapshot.result
            if (Schema.is(ReplicaHeader)(value)) {
              try {
                const base = store.get("base")
                base.addEventListener("success", () => {
                  baseRows = base.result
                })
                const rows = tx.objectStore("rows").getAll()
                rows.addEventListener("success", () => {
                  storedRows = rows.result
                })
              } catch (cause) {
                tx.abort()
                resume(storageFailure(cause))
              }
            }
          })
        } catch (cause) {
          // Prevent complete from installing an absent snapshot in the cache after a failed read.
          tx.abort()
          resume(storageFailure(cause))
        }
      }
    })
    tx.addEventListener("complete", () => {
      if (unchanged) {
        resume(Effect.succeed(Option.none()))
        return
      }
      const header = value
      if (Schema.is(ReplicaHeader)(header)) {
        resume(
          Schema.decodeUnknownEffect(Schema.Struct({ base: Schema.Array(Row), changes: Schema.Array(StoredRow) }))({
            base: baseRows,
            changes: storedRows
          }).pipe(
            Effect.mapError((cause) => new StorageError({ cause })),
            Effect.map(({ base, changes }) => {
              const rows = new Map(Arr.map(base, (row, order) => [rowKey(row), { row, order }]))
              for (const change of changes) {
                if ("gone" in change) rows.delete(rowKey(change.gone))
                else rows.set(rowKey(change.row), change)
              }
              const sorted = Arr.sort(
                [...rows.values()],
                Order.mapInput(Order.Number, (item: typeof OrderedRow.Type) => item.order)
              )
              const replica: Replica = {
                epoch: header.epoch,
                revision: header.revision,
                grants: header.grants,
                cursors: header.cursors,
                rows: Arr.map(sorted, (item) => item.row)
              }
              db.replica = {
                token: request.result,
                value: replica,
                orders: new Map(Arr.map(sorted, (item) => [rowKey(item.row), item.order])),
                nextOrder: header.nextOrder
              }
              db.cache = { token: request.result, value: replica }
              return Option.some(replica)
            })
          )
        )
      } else {
        if (db.replica?.token !== request.result) db.replica = undefined
        db.cache = { token: request.result, value }
        resume(Effect.succeed(Option.some(value)))
      }
    })
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })

/** Results are borrowed immutable values detached from callers' saved objects. */
export const load = (db: Database) => loadIfChanged(db).pipe(Effect.map(Option.getOrUndefined))

/** Resolve only on transaction completion, never on request success.
 * Do not cache the caller's value: the next load borrows IndexedDB's detached clone. */
export const save = (db: Database, value: unknown) =>
  Effect.callback<string, StorageError | CompatibilityError>((resume) => {
    if (db.superseded) {
      resume(Effect.fail(new CompatibilityError({ reason: "superseded" })))
      return
    }
    // Browser platform edge: a unique token also fences reset/recreation (no counter ABA).
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid-in-effect -- native token fences cross-tab reset and recreation
    const token = crypto.randomUUID()
    let tx: IDBTransaction
    try {
      tx = db.database.transaction(["data", "rows"], "readwrite", { durability: "strict" })
    } catch (cause) {
      resume(storageFailure(cause))
      return
    }
    try {
      tx.objectStore("data").put(value, "snapshot")
      tx.objectStore("data").put(token, "token")
      tx.objectStore("data").delete("base")
      tx.objectStore("rows").clear()
    } catch (cause) {
      // A successful first put must never commit without its matching token.
      tx.abort()
      resume(storageFailure(cause))
      return
    }
    tx.addEventListener("complete", () => {
      db.replica = undefined
      resume(Effect.succeed(token))
    })
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })

/** A strict transaction installs metadata, changed records and token together. The caller
 * holds the profile mutation lock; the token check also refuses a stale storage baseline. */
export const saveReplica = (db: Database, value: Replica, replace = false) =>
  Effect.callback<{ readonly token: string; readonly writes: number }, StorageError | CompatibilityError>((resume) => {
    if (db.superseded) {
      resume(Effect.fail(new CompatibilityError({ reason: "superseded" })))
      return
    }
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid-in-effect -- native token fences cross-tab reset and recreation
    const token = crypto.randomUUID()
    let tx: IDBTransaction
    try {
      tx = db.database.transaction(["data", "rows"], "readwrite", { durability: "strict" })
    } catch (cause) {
      resume(storageFailure(cause))
      return
    }
    const baseline = db.replica
    const full =
      replace ||
      baseline === undefined ||
      baseline.value.rows.length === 0 ||
      baseline.nextOrder + value.rows.length > Number.MAX_SAFE_INTEGER
    const orders = new Map<string, number>()
    let nextOrder = full ? 0 : baseline.nextOrder
    let writes = 0
    const apply = () => {
      try {
        const store = tx.objectStore("rows")
        const prior = new Map(Arr.map(baseline?.value.rows ?? [], (row) => [rowKey(row), row]))
        let lastOrder = -1
        let append = false
        if (full) {
          store.clear()
          tx.objectStore("data").put(value.rows, "base")
        }
        for (const row of value.rows) {
          const key = rowKey(row)
          const old = prior.get(key)
          const order = baseline?.orders.get(key)
          if (full) {
            orders.set(key, nextOrder++)
            writes++
          } else if (
            !append &&
            old !== undefined &&
            order !== undefined &&
            order > lastOrder &&
            Equal.equals(old, row)
          ) {
            orders.set(key, order)
            lastOrder = order
          } else {
            append = true
            orders.set(key, nextOrder)
            store.put({ order: nextOrder++, row }, [row.aggregate, row.id])
            writes++
          }
          prior.delete(key)
        }
        for (const row of prior.values()) {
          if (!full) store.put({ gone: { aggregate: row.aggregate, id: row.id } }, [row.aggregate, row.id])
          writes++
        }
        tx.objectStore("data").put(
          {
            _storage: "rows",
            epoch: value.epoch,
            revision: value.revision,
            grants: value.grants,
            cursors: value.cursors,
            nextOrder
          },
          "snapshot"
        )
        tx.objectStore("data").put(token, "token")
      } catch (cause) {
        tx.abort()
        resume(storageFailure(cause))
      }
    }
    try {
      const current = tx.objectStore("data").get("token")
      current.addEventListener("success", () => {
        if (baseline !== undefined && baseline.token !== current.result) {
          tx.abort()
          resume(Effect.fail(new StorageError({ cause: "Replica changed before commit" })))
          return
        }
        apply()
      })
    } catch (cause) {
      tx.abort()
      resume(storageFailure(cause))
      return
    }
    tx.addEventListener("complete", () => {
      db.replica = { token, value, orders, nextOrder }
      resume(Effect.succeed({ token, writes }))
    })
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })
