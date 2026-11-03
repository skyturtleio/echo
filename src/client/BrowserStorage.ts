import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import { constVoid } from "effect/Function"
import * as Schema from "effect/Schema"

export class StorageError extends Data.TaggedError("StorageError")<{ readonly cause: unknown }> {}
export class ProfileBusy extends Data.TaggedError("ProfileBusy")<{ readonly profile: string }> {}
export class CompatibilityError extends Data.TaggedError("CompatibilityError")<{
  readonly reason: "blocked" | "outdated" | "superseded" | "invalidGeneration" | "predictionUnavailable"
}> {}

const Generation = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER - 2)
)

// Browser platform edge; use the same native cryptographic source as Web Locks/IndexedDB's host.
// oxlint-disable-next-line effecttsgo/crypto-random-uuid, effecttsgo/crypto-random-uuid-in-effect
export const randomId = Effect.sync(() => crypto.randomUUID())

/** Browser platform edge: the lock is held until the owning Effect scope closes. */
export const ownProfile = (profile: string, wait = false) =>
  Effect.acquireRelease(
    Effect.callback<Effect.Effect<void>, StorageError | ProfileBusy>((resume) => {
      if (!globalThis.navigator?.locks) {
        resume(Effect.fail(new StorageError({ cause: "Web Locks unavailable" })))
        return
      }
      let release = constVoid
      // Web Locks requires a native pending Promise to retain ownership; this is not an Effect task.
      // oxlint-disable-next-line effecttsgo/new-promise
      const held = new Promise<void>((resolve) => {
        release = resolve
      })
      const controller = new AbortController()
      const finished = navigator.locks
        .request(`echo:${profile}`, wait ? { signal: controller.signal } : { ifAvailable: true }, (lock) => {
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
        controller.abort()
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
}

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
      const request = indexedDB.open(name, 2 + compatibilityGeneration)
      request.addEventListener("upgradeneeded", () => {
        if (abandoned) {
          request.transaction?.abort()
          return
        }
        if (!request.result.objectStoreNames.contains("data")) request.result.createObjectStore("data")
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

/** Results are borrowed immutable values. Token and snapshot are read in one transaction. */
export const load = (db: Database) =>
  Effect.callback<unknown, StorageError | CompatibilityError>((resume) => {
    if (db.superseded) {
      resume(Effect.fail(new CompatibilityError({ reason: "superseded" })))
      return
    }
    const tx = db.database.transaction("data", "readonly")
    const store = tx.objectStore("data")
    const request = store.get("token")
    let value: unknown
    request.addEventListener("success", () => {
      if (db.cache !== undefined && db.cache.token === request.result) value = db.cache.value
      else {
        const snapshot = store.get("snapshot")
        snapshot.addEventListener("success", () => {
          value = snapshot.result
        })
      }
    })
    tx.addEventListener("complete", () => {
      db.cache = { token: request.result, value }
      resume(Effect.succeed(value))
    })
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })

/** Resolve only on transaction completion, never on request success.
 * Do not cache the caller's value: the next load borrows IndexedDB's detached clone. */
export const save = (db: Database, value: unknown) =>
  Effect.callback<void, StorageError | CompatibilityError>((resume) => {
    if (db.superseded) {
      resume(Effect.fail(new CompatibilityError({ reason: "superseded" })))
      return
    }
    const tx = db.database.transaction("data", "readwrite", { durability: "strict" })
    tx.objectStore("data").put(value, "snapshot")
    // Browser platform edge: a unique token also fences reset/recreation (no counter ABA).
    // oxlint-disable-next-line effecttsgo/crypto-random-uuid, effecttsgo/crypto-random-uuid-in-effect
    tx.objectStore("data").put(crypto.randomUUID(), "token")
    tx.addEventListener("complete", () => resume(Effect.void))
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })
