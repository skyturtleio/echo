import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import { constVoid } from "effect/Function"

export class StorageError extends Data.TaggedError("StorageError")<{ readonly cause: unknown }> {}
export class ProfileBusy extends Data.TaggedError("ProfileBusy")<{ readonly profile: string }> {}

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

/** Separate databases deliberately prevent cache disposal from deleting pending commands. */
export const openDatabase = (name: string) =>
  Effect.acquireRelease(
    Effect.callback<IDBDatabase, StorageError>((resume) => {
      const request = indexedDB.open(name, 1)
      request.addEventListener("upgradeneeded", () => {
        request.result.createObjectStore("data")
      })
      request.addEventListener("success", () => resume(Effect.succeed(request.result)))
      request.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: request.error }))))
    }),
    (db) => Effect.sync(() => db.close())
  )

export const load = (db: IDBDatabase) =>
  Effect.callback<unknown, StorageError>((resume) => {
    const tx = db.transaction("data", "readonly")
    const request = tx.objectStore("data").get("snapshot")
    tx.addEventListener("complete", () => {
      const value: unknown = request.result
      resume(Effect.succeed(value))
    })
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })

/** Resolve only on transaction completion, never on request success. */
export const save = (db: IDBDatabase, value: unknown) =>
  Effect.callback<void, StorageError>((resume) => {
    const tx = db.transaction("data", "readwrite", { durability: "strict" })
    tx.objectStore("data").put(value, "snapshot")
    tx.addEventListener("complete", () => resume(Effect.void))
    tx.addEventListener("abort", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
    tx.addEventListener("error", () => resume(Effect.fail(new StorageError({ cause: tx.error }))))
  })
