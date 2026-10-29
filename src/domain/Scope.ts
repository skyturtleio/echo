import * as Schema from "effect/Schema"
import * as Str from "effect/String"
import * as Rejection from "./Rejection.ts"

/** Who sent a command: the authenticated user id (design §7). */
export const Principal = Schema.String.pipe(Schema.brand("Principal"))
export type Principal = typeof Principal.Type

/**
 * An opaque visibility id (`user:<id>`, `household:<id>`, `catalog:projects`). Every stream is
 * in exactly one scope at a time, given by `Aggregate.scope(state)` (design §7).
 */
export const Scope = Schema.String.pipe(Schema.brand("Scope"))
export type Scope = typeof Scope.Type

/** The scope only its principal can see and write. */
export const privateScope = (principal: Principal): Scope => Scope.make(`user:${principal}`)

/** A catalog scope: every principal reads it, only the server writes it (design §7). */
export const catalogScope = (name: string): Scope => Scope.make(`catalog:${name}`)

/**
 * The grants a principal holds before there is a grants aggregate (phase 3b): their private
 * scope as writer and every catalog as reader. Shared scopes (`household:<id>`) need the grants
 * aggregate, which arrives in phase 3b.
 */
export const canRead = (principal: Principal, scope: Scope): boolean =>
  scope === privateScope(principal) || Str.startsWith("catalog:")(scope)

export const canWrite = (principal: Principal, scope: Scope): boolean => scope === privateScope(principal)

/**
 * The engine's answer when a principal commands a stream whose scope they hold no writer grant
 * on. It is checked once, generically, before `decide` runs, so no aggregate repeats the
 * ownership rule (design §15, story 5).
 */
export class ScopeForbidden extends Rejection.make("ScopeForbidden", { scope: Scope }) {}
