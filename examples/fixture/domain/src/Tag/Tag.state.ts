import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"

/** A defined tag. `null` is the state of a tag stream nothing has happened to. */
export const TagState = Schema.NullOr(
  Schema.Struct({
    name: Schema.String,
    /** The member's private scope, or `catalog:tags` for the catalog the server writes. */
    scope: Domain.Scope.Scope,
    removed: Schema.Boolean
  })
)
export type TagState = typeof TagState.Type
