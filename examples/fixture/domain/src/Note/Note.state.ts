import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { TagId } from "../Tag/Tag.id.ts"

/** A written note. `null` is the state of a note stream nothing has happened to. */
export const NoteState = Schema.NullOr(
  Schema.Struct({
    tagId: TagId,
    text: Schema.String,
    /** The author's private scope; set when the note is written. */
    scope: Domain.Scope.Scope
  })
)
export type NoteState = typeof NoteState.Type
