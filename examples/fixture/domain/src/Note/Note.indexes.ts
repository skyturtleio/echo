import * as Domain from "echo/domain"
import { NoteId } from "./Note.id.ts"
import { NoteState } from "./Note.state.ts"

/** Declared apart from the aggregate so `RemoveTag` can read `by_tag` without importing `Note`. */
export const NoteIndexes = Domain.Index(NoteId, NoteState, {
  by_tag: ["tagId"]
})
