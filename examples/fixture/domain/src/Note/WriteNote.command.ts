import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { Tag } from "../Tag/Tag.aggregate.ts"
import { TagId } from "../Tag/Tag.id.ts"
import { NoteId } from "./Note.id.ts"

export class NoteWritten extends Domain.Event("NoteWritten", {
  tagId: TagId,
  text: Schema.String,
  scope: Domain.Scope.Scope
}) {}

/** No tag with this id is visible to the author: it does not exist, or it is another member's. */
export class UnknownTag extends Domain.Rejection("UnknownTag", { id: TagId }) {}

export class NoteAlreadyWritten extends Domain.Rejection("NoteAlreadyWritten", {}) {}

/** Exercises a read of another aggregate by id. */
export class WriteNote extends Domain.Command("WriteNote", {
  target: NoteId,
  input: { tagId: TagId, text: Schema.String },
  events: [NoteWritten],
  rejects: [UnknownTag, NoteAlreadyWritten],
  reads: [Tag]
}) {}
