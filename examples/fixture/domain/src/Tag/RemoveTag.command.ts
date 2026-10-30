import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { NoteIndexes } from "../Note/Note.indexes.ts"
import { TagId } from "./Tag.id.ts"

export class TagRemoved extends Domain.Event("TagRemoved", {}) {}

/** Notes still carry the tag; `notes` is how many. */
export class TagInUse extends Domain.Rejection("TagInUse", { notes: Schema.Int }) {}

export class NoSuchTag extends Domain.Rejection("NoSuchTag", {}) {}

/** Exercises a read over another aggregate's index (design §2 item 2). */
export class RemoveTag extends Domain.Command("RemoveTag", {
  target: TagId,
  events: [TagRemoved],
  rejects: [TagInUse, NoSuchTag],
  reads: [NoteIndexes.by_tag]
}) {}
