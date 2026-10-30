import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { NoteId } from "./Note.id.ts"

export class NoteEdited extends Domain.Event("NoteEdited", { text: Schema.String }) {}

export class NoSuchNote extends Domain.Rejection("NoSuchNote", {}) {}

/** Reads nothing; exists so a story can command another member's stream. */
export class EditNote extends Domain.Command("EditNote", {
  target: NoteId,
  input: { text: Schema.String },
  events: [NoteEdited],
  rejects: [NoSuchNote]
}) {}
