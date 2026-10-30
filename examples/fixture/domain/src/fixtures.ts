/** Shared story fixtures: two members and the ids the stories use. */
import * as Domain from "echo/domain"
import { NoteId } from "./Note/Note.id.ts"
import { TagId } from "./Tag/Tag.id.ts"

export const member = Domain.Scope.Principal.make("member-ada")
export const otherMember = Domain.Scope.Principal.make("member-grace")

export const noteId = NoteId.make("note-1")
export const otherNoteId = NoteId.make("note-2")
export const idea = TagId.make("tag-idea")
export const ideaAgain = TagId.make("tag-idea-again")
export const todo = TagId.make("tag-todo")
