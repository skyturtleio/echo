import * as Domain from "echo/domain"
import { Tag } from "../Tag/Tag.aggregate.ts"
import { NoteId } from "./Note.id.ts"
import { NoteIndexes } from "./Note.indexes.ts"
import { NoteState } from "./Note.state.ts"
import { NoteAlreadyWritten, NoteWritten, UnknownTag, WriteNote } from "./WriteNote.command.ts"

/** One stream per note, in its author's private scope. */
export class Note extends Domain.Aggregate("Note", {
  id: NoteId,
  state: NoteState,
  initial: null,
  commands: [WriteNote],
  scope: (state) => state?.scope ?? null,
  indexes: NoteIndexes,
  decide: {
    WriteNote: (state, { tagId, text }, ctx) => {
      if (state !== null) return new NoteAlreadyWritten()
      if (ctx.read(Tag, tagId) === null) return new UnknownTag({ id: tagId })
      return [new NoteWritten({ tagId, text, scope: Domain.Scope.privateScope(ctx.principal) })]
    }
  },
  evolve: {
    NoteWritten: (_, { tagId, text, scope }) => ({ tagId, text, scope })
  }
}) {}
