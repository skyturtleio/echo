import * as Domain from "echo/domain"
import { Story } from "echo/domain/testing"
import { idea, member, noteId, otherMember } from "../fixtures.ts"
import { TagDefined } from "../Tag/DefineTag.command.ts"
import { Tag } from "../Tag/Tag.aggregate.ts"
import { Note } from "./Note.aggregate.ts"
import { NoteWritten, UnknownTag, WriteNote } from "./WriteNote.command.ts"

const { catalogScope, privateScope } = Domain.Scope

// An accepted command: its events, stamped with the author's scope; the read by id sees a catalog stream.
Story.make(Note, "a note against a catalog tag is written into the author's private scope", {
  given: [Story.stream(Tag, idea, [new TagDefined({ name: "idea", scope: catalogScope("tags") })])],
  when: new WriteNote({ target: noteId, tagId: idea, text: "a thought" }),
  as: member,
  then: [new NoteWritten({ tagId: idea, text: "a thought", scope: privateScope(member) })]
})

// A read by id answers `null` for a stream in a scope the principal cannot read.
Story.make(Note, "a note against a tag the author cannot see is refused", {
  given: [Story.stream(Tag, idea, [new TagDefined({ name: "idea", scope: privateScope(otherMember) })])],
  when: new WriteNote({ target: noteId, tagId: idea, text: "a thought" }),
  as: member,
  then: new UnknownTag({ id: idea })
})
