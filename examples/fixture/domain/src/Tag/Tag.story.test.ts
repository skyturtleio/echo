import * as Domain from "echo/domain"
import { Story } from "echo/domain/testing"
import { idea, ideaAgain, member, noteId, otherNoteId, todo } from "../fixtures.ts"
import { Note } from "../Note/Note.aggregate.ts"
import { NoteWritten } from "../Note/WriteNote.command.ts"
import { DefineTag, DuplicateName, TagDefined } from "./DefineTag.command.ts"
import { RemoveTag, TagInUse } from "./RemoveTag.command.ts"
import { Tag } from "./Tag.aggregate.ts"

const { catalogScope, privateScope, ScopeForbidden } = Domain.Scope

// A read over the command's own aggregate's index, for a uniqueness check.
Story.make(Tag, "a second tag with the same name is refused", {
  given: [Story.stream(Tag, idea, [new TagDefined({ name: "idea", scope: privateScope(member) })])],
  when: new DefineTag({ target: ideaAgain, name: "idea" }),
  as: member,
  then: new DuplicateName({ existing: idea })
})

// A read over another aggregate's index, for an in-use check (design §2 item 2); only the
// notes carrying this tag count.
Story.make(Tag, "a tag with notes cannot be removed", {
  given: [
    Story.stream(Tag, idea, [new TagDefined({ name: "idea", scope: privateScope(member) })]),
    Story.stream(Note, noteId, [new NoteWritten({ tagId: idea, text: "a thought", scope: privateScope(member) })]),
    Story.stream(Note, otherNoteId, [new NoteWritten({ tagId: todo, text: "a chore", scope: privateScope(member) })])
  ],
  when: new RemoveTag({ target: idea }),
  as: member,
  then: new TagInUse({ notes: 1 })
})

// The engine's scope check runs before `decide` (design §2 item 8), and it is the write grant:
// a member reads a catalog but cannot command it.
Story.make(Tag, "a member cannot remove a catalog tag", {
  given: [new TagDefined({ name: "idea", scope: catalogScope("tags") })],
  when: new RemoveTag({ target: idea }),
  as: member,
  then: new ScopeForbidden({ scope: catalogScope("tags") })
})
