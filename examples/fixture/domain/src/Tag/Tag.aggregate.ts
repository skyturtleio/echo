import * as Domain from "echo/domain"
import * as Arr from "effect/Array"
import * as Option from "effect/Option"
import { NoteIndexes } from "../Note/Note.indexes.ts"
import { DefineTag, DuplicateName, TagAlreadyDefined, TagDefined } from "./DefineTag.command.ts"
import { NoSuchTag, RemoveTag, TagInUse, TagRemoved } from "./RemoveTag.command.ts"
import { TagId } from "./Tag.id.ts"
import { TagIndexes } from "./Tag.indexes.ts"
import { TagState } from "./Tag.state.ts"

/**
 * One stream per tag, in a member's private scope or in the `catalog:tags` scope the server
 * writes. Who may command it is not decided here: the engine refuses a command on a stream
 * whose scope the principal cannot write before `decide` runs (design §2 item 8).
 */
export class Tag extends Domain.Aggregate("Tag", {
  id: TagId,
  state: TagState,
  initial: null,
  commands: [DefineTag, RemoveTag],
  scope: (state) => state?.scope ?? null,
  indexes: TagIndexes,
  decide: {
    DefineTag: (state, { name }, ctx) => {
      if (state !== null) return new TagAlreadyDefined()
      const existing = Arr.head(ctx.read(TagIndexes.by_name, name))
      if (Option.isSome(existing)) return new DuplicateName({ existing: existing.value.id })
      return [new TagDefined({ name, scope: Domain.Scope.privateScope(ctx.principal) })]
    },
    RemoveTag: (state, { target }, ctx) => {
      if (state === null || state.removed) return new NoSuchTag()
      const notes = ctx.read(NoteIndexes.by_tag, target).length
      return notes > 0 ? new TagInUse({ notes }) : [new TagRemoved()]
    }
  },
  evolve: {
    TagDefined: (_, { name, scope }) => ({ name, scope, removed: false }),
    TagRemoved: (state) => (state === null ? state : { ...state, removed: true })
  }
}) {}
