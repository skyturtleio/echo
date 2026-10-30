import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { TagId } from "./Tag.id.ts"
import { TagIndexes } from "./Tag.indexes.ts"

export class TagDefined extends Domain.Event("TagDefined", {
  name: Schema.String,
  scope: Domain.Scope.Scope
}) {}

/** A tag the principal can see already carries this name. */
export class DuplicateName extends Domain.Rejection("DuplicateName", { existing: TagId }) {}

export class TagAlreadyDefined extends Domain.Rejection("TagAlreadyDefined", {}) {}

/** Exercises a read over the command's own aggregate's index. */
export class DefineTag extends Domain.Command("DefineTag", {
  target: TagId,
  input: { name: Schema.String },
  events: [TagDefined],
  rejects: [DuplicateName, TagAlreadyDefined],
  reads: [TagIndexes.by_name]
}) {}
