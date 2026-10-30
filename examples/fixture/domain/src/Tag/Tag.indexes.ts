import * as Domain from "echo/domain"
import { TagId } from "./Tag.id.ts"
import { TagState } from "./Tag.state.ts"

/** Declared apart from the aggregate so `DefineTag` can read `by_name` without importing it. */
export const TagIndexes = Domain.Index(TagId, TagState, {
  by_name: ["name"]
})
