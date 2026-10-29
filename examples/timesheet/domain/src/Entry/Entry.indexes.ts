import * as Domain from "echo/domain"
import { EntryId } from "./Entry.id.ts"
import { EntryState } from "./Entry.state.ts"

export const EntryIndexes = Domain.Index(EntryId, EntryState, {
  by_day: ["scope", "day"],
  by_task: ["taskId"],
  by_project: ["projectId"]
})
