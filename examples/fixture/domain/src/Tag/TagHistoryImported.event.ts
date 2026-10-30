import * as Domain from "echo/domain"
import { TagState } from "./Tag.state.ts"

/** A known snapshot imported now, not a reconstruction of unknown past actions. */
export class TagHistoryImported extends Domain.Event("TagHistoryImported", {
  snapshot: TagState.members[0]
}) {}
