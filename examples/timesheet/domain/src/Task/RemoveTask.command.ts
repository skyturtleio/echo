import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { Entry } from "../Entry/Entry.aggregate.ts"
import { TaskId } from "./Task.id.ts"

export class TaskRemoved extends Domain.Event("TaskRemoved", {}) {}

/** The task still has logged entries; `entries` is how many. */
export class TaskInUse extends Domain.Rejection("TaskInUse", { entries: Schema.Int }) {}

export class NoSuchTask extends Domain.Rejection("NoSuchTask", {}) {}

export class RemoveTask extends Domain.Command("RemoveTask", {
  target: TaskId,
  events: [TaskRemoved],
  rejects: [TaskInUse, NoSuchTask],
  /** In-use is a read over the entries, not a counter the app maintains (design §14 item 2). */
  reads: [Entry.by_task]
}) {}
