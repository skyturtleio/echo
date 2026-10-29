import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { ProjectId } from "../Project/Project.id.ts"

/** A task time is booked on, within one project. `null` is the state of a task stream nothing has happened to. */
export const TaskState = Schema.NullOr(
  Schema.Struct({
    /** What the task is, e.g. `Code review`. */
    name: Schema.NonEmptyString,
    projectId: ProjectId,
    scope: Domain.Scope.Scope,
    removed: Schema.Boolean
  })
)
export type TaskState = typeof TaskState.Type
