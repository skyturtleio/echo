import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { ProjectId } from "../Project/Project.id.ts"
import { TaskId } from "./Task.id.ts"

export class TaskAdded extends Domain.Event("TaskAdded", {
  name: Schema.NonEmptyString,
  projectId: ProjectId,
  scope: Domain.Scope.Scope
}) {}

export class TaskAlreadyAdded extends Domain.Rejection("TaskAlreadyAdded", {}) {}

export class AddTask extends Domain.Command("AddTask", {
  target: TaskId,
  input: { data: Schema.Struct({ name: Schema.NonEmptyString, projectId: ProjectId }) },
  events: [TaskAdded],
  rejects: [TaskAlreadyAdded]
}) {}
