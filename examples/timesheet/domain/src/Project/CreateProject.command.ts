import * as Domain from "echo/domain"
import * as Schema from "effect/Schema"
import { ProjectId } from "./Project.id.ts"
import { ProjectIndexes } from "./Project.indexes.ts"
import { Code } from "./Project.state.ts"

export class ProjectCreated extends Domain.Event("ProjectCreated", {
  code: Code,
  name: Schema.String,
  scope: Domain.Scope.Scope
}) {}

/** A project the principal can see already carries this code (normalised). */
export class DuplicateCode extends Domain.Rejection("DuplicateCode", { existing: ProjectId }) {}

export class ProjectAlreadyCreated extends Domain.Rejection("ProjectAlreadyCreated", {}) {}

export class CreateProject extends Domain.Command("CreateProject", {
  target: ProjectId,
  input: {
    data: Schema.Struct({
      /** As typed by the member; `decide` normalises it. */
      code: Schema.String,
      name: Schema.String
    })
  },
  events: [ProjectCreated],
  rejects: [DuplicateCode, ProjectAlreadyCreated],
  reads: [ProjectIndexes.by_code]
}) {}
