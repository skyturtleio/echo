import * as Domain from "echo/domain"
import { ProjectId } from "./Project.id.ts"
import { ProjectState } from "./Project.state.ts"

/** Declared apart from the aggregate so `CreateProject` can read `by_code` without importing it. */
export const ProjectIndexes = Domain.Index(ProjectId, ProjectState, {
  by_code: ["code"]
})
