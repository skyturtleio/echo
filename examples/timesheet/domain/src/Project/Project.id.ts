import * as Schema from "effect/Schema"

export const ProjectId = Schema.String.pipe(Schema.brand("ProjectId"))
export type ProjectId = typeof ProjectId.Type
