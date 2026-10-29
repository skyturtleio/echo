import * as Schema from "effect/Schema"

export const TaskId = Schema.String.pipe(Schema.brand("TaskId"))
export type TaskId = typeof TaskId.Type
