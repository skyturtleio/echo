import * as Schema from "effect/Schema"

export const EntryId = Schema.String.pipe(Schema.brand("EntryId"))
export type EntryId = typeof EntryId.Type
