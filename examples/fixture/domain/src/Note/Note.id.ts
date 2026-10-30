import * as Schema from "effect/Schema"

export const NoteId = Schema.String.pipe(Schema.brand("NoteId"))
export type NoteId = typeof NoteId.Type
