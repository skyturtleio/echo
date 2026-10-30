import * as Schema from "effect/Schema"

export const TagId = Schema.String.pipe(Schema.brand("TagId"))
export type TagId = typeof TagId.Type
