import * as Order from "effect/Order"
import * as Schema from "effect/Schema"
import * as Str from "effect/String"

/** A wall-clock time in the entry's local day, `HH:MM`, zero-padded so that string order is time order. */
export const LocalTime = Schema.String.check(Schema.isPattern(/^([01]\d|2[0-3]):[0-5]\d$/)).pipe(
  Schema.brand("LocalTime")
)
export type LocalTime = typeof LocalTime.Type

/** When an entry's work started and ended. */
export interface Times {
  readonly startedAt: LocalTime
  readonly endedAt: LocalTime
}

const notAfter = Order.isLessThanOrEqualTo(Str.Order)

/** started ≤ ended: an entry cannot end before it starts. */
export const inOrder = ({ endedAt, startedAt }: Times): boolean => notAfter(startedAt, endedAt)
