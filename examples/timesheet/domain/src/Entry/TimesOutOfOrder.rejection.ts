import * as Domain from "echo/domain"

/** The entry's times do not run started ≤ ended. */
export class TimesOutOfOrder extends Domain.Rejection("TimesOutOfOrder", {}) {}
