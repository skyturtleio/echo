import { expect, it } from "vitest"
import type * as Aggregate from "../Aggregate.ts"
import * as Story from "../Story.ts"

export type { Definition, Given, Seed, Then } from "../Story.ts"
export { run, StoryError, stream } from "../Story.ts"

/**
 * Registers a story as a vitest test: *given* events, *when* command, *as* principal, *then*
 * events or rejection (design §1, §5). The test fails when `decide` answers something else.
 * The `then` key trips oxlint's `unicorn/no-thenable`; turn that rule off for `*.story.test.ts`.
 */
export const make = <A extends Aggregate.Any, W extends Aggregate.CommandInstance<A>>(
  aggregate: A,
  name: string,
  story: Story.Definition<A, W>
): void => {
  it(`Story(${aggregate.tag}): ${name}`, () => {
    expect(Story.run(aggregate, story)).toEqual(story.then)
  })
}
