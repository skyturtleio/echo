import { defineConfig } from "vitest/config"

// Examples import the engine the way an app does (`echo/domain`); these aliases resolve that
// name to the source in this repository, matching `paths` in tsconfig.json.
const domain = `${import.meta.dirname}/src/domain`

export default defineConfig({
  resolve: {
    alias: {
      "echo/domain/testing": `${domain}/testing/index.ts`,
      "echo/domain": `${domain}/index.ts`
    }
  },
  test: {
    include: ["src/**/*.test.ts", "test/**/*.test.ts", "examples/**/*.test.ts"],
    passWithNoTests: true
  }
})
