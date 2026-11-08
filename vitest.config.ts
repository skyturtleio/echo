import { defineConfig } from "vitest/config"

// Examples import the engine the way an app does (`echo-sync/domain`); these aliases resolve that
// name to the source in this repository, matching `paths` in tsconfig.json. Order matters:
// Vite takes the first alias whose key equals the import or prefixes it with a `/`, so the
// longer `echo-sync/domain/testing` must come before `echo-sync/domain`.
const domain = `${import.meta.dirname}/src/domain`

export default defineConfig({
  resolve: {
    alias: {
      "echo-sync/domain/testing": `${domain}/testing/index.ts`,
      "echo-sync/domain": `${domain}/index.ts`
    }
  },
  test: {
    include: ["src/**/*.test.ts", "test/**/*.test.ts", "examples/**/*.test.ts"]
  }
})
