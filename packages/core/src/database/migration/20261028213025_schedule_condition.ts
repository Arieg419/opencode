import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261028213025_schedule_condition",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`schedule\` ADD \`if\` text;`)
      yield* tx.run(`ALTER TABLE \`schedule\` ADD \`state\` text;`)
    })
  },
}

export default migration
