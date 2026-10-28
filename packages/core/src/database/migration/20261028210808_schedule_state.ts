import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261028210808_schedule_state",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`schedule\` ADD \`state\` text;`)
    })
  },
}

export default migration
