import { expect, story } from "../../stories/playwright/story"

story("lines up model tags whether or not a model has variants", async ({ mount, page }, testInfo) => {
  await mount("app-dialog-model-selector--free")
  const dialog = page.getByRole("dialog")

  const tag = (name: string) =>
    dialog.locator('[data-slot="search-list-row"]', { hasText: name }).last().getByText("Free", { exact: true })

  await expect(tag("Beam Free")).toBeVisible()
  await expect(tag("Big Pickle")).toBeVisible()
  await page.mouse.move(0, 0)

  const right = async (name: string) => {
    const box = await tag(name).boundingBox()

    if (!box) throw new Error(`No Free tag for ${name}`)

    return box.x + box.width
  }

  expect(await right("Big Pickle")).toBeCloseTo(await right("Beam Free"), 0)
  await dialog.screenshot({ path: testInfo.outputPath("model-selector-free.png") })
})
