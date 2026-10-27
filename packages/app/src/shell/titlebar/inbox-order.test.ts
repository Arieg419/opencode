import { expect, test } from "bun:test"
import { arrivalOrder } from "./inbox-order"

test("starts in the incoming order", () => {
  expect(arrivalOrder([], ["a", "b", "c"])).toEqual(["a", "b", "c"])
})

test("keeps existing keys in place when the incoming order changes", () => {
  expect(arrivalOrder(["a", "b", "c"], ["c", "a", "b"])).toEqual(["a", "b", "c"])
})

test("appends new keys after existing ones", () => {
  expect(arrivalOrder(["a", "b"], ["d", "b", "a", "c"])).toEqual(["a", "b", "d", "c"])
})

test("drops keys that left and appends them again when they return", () => {
  expect(arrivalOrder(["a", "b", "c"], ["c", "a"])).toEqual(["a", "c"])
  expect(arrivalOrder(["a", "c"], ["b", "c", "a"])).toEqual(["a", "c", "b"])
})
