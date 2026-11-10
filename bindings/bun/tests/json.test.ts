/*
 * Copyright 2026 Nowledge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, expect, test } from "bun:test";
import { parseLosslessJson, stringifyParameters } from "../src/json.ts";

describe("lossless boundary JSON", () => {
  test("keeps safe integers as numbers and wide integers as bigint", () => {
    expect(parseLosslessJson("[9007199254740991,9007199254740992,-9223372036854775808,1.5]")).toEqual([
      9007199254740991,
      9007199254740992n,
      -9223372036854775808n,
      1.5,
    ]);
  });

  test("does not let an object key mutate the parser result prototype", () => {
    const parsed = parseLosslessJson('{"__proto__":{"polluted":true},"constructor":1}') as Record<
      string,
      unknown
    >;
    expect(Object.getPrototypeOf(parsed)).toBeNull();
    expect(parsed["__proto__"]).toEqual({ polluted: true });
    expect(parsed["constructor"]).toBe(1);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  test("serializes bigint and binary values without precision loss", () => {
    expect(
      stringifyParameters({
        maximum: 9223372036854775807n,
        binary: new Uint8Array([0, 255]),
      }),
    ).toBe('{"maximum":9223372036854775807,"binary":{"$binary":"AP8="}}');
  });

  test("rejects invalid parameter graphs", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => stringifyParameters(cyclic)).toThrow("must not contain cycles");
    expect(() => stringifyParameters(Number.NaN)).toThrow("finite numbers");
    expect(() => stringifyParameters(9007199254740992)).toThrow("must use bigint");
  });
});
