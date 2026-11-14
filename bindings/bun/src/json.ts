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

const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;
const MIN_SAFE_INTEGER = BigInt(Number.MIN_SAFE_INTEGER);
const MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);

type ParsedJson =
  | null
  | boolean
  | number
  | bigint
  | string
  | ParsedJson[]
  | { [key: string]: ParsedJson };

/** Parse valid JSON while preserving integers outside JavaScript's safe range. */
export function parseLosslessJson(text: string): ParsedJson {
  let offset = 0;

  function fail(message: string): never {
    throw new SyntaxError(`${message} at JSON byte ${offset}`);
  }

  function skipWhitespace(): void {
    while (
      offset < text.length &&
      (text[offset] === " " ||
        text[offset] === "\n" ||
        text[offset] === "\r" ||
        text[offset] === "\t")
    ) {
      offset += 1;
    }
  }

  function parseString(): string {
    const start = offset;
    offset += 1;
    while (offset < text.length) {
      const character = text[offset];
      if (character === '"') {
        offset += 1;
        return JSON.parse(text.slice(start, offset)) as string;
      }
      if (character === "\\") {
        offset += 2;
      } else {
        offset += 1;
      }
    }
    return fail("unterminated JSON string");
  }

  function digits(): void {
    const start = offset;
    while (offset < text.length && text[offset] >= "0" && text[offset] <= "9") {
      offset += 1;
    }
    if (offset === start) {
      fail("expected a JSON digit");
    }
  }

  function parseNumber(): number | bigint {
    const start = offset;
    if (text[offset] === "-") {
      offset += 1;
    }
    if (text[offset] === "0") {
      offset += 1;
      if (text[offset] >= "0" && text[offset] <= "9") {
        fail("leading zero in JSON number");
      }
    } else {
      digits();
    }
    let integer = true;
    if (text[offset] === ".") {
      integer = false;
      offset += 1;
      digits();
    }
    if (text[offset] === "e" || text[offset] === "E") {
      integer = false;
      offset += 1;
      if (text[offset] === "+" || text[offset] === "-") {
        offset += 1;
      }
      digits();
    }
    const token = text.slice(start, offset);
    if (!integer) {
      return Number(token);
    }
    const value = BigInt(token);
    return value >= MIN_SAFE_INTEGER && value <= MAX_SAFE_INTEGER ? Number(value) : value;
  }

  function parseArray(): ParsedJson[] {
    offset += 1;
    skipWhitespace();
    const values: ParsedJson[] = [];
    if (text[offset] === "]") {
      offset += 1;
      return values;
    }
    while (true) {
      values.push(parseValue());
      skipWhitespace();
      if (text[offset] === "]") {
        offset += 1;
        return values;
      }
      if (text[offset] !== ",") {
        fail("expected ',' or ']' in JSON array");
      }
      offset += 1;
      skipWhitespace();
    }
  }

  function parseObject(): { [key: string]: ParsedJson } {
    offset += 1;
    skipWhitespace();
    const value: { [key: string]: ParsedJson } = Object.create(null) as {
      [key: string]: ParsedJson;
    };
    if (text[offset] === "}") {
      offset += 1;
      return value;
    }
    while (true) {
      if (text[offset] !== '"') {
        fail("expected a JSON object key");
      }
      const key = parseString();
      skipWhitespace();
      if (text[offset] !== ":") {
        fail("expected ':' after JSON object key");
      }
      offset += 1;
      value[key] = parseValue();
      skipWhitespace();
      if (text[offset] === "}") {
        offset += 1;
        return value;
      }
      if (text[offset] !== ",") {
        fail("expected ',' or '}' in JSON object");
      }
      offset += 1;
      skipWhitespace();
    }
  }

  function parseValue(): ParsedJson {
    skipWhitespace();
    const character = text[offset];
    if (character === '"') return parseString();
    if (character === "[") return parseArray();
    if (character === "{") return parseObject();
    if (text.startsWith("true", offset)) {
      offset += 4;
      return true;
    }
    if (text.startsWith("false", offset)) {
      offset += 5;
      return false;
    }
    if (text.startsWith("null", offset)) {
      offset += 4;
      return null;
    }
    if (character === "-" || (character >= "0" && character <= "9")) {
      return parseNumber();
    }
    return fail("expected a JSON value");
  }

  const value = parseValue();
  skipWhitespace();
  if (offset !== text.length) {
    fail("trailing JSON content");
  }
  return value;
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

/** Serialize HawDB parameter JSON without rounding bigint values. */
export function stringifyParameters(value: unknown): string {
  const active = new WeakSet<object>();

  function encode(item: unknown): string {
    if (item === null) return "null";
    if (typeof item === "boolean" || typeof item === "string") {
      return JSON.stringify(item);
    }
    if (typeof item === "number") {
      if (!Number.isFinite(item)) {
        throw new TypeError("HawDB parameters require finite numbers");
      }
      if (Number.isInteger(item) && !Number.isSafeInteger(item)) {
        throw new TypeError("unsafe integer HawDB parameters must use bigint");
      }
      return JSON.stringify(item);
    }
    if (typeof item === "bigint") {
      if (item < MIN_I64 || item > MAX_I64) {
        throw new RangeError("HawDB bigint parameter is outside the signed 64-bit range");
      }
      return item.toString();
    }
    if (item instanceof Uint8Array) {
      return `{"$binary":${JSON.stringify(base64(item))}}`;
    }
    if (typeof item !== "object") {
      throw new TypeError(`unsupported HawDB parameter type: ${typeof item}`);
    }
    if (active.has(item)) {
      throw new TypeError("HawDB parameters must not contain cycles");
    }
    active.add(item);
    try {
      if (Array.isArray(item)) {
        const length = item.length;
        const values: string[] = [];
        for (let index = 0; index < length; index += 1) {
          if (!Object.hasOwn(item, index)) {
            throw new TypeError("HawDB array parameters must not contain holes");
          }
          values.push(encode(item[index]));
        }
        return `[${values.join(",")}]`;
      }
      const prototype = Object.getPrototypeOf(item);
      if (prototype !== Object.prototype && prototype !== null) {
        throw new TypeError("HawDB map parameters must be plain objects");
      }
      return `{${Object.entries(item)
        .map(([key, child]) => `${JSON.stringify(key)}:${encode(child)}`)
        .join(",")}}`;
    } finally {
      active.delete(item);
    }
  }

  return encode(value);
}
