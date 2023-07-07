---
title: "RESP & JavaScript"
linkTitle: "RESP & JavaScript"
weight: 7
description: >
    Converting RESP to and from JavaScript
---

When running Redis commands from within a function using the `client.call` API, the reply is parsed as a resp3 reply and converted to a JS object using the following rules:

| resp 3            | JS object type                                                                                                                                 |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `status`          | `StringObject` with a field called `__reply_type` and value `status` (or error if failed to convert to utf8)                                   |
| `bulk string`     | JS `String` (or error if failed to convert to utf8)                                                                                            |
| `Error`           | Raise JS exception                                                                                                                             |
| `long`            | JS big integer                                                                                                                                 |
| `double`          | JS number                                                                                                                                      |
| `array`           | JS array                                                                                                                                       |
| `map`             | JS object                                                                                                                                      |
| `set`             | JS set                                                                                                                                         |
| `bool`            | JS boolean                                                                                                                                     |
| `big number`      | `StringObject` with a field called `__reply_type` and value `big_number`                                                                       |
| `verbatim string` | `StringObject` with 2 additional fields: 1. `__reply_type` and value `verbatim` 2. `__format` with the value of the ext in the verbatim string (or error if failed to convert to utf8) |
| `null`            | JS null                                                                                                                                        |
|                   |                                                                                                                                                |

When running Redis commands from within a function using the `client.callRaw` API, the reply is parsed as a resp3 reply and converted to a JS object using the following rules:

| resp 3            | JS object type                                                                                                                                 |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `status`          | JS `ArrayBuffer` with a field called `__reply_type` and value `status`                                                                        |
| `bulk string`     | JS `ArrayBuffer`                                                                                                                              |
| `Error`           | Raise JS exception                                                                                                                             |
| `long`            | JS big integer                                                                                                                                 |
| `double`          | JS number                                                                                                                                      |
| `array`           | JS array                                                                                                                                       |
| `map`             | JS object                                                                                                                                      |
| `set`             | JS set                                                                                                                                         |
| `bool`            | JS boolean                                                                                                                                     |
| `big number`      | `StringObject` with a field called `__reply_type` and value `big_number`                                                                       |
| `verbatim string` | JS `ArrayBuffer` with 2 additional fields: 1. `__reply_type` and value `verbatim` 2. `__format` with the value of the ext in the verbatim string |
| `null`            | JS null                                                                                                                                        |
|                   |                                                                                                                                                |

## JS -> RESP Conversion

| JS type                                                          | RESP2         | RESP3                                  |
|------------------------------------------------------------------|---------------|----------------------------------------|
| `string`                                                         | `bulk string` | `bulk string`                          |
| `string` object with field `__reply_type=status`                 | `status`      | `status`                               |
| Exception                                                        | `error`       | `error`                                |
| `big integer`                                                    | `long`        | `long`                                 |
| `number`                                                         | `bulk string` | `double`                               |
| `array`                                                          | `array`       | `array`                                |
| `map`                                                            | `array`       | `map`                                  |
| `set`                                                            | `array`       | `set`                                  |
| `bool`                                                           | `long`        | `bool`                                 |
| `string` object with field`__reply_type=varbatim` and `__format=txt` | `bulk string` | `verbatim string` with format as `txt` |
| `null`                                                           | resp2 `null`  | resp3 `null`                           |
