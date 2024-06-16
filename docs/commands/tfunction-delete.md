---
bannerText: |
  Triggers and functions preview is no longer under active development. Triggers and functions feature preview has ended and it will not be promoted to GA.
syntax: |
    TFUNCTION DELETE "<library name>" 
---

Delete a JavaScript library from Redis.

## Required arguments

<details open>
<summary><code>library name</code></summary>

The name of the library to delete.
</details>

## Return

`TFUNCTION DELETE` returns either

* ["OK"](/docs/reference/protocol-spec/#resp-simple-strings) when the library was deleted correctly.
* [Error reply](/docs/reference/protocol-spec/#resp-errors) when the library could not be deleted.

## Examples

{{< highlight bash >}}
TFUNCTION DELete lib
1) "OK"
{{</ highlight>}}

## See also

## Related topics
