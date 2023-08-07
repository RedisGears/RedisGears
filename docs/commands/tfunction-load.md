---
bannerText: The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 RC3 or later. If you notice any errors, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
syntax: |
    TFUNCTION LOAD [REPLACE] [CONFIG <config>] "<library code>" 
---

Load a new library to Triggers and Functions.

## Required arguments

<details open>
<summary><code>library code</code></summary>

The library code.
</details>

## Optional arguments

<details open>
<summary><code>replace</code></summary>

Instructs Triggers and Functions to replace the function if it already exists.
</details>

<details open>
<summary><code>config</code></summary>

A string representation of a JSON object that will be provided to the library on load time, for more information refer to [library configuration](../docs/concepts/Library_Configuration.md).
</details>

## Return

TFUNCTION LOAD returns either

* ["OK"](/docs/reference/protocol-spec/#resp-simple-strings) when the library was loaded correctly.
* [Error reply](/docs/reference/protocol-spec/#resp-errors) when the library could not be loaded.

## Examples

{{< highlight bash >}}
TFUNCTION LOAD "#!js api_version=1.0 name=lib\n redis.registerFunction('hello', ()=>{return 'Hello world'})"
1) "OK"
{{</ highlight>}}

## See also

## Related topics
