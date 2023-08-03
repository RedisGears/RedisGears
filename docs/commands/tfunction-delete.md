---
bannerText: The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 RC3 or later. If you notice any errors, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
syntax: |
    RG.FUNCTION DELETE "<library name>" 
---

Delete a library from Triggers and Functions.

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
