--
syntax: |
    RG.FUNCTION DELETE "<library name>" 
--

Delete a library from Triggers and Functions.

## Required arguments

<details open>
<summary><code>library name</code></summar>

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
