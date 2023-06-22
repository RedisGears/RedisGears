--
syntax: |
    TFCALL <library name>.<function name> <number of keys> [<key1> ... <keyn>] [<arg1> ... <argn>]
--

Invoke a function.

## Required arguments

<details open>
<summary><code>library name</code></summar>

The name of the library that contains the function.
</details>

<details open>
<summary><code>function name</code></summar>

The function name to run.
</details>

<details open>
<summary><code>number of keys</code></summar>

The number keys that will follow.
</details>

<details open>
<summary><code>keys</code></summary>

The keys that will be touched by the function
</details>

<details open>
<summary><code>arguments</code></summary>

The arguments passed to the function.
</details>

## Return

`TFCALL` returns either

* The return value of the function.
* [Error reply](/docs/reference/protocol-spec/#resp-errors) when the fucntion execution failed.

## Examples

{{< highlight bash >}}
TFCALL lib.hello 0
"Hello World"
{{</ highlight>}}

## See also

## Related topics