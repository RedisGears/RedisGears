---
bannerText: The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 RC3 or later. If you notice any errors, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
syntax: |
    TFUNCTION LIST [WITHCODE] [VERBOSE] [v] [LIBRARY <library name>] 
---

List the functions with additional information about each function.

## Optional arguments

<details open>
<summary><code>WITHCODE</code></summary>

Include the code in the library.
</details>

<details open>
<summary><code>VERBOSE | v</code></summary>

Increase output verbosity (can be used multiple times to increase verbosity level).
</details>

<details open>
<summary><code>LIBRARY</code></summary>

Specify a library name to show, can be used multiple times to show multiple libraries in a single command.
</details>

## Return

`TFUNCTION LIST` returns information about the requested libraries.

## Examples

{{< highlight bash >}}
TFUNCTION LIST vvv
1)  1) "engine"
    2) "js"
    3) "api_version"
    4) "1.0"
    5) "name"
    6) "lib"
    7) "pending_jobs"
    8) (integer) 0
    9) "user"
    10) "default"
    11) "functions"
    12) 1)  1) "name"
            2) "foo"
            3) "flags"
            4) (empty array)
    13) "keyspace_triggers"
    14) (empty array)
    15) "stream_triggers"
    16) (empty array)
{{</ highlight>}}

## See also

## Related topics
