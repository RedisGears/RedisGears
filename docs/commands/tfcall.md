---
bannerText: |
  The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 or later. You can try out the triggers and functions preview with a [free Redis Cloud account](https://redis.com/try-free/?utm_source=redisio&utm_medium=referral&utm_campaign=2023-09-try_free&utm_content=cu-redis_cloud_users). The preview is available in the fixed subscription plan for the **Google Cloud Asia Pacific (Tokyo)** and **AWS Asia Pacific (Singapore)** regions.

  If you notice any errors in this documentation, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
syntax: |
    TFCALL <library name>.<function name> <number of keys> [<key1> ... <keyn>] [<arg1> ... <argn>]
---

Invoke a function.

## Required arguments

<details open>
<summary><code>library name</code></summary>

The name of the JavaScript library that contains the function.
</details>

<details open>
<summary><code>function name</code></summary>

The function name to run.
</details>

<details open>
<summary><code>number of keys</code></summary>

The number keys that will follow.
</details>

<details open>
<summary><code>keys</code></summary>

The keys that will be touched by the function.
</details>

<details open>
<summary><code>arguments</code></summary>

The arguments passed to the function.
</details>

## Return

`TFCALL` returns either

* The return value of the function.
* [Error reply](/docs/reference/protocol-spec/#resp-errors) when the function execution failed.

## Examples

{{< highlight bash >}}
TFCALL lib.hello 0
"Hello World"
{{</ highlight>}}

## See also

## Related topics
