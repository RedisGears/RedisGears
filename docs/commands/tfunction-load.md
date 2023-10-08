---
bannerText: |
  The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 or later. You can try out the triggers and functions preview with a [free Redis Cloud account](https://redis.com/try-free/?utm_source=redisio&utm_medium=referral&utm_campaign=2023-09-try_free&utm_content=cu-redis_cloud_users). The preview is available in the fixed subscription plan for the **Google Cloud Asia Pacific (Tokyo)** and **AWS Asia Pacific (Singapore)** regions.

  If you notice any errors in this documentation, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
syntax: |
    TFUNCTION LOAD [REPLACE] [CONFIG <config>] "<library code>" 
---

Load a new JavaScript library into Redis.

## Required arguments

<details open>
<summary><code>library code</code></summary>

The library code.
</details>

## Optional arguments

<details open>
<summary><code>replace</code></summary>

Instructs Redis to replace the function if it already exists.
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
