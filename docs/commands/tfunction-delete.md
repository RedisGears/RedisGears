---
bannerText: |
  The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 or later. You can try out the triggers and functions preview with a [free Redis Cloud account](https://redis.com/try-free/?utm_source=redisio&utm_medium=referral&utm_campaign=2023-09-try_free&utm_content=cu-redis_cloud_users). The preview is available in the fixed subscription plan for the **Google Cloud Asia Pacific (Tokyo)** and **AWS Asia Pacific (Singapore)** regions.

  If you notice any errors in this documentation, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
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
