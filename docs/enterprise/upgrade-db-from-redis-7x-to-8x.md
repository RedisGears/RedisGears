# Upgrading a Redis Enterprise database with RedisGears from Redis 7.x to Redis 8.x

**Audience:** Redis Enterprise operators, Support and Solution Architects handling customers that
run the RedisGears (`rg`) module.

**Question this answers:** *Can a Redis Enterprise database that has RedisGears 1.2.x loaded and
runs Redis 7.x be upgraded in place to Redis 8.x, and if so, what is the exact procedure?*

---

## 1. Short answer

**Yes — for Redis 8.0, 8.2 and 8.4.** The database is upgraded in place with the regular
`rladmin upgrade db` / `POST /v1/bdbs/<uid>/upgrade` flow. There is **no** need to create a new
database, export/import the data, or drop the module.

The one mandatory extra step is that a **RedisGears package built for the exact target Redis
`major.minor`** must be uploaded to the cluster *before* the database upgrade, because Redis
Enterprise matches a module to a Redis feature set exactly (see [§3](#3-why-a-new-package-is-required)).

| Target Redis version | RedisGears package to upload | Status |
|---|---|---|
| 8.0 | `8.0.0` | Available |
| 8.2 | `8.2.0` | Available |
| 8.4 | `8.4.0` | Available |
| 8.6 | — | **Not released yet.** Upgrade is not possible today; a `8.6.0` build must be produced first. |
| 8.8 and later | — | Not released yet. |

Two important qualifications:

* This document covers the **`rg`** module (RedisGears 1.2.x, the Python/JVM execution
  framework). It does **not** cover **`redisgears_2`** ("Triggers and functions", RedisGears 2.x) —
  that module is deprecated and *blocks* the Redis Enterprise software upgrade entirely. See
  [§2](#2-first-establish-which-gears-module-the-customer-actually-has).
* The RedisGears `8.0.0` / `8.2.0` / `8.4.0` releases are **the same code as `1.2.13`**, republished
  with new version numbers and new Redis-compatibility metadata. They are not a new feature line —
  see [§4](#4-what-the-8xx-releases-actually-are).

---

## 2. First, establish which Gears module the customer actually has

The two modules look similar in a ticket but behave completely differently on upgrade.

| | RedisGears 1.2.x | Triggers and functions |
|---|---|---|
| Module name in the cluster | `rg` | `redisgears_2` |
| Semantic versions | `1.0.x` … `1.2.13`, and `8.0.0` / `8.2.0` / `8.4.0` | `2.0.x` |
| API | `RG.PYEXECUTE`, `RG.TRIGGER`, … | `TFCALL`, `TFUNCTION LOAD`, … |
| Runtime | Embedded CPython 3.7 (+ optional JVM) | Embedded V8 / JavaScript |
| Can the DB be upgraded to Redis 8.x? | **Yes**, with this procedure | **No** |
| Effect on Redis Enterprise software upgrade | None — it is not a deprecated module | **Blocks the cluster upgrade.** Must be removed from every database first |

`redisgears_2` (together with `graph`) is listed in `DEPRECATED_MODULES`
(`cnm/cnm/utils/module_utils.py:37` in the Redis Enterprise source). On clusters that predate the
`module_management` capability the pre-upgrade check aborts the software upgrade with:

```
Upgrade aborted. The following deprecated modules cannot be used in the cluster: redisgears_2.
Please remove them from all databases and then rerun the upgrade process.
```

and on upgraded nodes the module files and CCS objects for those modules are deleted outright.
The internal Confluence page *"Resolving Graph/Gears compatibility issues during upgrade"* — which
says a database with Gears cannot be upgraded and must be migrated — describes **this**
(`redisgears_2` / `graph`) situation. It does **not** apply to `rg`.

Check what you are dealing with:

```bash
# Cluster-wide catalogue + which module each database uses,
# including min/compatible Redis version columns
rladmin status modules extra all
```

```bash
# Per-database view
curl -s -k -u "<user>:<password>" \
  "https://<cluster-fqdn>:9443/v1/bdbs/<uid>?fields=uid,name,redis_version,module_list" | jq
```

If `module_name` is `redisgears_2`, stop — this runbook does not apply.

---

## 3. Why a new package is required

Redis Enterprise decides whether a module may be used with a given Redis version in
`is_redis_version_compatible()` (`cnm/cnm/utils/module_utils.py:823`):

* If the module declares **`compatible_redis_version`**, the match must be **exact** on
  `major.minor`. A module marked `8.2` can only be attached to a Redis 8.2 database — not 8.0, not 8.4.
* If the module does **not** declare `compatible_redis_version`, the fallback rule uses
  `min_redis_version` and additionally requires the **same major version**.

Every RedisGears release up to and including `1.2.13` ships `min_redis_version: '6.0.0'` and no
`compatible_redis_version`. Under the fallback rule, major `6` ≠ major `8`, so **no 1.2.x package can
ever be selected for a Redis 8.x database**, regardless of the numeric comparison.

That is exactly what the `8.x` re-releases fix. `ramp.yml` at tag `v8.4.0`:

```yaml
compatible_redis_version: "8.4"
min_redis_version: "8.4"
min_redis_pack_version: '6.0.12'
```

Consequences to keep in mind:

* You need **one package per target Redis feature set**. If the estate has both Redis 8.2 and
  Redis 8.4 databases with Gears, upload both `8.2.0` and `8.4.0`.
* When the upgrade request does not name a module explicitly, Redis Enterprise auto-selects the
  highest-semantic-version *allowed* package for that module
  (`get_latest_allowed_module()`, `cnm/cnm/utils/module_utils.py:803`). Because the exact-match rule
  filters first, having 8.0.0, 8.2.0 and 8.4.0 all uploaded is unambiguous and safe.
* If no matching package exists, the upgrade is rejected with a `400` (see
  [§9](#9-troubleshooting)).

---

## 4. What the 8.x.x releases actually are

RedisGears `8.0.0`, `8.2.0` and `8.4.0` are all tagged off the **same commit as `v1.2.13`**
(`4daa280`), on the `8.4` branch. The full diff between `v1.2.13` and `v8.4.0` is:

```
 .github/workflows/flow-gears.yaml | 12 +++++++++---
 Dockerfile.focal                  |  2 +-
 Dockerfile.jammy                  |  2 +-
 Dockerfile.rhel8                  |  2 +-
 Dockerfile.rhel9                  |  2 +-
 build/cpython/system-setup.py     |  2 +-
 ramp.yml                          |  3 ++-
 src/version.h                     |  6 +++---
 system-setup.py                   |  2 +-
```

i.e. version constants, the RAMP compatibility metadata, and build-tooling pins (`setuptools<81`).
**No engine, data-format or API change.** In particular:

* `REDISGEARS_DATATYPE_VERSION` stays `4` and `REDISGEARS_DATATYPE_NAME` stays `GEARS_DT0`, so RDB
  and replication payloads written by 1.2.x are read by 8.x without conversion.
* The embedded interpreter is **CPython 3.7** in both `1.2.7` and `1.2.13`/`8.x`
  (`build/cpython/Makefile`), so Python requirement wheels stored in the RDB stay ABI-compatible
  across the upgrade.
* `REDISGEARS_MODULE_NAME` stays `rg` and the on-the-wire commands are unchanged.

What *is* different from `1.2.7` is everything that landed in `1.2.8`–`1.2.13`, which the customer
gets as a side effect. The notable ones:

* `1.2.12` — fix for duplicate registration IDs that could corrupt the RDB; the embedded virtualenv
  is recreated on startup; stale not-yet-started executions are cleared.
* `1.2.10` — cross-slot violation fix relevant to Replica Of and Active-Active.
* `1.2.13` — Ubuntu 22.04 support added; **Ubuntu 16.04/18.04 and RHEL7 support dropped**;
  `redis-py 5.0` bundled in the embedded Python.

Treat the module change as a real (if small) version jump, not a no-op: it is `1.2.7 → 1.2.13`
behaviour with an `8.x` label.

---

## 5. Artifacts

Base URL: `https://redismodules.s3.amazonaws.com/redisgears/`

Naming: `redisgears.Linux-<osnick>-<arch>.<semver>-withdeps.zip`

### 5.1 Use the `-withdeps` package. Always.

Two zips are published per platform/version:

| File | Size (8.4.0, rhel9) | Contents | Usable in Redis Enterprise? |
|---|---|---|---|
| `redisgears.Linux-rhel9-x86_64.8.4.0.zip` | ~0.5 MB | `redisgears.so` + `module.json` only | **No** |
| `redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip` | ~246 MB | the above **plus** `deps/` with the embedded Python and JVM tarballs | **Yes** |

The `module.json` declares its dependencies (`gears_python`, `gears_jvm`) by URL, but Redis
Enterprise does not fetch them — it reads them out of the uploaded zip
(`_write_module_to_disk()` → `_get_dependency_file()`, `cnm/cnm/utils/module_utils.py:566`) and
fails the upload with `Missing dependency <file>` if they are absent. The `-withdeps` variant is
produced by `append_deps.py` in the RedisGears build precisely for this.

246 MB is comfortably inside the Redis Enterprise 500 MB module limit
(`MAX_MODULE_SIZE_BYTES`, `cnm/cnm/utils/module_utils.py:38`).

### 5.2 Do **not** use the `redisgears_python.*` variant for this upgrade

There is a smaller Python-only variant (`redisgears_python.Linux-rhel9-x86_64.8.4.0-withdeps.zip`,
~62 MB, no JVM). **It is mispackaged in the 8.x releases**: `ramp_python.yml` was never given the new
compatibility attributes, so the published `module.json` still reads

```
min_redis_version = 6.0.0
compatible_redis_version = 7.4
```

Redis Enterprise will therefore only ever allow it on a **Redis 7.4** database. Uploading it and
then trying to upgrade a database to 8.x fails with the "unable to find a version of the module rg"
error. This is a packaging bug in the `8.4` branch and needs fixing before the 8.6 build
(`ramp_python.yml` must get the same `compatible_redis_version` / `min_redis_version` treatment that
`ramp.yml` received in `MOD-13816` / `MOD-13790` / `MOD-13818`).

### 5.3 Platform coverage

The `8.x` line is built for **x86_64 only**, on four OS targets
(`.github/workflows/flow-gears.yaml` matrix):

| Redis Enterprise node OS | `<osnick>` in the filename | `operating_systems` in `module.json` |
|---|---|---|
| RHEL / Rocky / Oracle Linux 8 | `rhel8` | `rhel8` |
| RHEL / Rocky / Oracle Linux 9 | `rhel9` | `rhel9` |
| Ubuntu 20.04 | `ubuntu20.04` | `ubuntu20` |
| Ubuntu 22.04 | `ubuntu22.04` | `ubuntu22` |

There is **no aarch64/arm64 build** and no RHEL7 / Ubuntu 18.04 build. On an ARM cluster this
upgrade path is not available.

The package OS must match the node OS: Redis Enterprise rejects the upload otherwise
(`_verify_module_os_compatibility()`, `cnm/cnm/utils/module_utils.py:412`):

```
Module 'rg' is not compatible with this node's OS 'rhel9'. Supported operating systems: ['ubuntu22']
```

### 5.4 Verified metadata (8.4.0, rhel9)

```
module_name              = rg
version                  = 80400
semantic_version         = 8.4.0
min_redis_version        = 8.4
compatible_redis_version = 8.4
min_redis_pack_version   = 6.0.12
operating_systems        = ['rhel9']
architecture             = x86_64
command_line_args        = Plugin gears_python CreateVenv 1
config_command           = RG.CONFIGSET
git_sha                  = 986be1891f5008b0be8fdb083c9c1472df098b87   # == tag v8.4.0
capabilities             = types, crdb, failover_migrate, persistence_aof, persistence_rdb,
                           clustering, backup_restore, reshard_rebalance, eviction_expiry,
                           intershard_tls, intershard_tls_pass, ipv6
```

---

## 6. Prerequisites and hard limits

Check these **before** promising the customer an upgrade date.

1. **Redis Enterprise software must already offer the target Redis version.** The database upgrade
   can only target a version every node supports:

   ```bash
   curl -s -k -u "<user>:<password>" "https://<cluster-fqdn>:9443/v1/nodes" \
     | jq '.[] | {uid, os_version, architecture, supported_database_versions}'
   ```

   If Redis 8.4 is not listed, the *cluster software* must be upgraded first (§7 step 2).

2. **Before the cluster software upgrade, every database must be on a Redis version the new software
   still supports.** The `check-versions-match` pre-upgrade check aborts otherwise:

   > Prior to upgrading Redis Enterprise Software, all databases must be upgraded to the latest
   > supported version of redis.

   In practice this means: bring the Gears database up to the newest Redis 7.x feature set the
   *current* software supports, then upgrade the software, then upgrade the database to 8.x.

3. **Minimum Redis Enterprise version** for the module: `6.0.12` (`min_redis_pack_version`).

4. **`allow_modules_upload` must be enabled** on the cluster, otherwise every upload endpoint
   returns `403 modules_upload_disabled`. `rlutil upload_module` turns it on for you.

5. **Database features the Gears module does not support.** Redis Enterprise validates the database
   configuration against the intersection of its modules' capabilities
   (`validate_modules_capabilities_for_bdb()`, `cnm/cnm/http_services/cluster_api/common.py:1133`).
   The RedisGears `8.x` package does **not** declare:

   | Missing capability | Blocked database configuration |
   |---|---|
   | `flash` | Auto Tiering / RoF (`bigstore`) |
   | `bigstore_version_2` | Auto Tiering v2 |
   | `replica_of` | Replica Of (`replica_sync`) |
   | `hash_policy` | custom `shard_key_regex` (non-OSS hash policy) |
   | `asm` | auto shard-management scaling |

   A database using any of these plus Gears will fail the upgrade with
   `406 unsupported_module_capabilities`. This is not new in 8.x — the same capability list ships in
   `1.2.13` — but the upgrade is where it surfaces.

6. **Active-Active (CRDB).** The package declares the `crdb` *capability*, so an A-A database with
   Gears passes validation, but its `crdb` metadata block is empty — meaning `rg` is **not** a
   CRDT-*managed* module (`module_is_crdt_managed()`,
   `cnm/cnm/http_services/cluster_api/common.py:719`) and has no CRDT feature-set versions. The
   per-instance CRDT module feature-set negotiation therefore does not apply to it. The matching
   package must be uploaded to **every participating cluster** before any instance is upgraded, and
   the `check-crdt-missing-old-modules` pre-upgrade check will flag any module referenced by the CRDB
   configuration that is missing from a cluster. Validate A-A + Gears upgrades with R&D before
   running them at a customer.

7. **CPython 3.7 is end-of-life.** The 8.x packages carry it unchanged. Customers should be told
   that this path keeps them running, it does not modernise them; the strategic direction is to move
   off RedisGears.

8. **Every node needs the artifact — including nodes added later.** When a node joins, Redis
   Enterprise checks that the node has, for each custom module in use, a build for every Redis
   version in use, and fails the join with `incompatible_modules` otherwise
   (`validate_custom_module_compatibility()`,
   `cnm/cnm/services/bootstrap_mgr/bootstrap_utils.py:181`). See [§10](#10-automating-the-module-install-on-node-bootstrap).

---

## 7. Procedure

Throughout: `CLUSTER=https://<cluster-fqdn>:9443`, `AUTH="-u <user>:<password>"`, and the target is
Redis **8.4** with RedisGears **8.4.0** on **RHEL 9**. Substitute as needed.

### Step 0 — Inventory and baseline

```bash
rladmin status modules extra all
rladmin status databases
```

```bash
# Current module version, args and Redis version of the database
curl -s -k $AUTH "$CLUSTER/v1/bdbs/<uid>?fields=uid,name,redis_version,module_list,bigstore,replica_sync,shard_key_regex,crdt" | jq
```

Capture the Gears state from the database itself — you will compare against it after the upgrade:

```bash
redis-cli -h <endpoint> -p <port> -a <pass> RG.DUMPREGISTRATIONS
redis-cli -h <endpoint> -p <port> -a <pass> RG.PYDUMPREQS
redis-cli -h <endpoint> -p <port> -a <pass> RG.PYSTATS
redis-cli -h <endpoint> -p <port> -a <pass> RG.INFOCLUSTER
redis-cli -h <endpoint> -p <port> -a <pass> MODULE LIST
```

Take a backup / export of the database. This is a shard-restarting operation.

### Step 1 — Bring the database to the newest supported Redis 7.x

Only if step 2's pre-upgrade check requires it (see §6.2):

```bash
rladmin upgrade db db:<uid> redis_version 7.4
```

### Step 2 — Upgrade the Redis Enterprise cluster software

Standard node-by-node software upgrade to a version that supports Redis 8.4. Nothing
Gears-specific here; `rg` is not a deprecated module and does not block it.

Confirm afterwards:

```bash
rladmin status nodes
curl -s -k $AUTH "$CLUSTER/v1/nodes" | jq '.[] | {uid, supported_database_versions}'
```

All nodes must report the same software version — the database upgrade refuses to run otherwise
("not all nodes upgraded to the same version").

### Step 3 — Upload the matching RedisGears package

Download on a node (or upload from your workstation):

```bash
curl -fLO https://redismodules.s3.amazonaws.com/redisgears/redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip
```

Then, **on the cluster master node**:

```bash
/opt/redislabs/bin/rlutil upload_module path=/tmp/redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip
```

`rlutil upload_module` does three things (`cnm/cnm/cli/rlutil.py:2123`):

1. `PUT /v1/cluster {"allow_modules_upload": true}`
2. `POST /v2/modules/user-defined` with the `module.json` from the zip — creates the module object
   in the CCS
3. `POST /v2/local/modules/user-defined/artifacts` on **every** node — copies the binary and
   extracts the dependencies into `$modulesdatadir/rg/80400/`

Verify it landed everywhere (use the **numeric** version):

```bash
/opt/redislabs/bin/rlutil check_module name=rg version=80400
# -> module rg version 80400 is ready to be used
```

```bash
rladmin status modules extra all   # rg 8.4.0 should now appear with compatible_redis_version 8.4
```

<details>
<summary>REST equivalent (and what to do on clusters without <code>rlutil upload_module</code>)</summary>

```bash
curl -k $AUTH -X PUT "$CLUSTER/v1/cluster" \
  -H 'Content-Type: application/json' -d '{"allow_modules_upload": true}'

unzip -p redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip module.json > module.json
curl -k $AUTH -X POST "$CLUSTER/v2/modules/user-defined" \
  -H 'Content-Type: application/json' --data-binary @module.json

# then, on each node, against its local endpoint:
curl -k $AUTH -X POST "https://127.0.0.1:9443/v2/local/modules/user-defined/artifacts" \
  -F "module=@redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip"
```

On Redis Enterprise versions that predate the `module_management` capability (roughly pre-7.24) the
older single-shot `POST /v1/modules` multipart upload is used instead. Prefer `rlutil upload_module`
where it exists — it handles the per-node fan-out for you.
</details>

### Step 4 — Dry-run the database upgrade

The upgrade API validates everything and returns the resulting database object without touching the
shards:

```bash
curl -s -k $AUTH -X POST "$CLUSTER/v1/bdbs/<uid>/upgrade?dry_run=true" \
  -H 'Content-Type: application/json' \
  -d '{"redis_version": "8.4"}' | jq '{redis_version, module_list}'
```

Read the returned `module_list` carefully. Besides `rg` moving to `8.4.0`, expect Redis Enterprise
to add the bundled Redis 8 modules automatically — `search`, `timeseries`, `bf`, `ReJSON`
(`_add_bundled_modules_on_upgrade()`,
`cnm/cnm/http_services/cluster_api/bdb_handler.py:1897`; skipped for CRDT databases) — and to
replace `searchlight` with `search` if present. That is expected Redis 8 behaviour, not a
Gears-specific side effect, but tell the customer it is coming.

If you want to pin the Gears version explicitly rather than let the cluster pick it:

```bash
# module_id of the currently loaded rg, and uid of the newly uploaded rg 8.4.0
CUR=$(curl -s -k $AUTH "$CLUSTER/v1/bdbs/<uid>" | jq -r '.module_list[] | select(.module_name=="rg") | .module_id')
NEW=$(curl -s -k $AUTH "$CLUSTER/v1/modules" | jq -r '.[] | select(.module_name=="rg" and .semantic_version=="8.4.0") | .uid')

curl -s -k $AUTH -X POST "$CLUSTER/v1/bdbs/<uid>/upgrade?dry_run=true" \
  -H 'Content-Type: application/json' \
  -d "{\"redis_version\":\"8.4\",\"modules\":[{\"current_module\":\"$CUR\",\"new_module\":\"$NEW\",\"new_module_args\":\"\"}]}" | jq
```

### Step 5 — Run the upgrade

CLI (recommended — this is the "exact command" to hand to the customer):

```bash
# let the cluster select the matching rg build
rladmin upgrade db db:<uid> redis_version 8.4 preserve_roles

# or name the module explicitly, keeping its existing module args
rladmin upgrade db db:<uid> redis_version 8.4 \
  and module module_name rg version 8.4.0 module_args "keep_args" \
  preserve_roles
```

`version` accepts either the semantic (`8.4.0`) or numeric (`80400`) form. `module_args "keep_args"`
copies the currently configured args verbatim; for a stock Redis Enterprise deployment those are
`Plugin gears_python CreateVenv 1`, taken from the package's `command_line_args`.

Useful extra flags: `parallel_shards_upgrade <n>` to throttle the rolling restart, `preserve_roles`
to restore master/replica placement afterwards (costs one extra failover).

REST:

```bash
curl -s -k $AUTH -X POST "$CLUSTER/v1/bdbs/<uid>/upgrade" \
  -H 'Content-Type: application/json' \
  -d '{"redis_version": "8.4", "preserve_roles": true}' | jq '.action_uid'
```

Track it:

```bash
curl -s -k $AUTH "$CLUSTER/v1/actions/<action_uid>" | jq
rladmin status databases
```

Note on module args: the "module args v2" conversion that Redis Enterprise performs when crossing
from Redis 7.x to 8.x only applies to the bundled `search` / `timeseries` / `probabilistic` modules
(`to_v2_module_name()`, `cnm/cnm/ccs/objects/bdb.py:76`). The `rg` args are carried over unchanged.

### Step 6 — Verify

```bash
rladmin status databases
rladmin status modules extra all
```

```bash
redis-cli ... MODULE LIST              # rg should report version 80400
redis-cli ... RG.DUMPREGISTRATIONS     # compare against the Step 0 output
redis-cli ... RG.PYDUMPREQS            # requirements: IsDownloaded / IsInstalled == yes
redis-cli ... RG.PYSTATS
redis-cli ... RG.INFOCLUSTER
```

What to watch for specifically:

* **Registrations** must all come back. They live in the RDB and are restored on load.
* **Python requirements** are stored *as wheels inside the RDB* and reinstalled offline
  (`pip install --no-index`) on shard start, so no PyPI access is needed. Because the embedded
  interpreter is still CPython 3.7, the wheels remain valid.
* **The embedded virtualenv is deleted and rebuilt on every shard start** when `CreateVenv 1` is set
  — it lives at `$modulesdatadir/rg/.venv-<shard-uid>` (`plugins/python/redisgears_python.c:6871`).
  Expect the first start after the upgrade to be slower, and check the shard logs for
  `Failed to construct virtualenv` if a shard does not come up.
* Any requirement whose wheel contains compiled extensions built on an older OS (e.g. the customer
  came from RHEL7 or Ubuntu 18) is reinstalled as-is; the serialized OS tag is not validated. If such
  a requirement fails to import, reinstall it on the new platform with
  `RG.PYEXECUTE ... REQUIREMENTS <pkg>`.

### Step 7 — Clean up (optional)

Once the database is stable on 8.4 and you are past the rollback window, the old `rg` package can be
removed. It cannot be deleted while any database still references it
(`406 module_in_use`).

```bash
curl -k $AUTH -X DELETE "$CLUSTER/v2/modules/user-defined/<old-module-uid>"
# then on each node:
curl -k $AUTH -X DELETE "https://127.0.0.1:9443/v2/local/modules/user-defined/artifacts/rg/<old-numeric-version>"
```

Keep the old package if you may still need to roll back (see §8).

---

## 8. Rollback

There is **no supported in-place downgrade**. Redis Enterprise rejects any upgrade request whose
target module semantic version is lower than the current one
(`406 module_downgrade_unsupported`, `verify_module_upgrade_is_possible()`,
`cnm/cnm/http_services/cluster_api/common.py:760`), and a Redis version lower than the database's
current version is rejected too.

Rollback therefore means **restore from the backup taken in Step 0** into a database created with
the previous Redis version and the previous `rg` package. Plan the maintenance window accordingly,
and do not delete the old `rg` package until you have signed off.

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `400 upgrade_bdb_failed` — *"Unable to find a version of the module rg that is supported by target redis version of BDB 8.4"* | No `rg` package with `compatible_redis_version: 8.4` in the cluster | Upload `redisgears.Linux-<os>-x86_64.8.4.0-withdeps.zip` (§7 step 3). If you already uploaded something, check you did not use the `redisgears_python.*` variant (§5.2) |
| Same error, but the target is 8.6 or 8.8 | No RedisGears build exists for that feature set | Not possible today. Upgrade to 8.4 instead, or wait for an `8.6.0` build |
| Upload fails: `Missing dependency redisgears-python.Linux-...tgz` | The plain zip was uploaded instead of `-withdeps` | Re-upload the `-withdeps` package |
| Upload fails: `Module 'rg' is not compatible with this node's OS 'rhel9'. Supported operating systems: ['ubuntu22']` | Wrong OS build | Download the package for the node OS (§5.3) |
| `403` on the upload endpoints | `allow_modules_upload` disabled | `PUT /v1/cluster {"allow_modules_upload": true}`, or just use `rlutil upload_module` |
| `406 unsupported_module_capabilities` | The database uses Auto Tiering, Replica Of, a custom hash policy or ASM scaling — none of which the Gears package declares | Reconfigure the database, or keep it on 7.x. See §6.5 |
| `400` — *"not all nodes upgraded to the same version"* | Mixed software versions mid-cluster-upgrade | Finish the software upgrade on all nodes first |
| `400` — *"Target version 8.4 is not higher (or equal) to the current version …"* | Downgrade attempt | Not supported (§8) |
| `400 redis_upgrade_policy_mismatch` | The cluster's `redis_upgrade_policy` is `major` and a non-major version was requested | Request a major version, or change the policy |
| `406 module_downgrade_unsupported` | The named `new_module` has a lower semantic version than the loaded one | Name the correct package |
| Node join fails with `incompatible_modules` | The joining node lacks an `rg` build for a Redis version in use | Upload the artifact to that node, or use `user_defined_modules` in the bootstrap config (§10) |
| Shard does not come up after the upgrade; log shows `Failed to construct virtualenv` | The rebuilt embedded virtualenv failed | Check disk space and permissions under `$modulesdatadir/rg/`, and that the deps were extracted into `$modulesdatadir/rg/80400/deps/` |
| Registrations present but a function fails on `import` | A requirement wheel was built for the customer's old OS | Reinstall the requirement via `RG.PYEXECUTE ... REQUIREMENTS <pkg>` |

---

## 10. Automating the module install on node bootstrap

Custom modules are *not* shipped with the Redis Enterprise installer — only `search`,
`timeseries`, `bf` and `ReJSON` are bundled (`BUNDLED_MODULES`,
`cnm/cnm/utils/module_utils.py:33`). That is why Gears has to be uploaded by hand, and it is the
answer to *"can Gears come with the cluster like the other modules?"* — today, no.

What *is* available is a bootstrap hook: the cluster v2 bootstrap configuration accepts a
`user_defined_modules` list, and **each node downloads and installs those modules itself** during
bootstrap (`handle_user_defined_modules()`,
`cnm/cnm/services/bootstrap_mgr/bootstrap_utils.py:3416`):

```json
{
  "bootstrap": {
    "user_defined_modules": [
      {
        "name": "redisgears-8.4.0",
        "location": {
          "location_type": "https",
          "url": "https://redismodules.s3.amazonaws.com/redisgears/redisgears.Linux-rhel9-x86_64.8.4.0-withdeps.zip"
        }
      }
    ]
  }
}
```

`location.credentials` (`username` / `password`) is supported for private artifact stores. A download
failure is recorded as a bootstrap *warning*, not a failure — but the subsequent
`validate_custom_module_compatibility()` check will fail the join if the module is genuinely needed
by an existing database, so it does not silently drift.

This is the recommended way to keep new nodes consistent in a cluster that runs Gears.

---

## 11. Reference

RedisGears:

* Tags: [`v8.0.0`](https://github.com/RedisGears/RedisGears/releases/tag/v8.0.0),
  [`v8.2.0`](https://github.com/RedisGears/RedisGears/releases/tag/v8.2.0),
  [`v8.4.0`](https://github.com/RedisGears/RedisGears/releases/tag/v8.4.0) — all on branch `8.4`,
  all rooted at the `v1.2.13` commit `4daa280`
* `ramp.yml` — RAMP manifest carrying `compatible_redis_version`
* `ramp_python.yml` — Python-only manifest, **not** updated for 8.x (see §5.2)
* `src/version.h` — `REDISGEARS_VERSION_*`, `REDISGEARS_MODULE_NAME`, `REDISGEARS_DATATYPE_VERSION`
* `append_deps.py` — produces the `-withdeps` packages
* `.github/workflows/flow-gears.yaml` — build matrix (focal, jammy, rhel8, rhel9; x86 only)
* `plugins/python/redisgears_python.c` — virtualenv lifecycle, requirement wheel (de)serialization

Redis Enterprise (`cnm/`):

* `cnm/utils/module_utils.py` — `is_redis_version_compatible`, `get_latest_allowed_module`,
  `BUNDLED_MODULES`, `DEPRECATED_MODULES`, `verify_module_before_upload`, `write_module_to_disk`
* `cnm/http_services/cluster_api/bdb_handler.py` — `POST /v1/bdbs/<uid>/upgrade`,
  `_add_bundled_modules_on_upgrade`, `_replace_searchlight_with_search_on_upgrade`,
  `fill_modules_to_upgrade`
* `cnm/http_services/cluster_api/module_handler.py` — `/v2/modules/user-defined`,
  `/v2/local/modules/user-defined/artifacts`
* `cnm/http_services/cluster_api/common.py` — `verify_module_upgrade_is_possible`,
  `validate_modules_for_bdb`, `validate_modules_capabilities_for_bdb`
* `cnm/statemachine/upgrade.py` — upgrade logic, target version selection, module-args v2 conversion
* `cnm/cli/rlutil.py` — `upload_module`, `check_module`
* `cnm/cli/rladmin/` — `rladmin upgrade db`, `rladmin status modules`
* `cnm/upgrade_checks.py` — pre-software-upgrade checks (`check-versions-match`,
  `check-no-deprecated-modules`, `check-crdt-missing-old-modules`)
* `cnm/services/bootstrap_mgr/bootstrap_utils.py` — `user_defined_modules`,
  `validate_custom_module_compatibility`

---

## 12. Open items

1. **RedisGears 8.6.0 does not exist.** It must be built and published (a version bump plus
   `compatible_redis_version: "8.6"`, following `MOD-13818`) before any customer can take a Gears
   database to Redis 8.6.
2. **`ramp_python.yml` is stale** on the `8.4` branch — the published `redisgears_python.*` 8.x
   packages advertise `compatible_redis_version: 7.4` and are unusable for this path (§5.2). Fix
   alongside the 8.6 release.
3. **No aarch64 build** for the 8.x line.
4. **Test coverage.** The 8.x packages were validated with minimal testing on top of `1.2.13`. A
   documented 7.x → 8.x database-upgrade test (including registrations, Python requirements and
   Active-Active) would let Support quote this procedure without caveats.
