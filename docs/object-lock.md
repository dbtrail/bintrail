# Ransomware-proof archives — S3 Object Lock

After `bintrail rotate` drops a partition from the live index, its Parquet
archive in S3 is the **only copy** of those events. Anyone — or anything —
holding credentials that can delete S3 objects can erase your recovery
safety net: a compromised access key, a ransomware operator, or a fat-fingered
cleanup script. The index itself can be rebuilt from the archives
([index-recovery.md](index-recovery.md)); the archives cannot be rebuilt from
anything.

S3 Object Lock closes that hole: with a default retention rule, every object
becomes **immutable for the retention period** the moment it is written. No
new bintrail flags are needed — uploads work on a locked bucket out of the
box.

## Threat model

| Threat | Without Object Lock | With Object Lock (COMPLIANCE) |
|---|---|---|
| Stolen AWS key deletes the bucket contents | Archives gone | Deletes fail until retention expires |
| Ransomware encrypts/deletes index **and** S3 | Total loss | Archive tier survives; `bintrail restore-index` rebuilds |
| Buggy cleanup script | Archives gone | Deletes fail |
| Malicious insider with root | Archives gone | COMPLIANCE binds **every** principal, including root |

GOVERNANCE mode is weaker: principals holding `s3:BypassGovernanceRetention`
can still delete. Use COMPLIANCE unless you specifically need the escape
hatch — but note COMPLIANCE is irreversible: nothing shortens the retention
of an already-written object.

## Bucket recipe

Object Lock can only be enabled **when the bucket is created** — it cannot be
turned on later. (Versioning is enabled automatically and cannot be
suspended on a locked bucket.)

```bash
# 1. Create the bucket with Object Lock enabled
aws s3api create-bucket --bucket my-bintrail-archives \
  --object-lock-enabled-for-bucket \
  --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2

# 2. Default retention: every uploaded object is locked automatically.
#    bintrail sets no per-object retention — WITHOUT this rule nothing is locked.
aws s3api put-object-lock-configuration --bucket my-bintrail-archives \
  --object-lock-configuration \
  'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=90}}'

# 3. Lifecycle: expire noncurrent versions and stray delete markers AFTER the
#    lock expires, so storage does not grow forever.
aws s3api put-bucket-lifecycle-configuration --bucket my-bintrail-archives \
  --lifecycle-configuration '{"Rules":[{"ID":"reap-after-lock","Status":"Enabled",
    "Filter":{},"NoncurrentVersionExpiration":{"NoncurrentDays":91},
    "Expiration":{"ExpiredObjectDeleteMarker":true}}]}'
```

**Choosing the retention period**: at least your `rotate --retain` window —
if the lock expires while the partition is still in the live index, the
object is already deletable by the time it becomes the only copy — plus
however long you want deletion protection after that. `bintrail doctor`
checks this arithmetic for you:

```bash
bintrail doctor --source-dsn "$SRC" --retain 30d \
  --archive-s3 s3://my-bintrail-archives/prod/
```

The check is advisory (WARN, never a failing exit code) and reports: lock
disabled, lock enabled but no default rule (uploads land **unlocked**),
retention shorter than `--retain`, and the mode/period on PASS. It needs the
`s3:GetBucketObjectLockConfiguration` permission; without it the check SKIPs.

## What works on a locked bucket — verified

- **Uploads** (`rotate --archive-s3`, `upload`, `baseline --upload`): Object
  Lock requires a content checksum on every write; the pinned AWS SDK
  computes one (CRC32) by default on both single-part and multipart uploads.
  Nothing to configure.
- **Reads** (`query`/`recover`/`reconstruct`/`restore-index`, the shim, the
  console): read-only GETs, unaffected.
- **`archive reconcile --prune`**: deletes **registry rows only** — it never
  touches data files, locked or not.
- **Baseline local prune** (`rotate` with `--baseline-*`): deletes **local**
  directories after confirming a durable S3 copy; it never deletes from S3.
- The one S3 delete bintrail ever performs is the best-effort `_INCOMPLETE`
  marker cleanup after a baseline upload. On a locked (hence versioned)
  bucket this places a *delete marker* — a logical delete that retention
  permits — so discovery behaves normally and the lifecycle rule above reaps
  the leftovers. If it ever fails it was already harmless: `_SUCCESS`
  decides completeness.

There is **no** code path in bintrail that permanently deletes archived data
from S3. Reclaiming aged archives is deliberately left to your bucket
lifecycle policy — which on a locked bucket can only act after retention
expires, exactly the guarantee you asked Object Lock for.
