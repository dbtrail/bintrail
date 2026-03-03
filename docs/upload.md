# Bintrail — Upload Command

The `bintrail upload` command uploads local Parquet files to S3, independently of the pipeline that generated them. This is useful when files were created by `baseline` or `rotate --archive-dir` without the S3 flags — for example, because the network was down, AWS credentials weren't configured, or S3 wasn't needed at the time.

---

## Quick Start

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/
```

This recursively walks the source directory and uploads every `*.parquet` file to S3, preserving the directory structure as key prefixes.

---

## Flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--source` | Yes | — | Local directory containing Parquet files |
| `--destination` | Yes | — | S3 destination URL (e.g. `s3://my-bucket/archives/`) |
| `--region` | No | SDK default | AWS region override |
| `--retry` | No | `false` | Skip files that already exist in S3 (checked via `HeadObject`) |
| `--index-dsn` | No | — | MySQL DSN for the index database; updates `archive_state` with S3 metadata |
| `--format` | No | `text` | Output format: `text` or `json` |

---

## AWS Credentials

`bintrail upload` uses the standard AWS SDK credential chain. The SDK checks these sources **in order** — the first one that provides valid credentials wins:

### 1. Environment variables (recommended for CI/CD and automation)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1

bintrail upload --source ./archives/ --destination s3://my-bucket/archives/
```

For temporary credentials (e.g. from `aws sts assume-role`), also set:

```bash
export AWS_SESSION_TOKEN=FwoGZXIvYXdzE...
```

### 2. Shared credentials file (`~/.aws/credentials`)

```ini
# ~/.aws/credentials
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

To use a named profile instead of `[default]`:

```bash
export AWS_PROFILE=bintrail-prod
bintrail upload --source ./archives/ --destination s3://my-bucket/archives/
```

### 3. Shared config file (`~/.aws/config`)

```ini
# ~/.aws/config
[default]
region = us-east-1

[profile bintrail-prod]
region = us-west-2
role_arn = arn:aws:iam::123456789012:role/BintrailUploader
source_profile = default
```

### 4. EC2 instance metadata / ECS task role / EKS IRSA

On AWS infrastructure, credentials are provided automatically:

- **EC2**: Attach an IAM instance profile to the instance
- **ECS**: Set a task IAM role in the task definition
- **EKS**: Use IAM Roles for Service Accounts (IRSA)

No environment variables or config files needed — the SDK discovers credentials from the instance metadata service.

### Region resolution

The AWS region is resolved in this order:

1. `--region` flag (if provided)
2. `AWS_REGION` environment variable
3. `AWS_DEFAULT_REGION` environment variable
4. `~/.aws/config` region setting for the active profile

### Minimum IAM permissions

The IAM principal (user or role) needs these S3 permissions on the destination bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:HeadObject"
      ],
      "Resource": "arn:aws:s3:::my-bucket/archives/*"
    }
  ]
}
```

- `s3:PutObject` — required for uploading files
- `s3:HeadObject` — required when using `--retry` to check if files already exist
- `s3:GetObject` — required if you later query archives with `bintrail query --archive-s3`

---

## Examples

### Upload archive files from a previous rotate

```bash
# Archives created by: bintrail rotate --archive-dir /var/lib/bintrail/archives/ ...
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/
```

### Upload with retry (skip already-uploaded files)

Useful when a previous upload was interrupted:

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/ \
  --retry
```

### Upload and update archive_state

When uploading rotate archives, pass `--index-dsn` to record the S3 location in the `archive_state` table. This allows `bintrail status` to show which partitions have been uploaded to S3:

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/ \
  --index-dsn 'user:pass@tcp(localhost:3306)/binlog_index'
```

The command extracts `partition_name` and `bintrail_id` from the Hive-partitioned directory structure (`bintrail_id=<uuid>/event_date=YYYY-MM-DD/event_hour=HH/events.parquet`) and updates the matching `archive_state` rows with `s3_bucket`, `s3_key`, and `s3_uploaded_at`. Files that don't match this pattern (e.g. baseline Parquet files) are still uploaded but skip the DB update.

### Upload baseline files

```bash
# Baselines created by: bintrail baseline --input ... --output /var/lib/bintrail/baselines/
bintrail upload \
  --source /var/lib/bintrail/baselines/ \
  --destination s3://my-bucket/baselines/ \
  --retry
```

### JSON output

```bash
bintrail upload \
  --source ./archives/ \
  --destination s3://my-bucket/archives/ \
  --format json
```

```json
{
  "uploaded": 42,
  "skipped": 0,
  "destination": "s3://my-bucket/archives/",
  "duration_ms": 12345
}
```

### Explicit region

```bash
bintrail upload \
  --source ./archives/ \
  --destination s3://my-bucket/archives/ \
  --region eu-west-1
```

---

## How It Works

1. **Walk**: Recursively scans `--source` for `*.parquet` files
2. **Key construction**: For each file, computes the S3 key by taking the path relative to `--source` and prepending the prefix from `--destination`
3. **Retry check** (if `--retry`): Issues a `HeadObject` request — skips the file if it already exists in S3
4. **Upload**: Uploads the file via `PutObject`
5. **DB update** (if `--index-dsn`): For files matching the Hive archive path pattern, updates `archive_state` with the S3 bucket, key, and upload timestamp

---

## Troubleshooting

### "load AWS config" error

The AWS SDK could not find valid credentials. Check that one of the credential sources above is configured. Common causes:

- Missing `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
- Expired temporary credentials (`AWS_SESSION_TOKEN`)
- Wrong `AWS_PROFILE` pointing to a non-existent profile in `~/.aws/credentials`

### "Access Denied" on PutObject

The IAM principal has insufficient permissions. Verify the policy attached to your IAM user/role includes `s3:PutObject` on the correct bucket and prefix.

### "no such host" or timeout

The S3 endpoint is unreachable. Check:

- Network connectivity to AWS
- The `--region` flag or `AWS_REGION` matches the bucket's region
- VPC endpoints are configured (if running inside a VPC with no internet access)

### archive_state not updated

The `--index-dsn` update only works for files whose paths match the Hive archive layout produced by `rotate --archive-dir`. Baseline files use a different directory structure and won't trigger DB updates.
