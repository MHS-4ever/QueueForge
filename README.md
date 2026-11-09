# QueueForge

QueueForge is a serverless AWS transaction-file processor:

`versioned S3 incoming/ -> SQS -> Lambda -> DynamoDB job state + S3 results/ report`

## Architecture and AWS services

1. A private, SSE-S3 encrypted, versioned S3 bucket accepts objects under
   `incoming/`. An object-created notification sends metadata to SQS.
2. A standard, SQS-managed encrypted processing queue buffers work. Its redrive
   policy moves a message to the encrypted DLQ after five receives.
3. A Python 3.12 Lambda consumes batches of up to five with maximum concurrency
   two and `ReportBatchItemFailures`. The queue visibility timeout is 180 seconds.
4. An on-demand DynamoDB table stores job state under the deterministic `job_id`
   partition key. A sparse `status-lease-index` supports bounded stale-lease queries.
5. Lambda reads the exact source object version and writes deterministic reports to
   `results/<job_id>.json` in the same bucket.
6. CloudWatch retains structured Lambda logs for seven days and alarms when the DLQ
   contains a visible message.

Terraform creates and tags these resources. There is no HTTP API, dashboard, or
additional database.

## Processing behavior

It accepts UTF-8 CSV files with a header row and JSON arrays of transaction objects.
Each record requires non-empty string `record_id` and `customer_id` values plus a
positive finite decimal `amount`. Files are limited to 1 MiB and 1,000 records.
Invalid records are reported without blocking valid records. For duplicate
`record_id` values, the first occurrence wins.

The processor always reads the exact S3 object version from the notification. Its
job ID is a stable SHA-256 digest of the bucket, key, and version ID. DynamoDB
conditional writes grant one worker a 120-second processing lease. Active duplicate
deliveries retry; expired leases can be reclaimed; terminal jobs are acknowledged
without reprocessing. A worker writes a deterministic report to
`results/<job_id>.json` before conditionally setting the terminal job status.

## Job states

- `PENDING`: notification registered but not claimed.
- `PROCESSING`: owned until `lease_expires_at` by `owner_token`.
- `COMPLETED`: every record was valid.
- `PARTIAL_SUCCESS`: at least one valid and one invalid/duplicate record.
- `REJECTED`: file-level rejection or no valid records.
- `FAILED`: reserved terminal state for later operational reconciliation.

Reports contain source identity, normalized valid records, counts, and validation
errors. DynamoDB stores source identity, lifecycle timestamps, counts, result
location, and transient lease ownership. Delivery remains at least once, but
conditional ownership and deterministic reports make repeated notifications safe.

## Local setup and validation

Use Python 3.12 and Terraform from PowerShell:

```powershell
Set-Location D:\Personal\Builder\03_QueueForge
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -e '.[dev]'
.\.venv\Scripts\python -m compileall -q src tests scripts
.\.venv\Scripts\python -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python -m ruff check src tests scripts
.\.venv\Scripts\python -m ruff format --check src tests scripts
terraform -chdir=infrastructure fmt -check -recursive
terraform -chdir=infrastructure init -input=false
terraform -chdir=infrastructure validate
```

Terraform uses profile `shipsafe-credits` in `us-east-1`. Generated ZIPs, plans,
state, caches, credentials, and local configuration are ignored. Keep the provider
lock file in version control.

## Review and deploy manually

No infrastructure is deployed by project setup or tests. Verify the target account,
then create and inspect a plan:

```powershell
aws sts get-caller-identity --profile shipsafe-credits --region us-east-1
terraform -chdir=infrastructure plan -input=false '-out=queueforge.tfplan'
terraform -chdir=infrastructure show -no-color queueforge.tfplan
```

After explicit deployment approval, the user runs:

```powershell
terraform -chdir=infrastructure apply queueforge.tfplan
terraform -chdir=infrastructure output
```

Lambda can read versions only below
`incoming/`, read/write only below `results/`, access only the jobs table, consume
only the processing queue, and write only its log group. Existing S3, SQS, DLQ,
notification, logging, and partial-batch behavior remain in place. Day 3 adds a
sparse DynamoDB index for bounded stale-lease queries, a DLQ-visible CloudWatch
alarm, and in-place Lambda changes; fault injection remains disabled by default.

## CLI and smoke test

The CLI uses standard AWS credential resolution. Set the profile plus values from
Terraform outputs:

Profiles configured for AWS browser login require the optional CRT dependency.
Install it with the development tools using:

```powershell
.\.venv\Scripts\python -m pip install -e '.[dev,aws-login]'
```

```powershell
$env:AWS_PROFILE = 'shipsafe-credits'
$env:AWS_REGION = 'us-east-1'
$env:QUEUEFORGE_BUCKET = terraform -chdir=infrastructure output -raw bucket_name
$env:QUEUEFORGE_TABLE = terraform -chdir=infrastructure output -raw jobs_table_name
$env:QUEUEFORGE_DLQ_URL = terraform -chdir=infrastructure output -raw dead_letter_queue_url
$env:QUEUEFORGE_PROCESSING_QUEUE_URL = terraform -chdir=infrastructure output -raw processing_queue_url
```

Create a local test file and upload it. The upload response prints the exact version
and expected job ID:

```powershell
@'
record_id,customer_id,amount
r-001,c-001,12.50
r-002,c-002,-1
r-001,c-003,4.00
'@ | Set-Content -Encoding utf8 build\smoke.csv

queueforge upload build\smoke.csv
queueforge jobs --limit 25
queueforge status <job-id>
queueforge report <job-id>
```

The expected terminal status is `PARTIAL_SUCCESS`: one normalized record and two
validation errors. Allow for asynchronous S3/SQS/Lambda delivery before checking.
Operational checks:

```powershell
$queueforgeLogs = terraform -chdir=infrastructure output -raw log_group_name
aws logs tail $queueforgeLogs --since 10m --format short --profile shipsafe-credits --region us-east-1
$queueforgeDlq = terraform -chdir=infrastructure output -raw dead_letter_queue_url
aws sqs get-queue-attributes --queue-url $queueforgeDlq --attribute-names ApproximateNumberOfMessages --profile shipsafe-credits --region us-east-1
```

## Failure inspection and recovery

Inspection is bounded and never deletes DLQ messages:

```powershell
queueforge failures --limit 10
queueforge reconcile --limit 100
```

`reconcile` reports expired `PROCESSING` leases plus DLQ messages, including
messages that have no DynamoDB job. It does not reprocess anything. Targeted
recovery is explicit and only accepts `FAILED`, expired `PROCESSING`, or a prior
partially prepared recovery. It conditionally returns that job to `PENDING` and
sends one reconstructed notification to the processing queue:

```powershell
queueforge recover --job-id <job-id> --confirm <job-id>
```

Targeted recovery does not remove a matching original message from the DLQ; inspect
that queue again before deciding whether a later queue-wide redrive is appropriate.

AWS-native redrive is a separate, queue-wide operation. It moves all eligible DLQ
messages and is not equivalent to targeted recovery:

```powershell
queueforge recover --redrive-dlq --confirm REDRIVE-DLQ
```

Completed, partially successful, rejected, and actively leased jobs are protected
from targeted recovery. The CloudWatch alarm `queueforge-dev-dlq-visible` enters
ALARM when the DLQ has a visible message. No Lambda `Errors` alarm is created because
SQS partial-batch failures are successful Lambda invocations and would not be
represented reliably by that metric.

## Controlled fault testing

Faults are disabled in normal plans. When manually enabled, they apply only to the
fixed `incoming/__queueforge_faults__/` namespace:

- `first/`: fail receive attempt 1, then allow retry.
- `persistent/`: fail every receive so the message can reach the DLQ.
- `after-report/`: fail attempt 1 after durable report creation and before finalization.

Enable only for a controlled test deployment:

```powershell
terraform -chdir=infrastructure plan -var='enable_fault_injection=true' '-out=faults-on.tfplan'
terraform -chdir=infrastructure show -no-color faults-on.tfplan
terraform -chdir=infrastructure apply faults-on.tfplan
```

Disable immediately after the test and verify the plan changes the Lambda environment
back to `false`:

```powershell
terraform -chdir=infrastructure plan -var='enable_fault_injection=false' '-out=faults-off.tfplan'
terraform -chdir=infrastructure show -no-color faults-off.tfplan
terraform -chdir=infrastructure apply faults-off.tfplan
```

## 100-file acceptance test

The harness uploads exactly 100 uniquely keyed CSV/JSON inputs: valid CSV, valid
JSON, mixed-quality CSV, malformed JSON, and duplicate-record JSON. It polls with a
bounded timeout, validates DynamoDB counts and S3 reports, and writes a JSON summary.
It never runs implicitly and refuses execution without `--execute`:

```powershell
.\.venv\Scripts\python scripts\load_test.py --execute --timeout 600 --interval 5
```

The default summary is `build/load-test-summary.json`. A timeout, missing job,
nonterminal job, duplicate job ID, status mismatch, count mismatch, or report mismatch
causes a nonzero exit code. Timings in the summary are measurements from that run.

## Verification record

Local tests use fakes for AWS clients; they do not constitute AWS integration tests.
The final local audit on 2026-11-09 passed 51 pytest tests covering parsing and
limits, deterministic IDs, conditional claims, ownership fencing, partial batches,
report conflicts, fault selection, recovery preparation, DLQ inspection, and load
test result verification. Python compilation, Ruff lint/format, Terraform formatting,
Terraform validation, and an isolated Lambda ZIP import also passed.

The manually executed AWS acceptance run recorded in the ignored
`build/load-test-summary.json` processed 100/100 files in 78.687 seconds: 47.250
seconds uploading and 31.437 seconds processing and verifying. It recorded 100
terminal jobs, with zero missing jobs, duplicate job IDs, unresolved jobs, timeouts,
or mismatches. This is one measured acceptance run, not a throughput benchmark.

Additional operator-observed AWS checks passed:

- first-attempt failure followed by automatic retry;
- crash after report creation followed by completion with unchanged S3 VersionId
  and ETag;
- five persistent failures followed by DLQ delivery;
- expired-lease discovery and targeted job recovery;
- AWS-native redrive of one of one DLQ messages;
- direct five-message Lambda invocation returning only
  `forced-envelope-failure`; and
- final `FAULT_INJECTION_ENABLED=false` verification.

The direct invocation verifies the deployed handler response but does not prove that
SQS happened to form a five-message poll batch. The 1 MiB/1,000-record boundaries,
unsupported encodings, conflicting report protection, concurrent claims, `FAILED`
state recovery, and active-lease rejection are covered locally rather than by a
dedicated live AWS case.

## Limitations and trade-offs

- Delivery is at least once. Deterministic IDs, conditional state changes, and
  create-if-absent reports make duplicates safe; they do not provide exactly-once
  delivery at the transport layer.
- Leases have no heartbeat. The 120-second lease assumes the enforced 1 MiB and
  1,000-record workload completes well inside the 180-second SQS visibility timeout.
- The stale-job GSI is eventually consistent, so reconciliation can briefly lag.
- `jobs` and reconciliation output are intentionally bounded. `jobs` is an unordered
  scan intended for small operator views, not analytics.
- The DLQ alarm has no notification action. Operators must view its CloudWatch state
  or add notification routing outside this project.
- Terraform state is local. It is ignored by Git but does not have remote locking,
  shared access, or managed backup.
- The acceptance result covers 100 small files in one region/account. Sustained load,
  multi-region recovery, cross-account operation, and disaster recovery were not
  tested.
- Reports share the versioned input bucket. Cleanup must remove input versions,
  report versions, and delete markers before Terraform can delete the bucket.

## Cleanup

Destroy is irreversible. It removes job history, reports, queue contents, alarms,
and logs. Export anything that must be retained, stop uploads, verify the AWS account,
and confirm fault injection is disabled. The bucket does not use `force_destroy`, so
Terraform correctly refuses to delete it while any version or delete marker remains.

The safest interactive option is the S3 console's **Empty** action after confirming
the exact bucket name. `aws s3 rm --recursive` is insufficient because it leaves old
versions and delete markers.

For CLI cleanup, review the resolved identity and bucket before deleting anything:

```powershell
Set-Location D:\Personal\Builder\03_QueueForge
$profile = 'shipsafe-credits'
$region = 'us-east-1'
$bucket = terraform -chdir=infrastructure output -raw bucket_name

aws sts get-caller-identity --profile $profile --region $region
Write-Host "Bucket to empty: $bucket"
$confirmation = Read-Host 'Type the complete bucket name to continue'
if ($confirmation -cne $bucket) { throw 'Bucket confirmation did not match.' }

New-Item -ItemType Directory -Force build | Out-Null
$deleteFile = Join-Path (Resolve-Path build) 's3-version-delete-batch.json'

do {
    $inventory = aws s3api list-object-versions `
      --bucket $bucket `
      --profile $profile `
      --region $region `
      --output json | ConvertFrom-Json

    $targets = @()
    $targets += @($inventory.Versions | ForEach-Object {
        @{ Key = $_.Key; VersionId = $_.VersionId }
    })
    $targets += @($inventory.DeleteMarkers | ForEach-Object {
        @{ Key = $_.Key; VersionId = $_.VersionId }
    })

    for ($offset = 0; $offset -lt $targets.Count; $offset += 1000) {
        $last = [Math]::Min($offset + 999, $targets.Count - 1)
        $batch = @($targets[$offset..$last])
        $body = @{ Objects = $batch; Quiet = $true } |
            ConvertTo-Json -Depth 5 -Compress
        [IO.File]::WriteAllText(
            $deleteFile,
            $body,
            [Text.UTF8Encoding]::new($false)
        )
        $deleteResult = aws s3api delete-objects `
          --bucket $bucket `
          --delete "file://$deleteFile" `
          --profile $profile `
          --region $region `
          --output json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw 'S3 version deletion failed.' }
        if (@($deleteResult.Errors).Count -gt 0) {
            $deleteResult.Errors | Format-Table
            throw 'S3 returned one or more per-object deletion errors.'
        }
    }
} while ($targets.Count -gt 0)

$remaining = aws s3api list-object-versions `
  --bucket $bucket `
  --profile $profile `
  --region $region `
  --output json | ConvertFrom-Json

if (@($remaining.Versions).Count -ne 0 -or
    @($remaining.DeleteMarkers).Count -ne 0) {
    throw 'Bucket still contains versions or delete markers.'
}
```

Only after the bucket is verified empty, create and review a fresh destroy plan:

```powershell
terraform -chdir=infrastructure plan -destroy '-out=cleanup.tfplan'
terraform -chdir=infrastructure show -no-color cleanup.tfplan
terraform -chdir=infrastructure apply cleanup.tfplan
```

Run `terraform -chdir=infrastructure state list` afterward. An empty result confirms
Terraform no longer tracks QueueForge resources. Do not delete local state before
the destroy completes successfully.
