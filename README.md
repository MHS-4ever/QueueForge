# QueueForge

QueueForge is a serverless AWS transaction-file processor:

`versioned S3 incoming/ -> SQS -> Lambda -> DynamoDB job state + S3 results/ report`

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
.\.venv\Scripts\python -m compileall -q src tests
.\.venv\Scripts\python -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
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
terraform -chdir=infrastructure plan -input=false '-out=queueforge-day2.tfplan'
terraform -chdir=infrastructure show -no-color queueforge-day2.tfplan
```

After explicit deployment approval, the user runs:

```powershell
terraform -chdir=infrastructure apply queueforge-day2.tfplan
terraform -chdir=infrastructure output
```

Day 2 adds an on-demand DynamoDB table and updates the Lambda package,
environment, and least-privilege policy. Lambda can read versions only below
`incoming/`, read/write only below `results/`, access only the jobs table, consume
only the processing queue, and write only its log group. Existing S3, SQS, DLQ,
notification, logging, and partial-batch behavior remain in place.

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

## Cleanup

Before destroy, empty the versioned S3 bucket including every object version and
delete marker. The S3 console's Empty action is suitable after carefully checking
the bucket name; `aws s3 rm --recursive` does not remove historical versions.
Destroying also removes job history, reports, queued messages, and logs.

```powershell
terraform -chdir=infrastructure plan -destroy '-out=cleanup.tfplan'
terraform -chdir=infrastructure show -no-color cleanup.tfplan
terraform -chdir=infrastructure apply cleanup.tfplan
```
