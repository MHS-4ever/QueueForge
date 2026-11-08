# QueueForge — Day 1

Python 3.12 / Terraform / AWS event intake:

`private versioned S3 (incoming/) → standard SQS → Lambda → structured CloudWatch logs`

Lambda inspects notification metadata only; it never downloads or parses object contents.
S3 test notifications are acknowledged. Failed SQS messages are returned through
`ReportBatchItemFailures`; successful messages are deleted by the Lambda integration.
After five receives, failed messages move to the DLQ. Delivery is at least once:
duplicate logs are possible, including when one nested record fails after earlier
records in the same message were logged. Day 1 has no deduplication or job tracking.

## Local setup and checks (PowerShell)

```powershell
Set-Location D:\Personal\Builder\03_QueueForge
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -e '.[dev]'
.\.venv\Scripts\python -m compileall -q src tests
.\.venv\Scripts\python -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
terraform -chdir=infrastructure fmt -check
terraform version
terraform -chdir=infrastructure init -input=false
terraform -chdir=infrastructure validate
```

Terraform requires >=1.6 and <2.0, AWS provider 6.x, and archive provider 2.x
(>=2.7). Inspect the installed platform and provider startup results; if a provider
does not support `windows_386`, stop and report that error. Do not substitute
unverified binaries. Keep the generated `.terraform.lock.hcl` in version control
after successful initialization. The archive provider creates `build/processor.zip`
from the handler source during planning; no runtime dependencies are needed.

## Review and deploy (user-operated)

Configure/authenticate the existing `shipsafe-credits` profile using your normal
AWS workflow. The provider fixes that profile and `us-east-1`. These commands read
the account and prepare a plan; planning does not deploy AWS resources:

```powershell
aws sts get-caller-identity --profile shipsafe-credits --region us-east-1
terraform -chdir=infrastructure plan -input=false '-out=queueforge.tfplan'
terraform -chdir=infrastructure show -no-color queueforge.tfplan
```

Review the account, region, IAM permissions, resource names, and all proposed
changes. After explicit deployment authorization, the user executes:

```powershell
terraform -chdir=infrastructure apply queueforge.tfplan
terraform -chdir=infrastructure output
```

The bucket name contains the account ID and region. Resources have Project,
Environment, and ManagedBy tags. Lambda has 30 seconds / 256 MB; SQS visibility
is 180 seconds, batch size is 5, and event-source maximum concurrency is 2.
The DLQ retains messages for 14 days; the processing queue retains them for 4 days.
CloudWatch logs retain data for 7 days. The Lambda role can consume only its queue
and write only its log group; it has no S3 read permission. S3 can send only from
this bucket and account. SQS uses managed encryption and S3 uses SSE-S3.

Local state, plan files, and ZIPs remain local and ignored. Protect local state;
do not commit credentials, state, plans, `.terraform/`, or generated build output.
Deployment creates billable resources. There is no dashboard or HTTP API.

## Verify after deployment

```powershell
$queueforgeBucket = terraform -chdir=infrastructure output -raw bucket_name
$queueforgeLogs = terraform -chdir=infrastructure output -raw log_group_name
New-Item -ItemType Directory -Force build | Out-Null
Set-Content -LiteralPath build\smoke.txt -Value 'QueueForge metadata smoke test'
aws s3 cp build\smoke.txt "s3://$queueforgeBucket/incoming/smoke.txt" --profile shipsafe-credits --region us-east-1
aws logs tail $queueforgeLogs --since 10m --format short --profile shipsafe-credits --region us-east-1
$queueforgeDlq = terraform -chdir=infrastructure output -raw dead_letter_queue_url
aws sqs get-queue-attributes --queue-url $queueforgeDlq --attribute-names ApproximateNumberOfMessages --profile shipsafe-credits --region us-east-1
```

Allow for asynchronous delivery. Look for `s3_object_created` with the bucket,
decoded key, message ID, size, version ID, and sequencer. A test event may produce
`s3_test_event_ignored`. Uploads outside `incoming/` should not generate object logs.
DLQ counts should stay zero during a successful smoke test. Logs include object
names, so avoid placing sensitive data in object keys.

## Cleanup (user-operated)

Before destroying, empty the S3 bucket **including all versions and delete
markers**, using the S3 console's Empty action after checking the bucket name.
`aws s3 rm --recursive` alone does not empty a versioned bucket. Terraform
intentionally does not force-delete stored objects. Destroying removes queued
messages and CloudWatch logs as well as the infrastructure.

```powershell
terraform -chdir=infrastructure plan -destroy '-out=cleanup.tfplan'
terraform -chdir=infrastructure show -no-color cleanup.tfplan
terraform -chdir=infrastructure apply cleanup.tfplan
```
