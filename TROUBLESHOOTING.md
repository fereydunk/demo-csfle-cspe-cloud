# Troubleshooting

The demo touches AWS, Confluent Cloud, and a long-running local Python server — there are several places where state can drift. This catalogues every gotcha hit during development plus the fix for each.

## Producer reports `Failed to decrypt with kms key id ... decryption failed`

**Counter-intuitive**: this error from the producer is almost always an **AWS credentials issue**, not a real decryption failure.

The encryption executor's first KMS call when producing is `kms:Decrypt` — to UNWRAP an existing DEK that's already cached in SR's DEK Registry for the (KEK, subject) pair. If your AWS creds are invalid/expired, AWS rejects the call and the executor surfaces it with the misleading message "decryption failed".

**Diagnose**:
```bash
aws kms describe-key --key-id alias/mortgage-csfle-kek --region us-west-2
```
If you see `UnrecognizedClientException: The security token included in the request is invalid` — that's it.

**Fix**: refresh AWS creds. Open the wizard at `/`, paste a new `export AWS_*` block in card 1, click Save. Re-click Produce — the existing DEK will unwrap cleanly now.

## Consumer page is "stuck" — no records appear

Three possible causes, in order of likelihood:

1. **No records on the topic** — check via:
   ```bash
   python3 -c "
   from confluent_kafka import Consumer, TopicPartition
   c = Consumer({'bootstrap.servers':'<bs>','security.protocol':'SASL_SSL',
                 'sasl.mechanism':'PLAIN','sasl.username':'<k>','sasl.password':'<s>',
                 'group.id':'diag','auto.offset.reset':'earliest'})
   from confluent_kafka.admin import AdminClient
   md = AdminClient({...same...}).list_topics(topic='mortgage-csfle')
   for p in md.topics['mortgage-csfle'].partitions:
       print(p, c.get_watermark_offsets(TopicPartition('mortgage-csfle', p), timeout=5))
   "
   ```
   If `low == high` for every partition → topic is empty. Run produce first.

2. **Consumer group already at end** — "Resume" mode picks up where the last commit left off. If this is the first ever read for this group, there's no committed offset → `auto.offset.reset=latest` skips existing records. Click "Start (from beginning)" instead — that gets a fresh group id with `-<timestamp>` suffix and reads from the beginning.

3. **Server is stale** — the server was started before you made code changes. Stop with Ctrl-C, restart `bash startup.sh`. The HTML response sets `Cache-Control: no-store` so browsers don't cache, but the Python process needs to be restarted to pick up code changes.

## Consumer with KEK access fails: `GroupAuthorizationException: Not authorized to access group`

The wizard appends a timestamp to the consumer group name on "Start (from beginning)" — e.g. `demo-csfle-with-kek-web-1778029231`. The RBAC step uses **PREFIXED** bindings on `Group:demo-{topic}-{kek-state}` so the timestamped names match.

If you see this error, the binding was probably created LITERAL (no `--prefix`). Check:
```bash
confluent iam rbac role-binding list --principal User:<sa-id> -o json
```

Look for `"resource_type": "Group"` and `"pattern_type": "PREFIXED"`. If `LITERAL`, re-run the wizard's RBAC step — the latest code uses `prefixed=True` for all Group bindings.

## Wizard log shows `✗ stream error` after every successful step

You're hitting a **cached browser copy of the JS** from before the `doneSeen` flag was added. Every successful SSE stream's clean close was being misinterpreted as an error.

**Fix**: hard-reload the wizard tab (`Cmd+Shift+R` on macOS). The server now sets `Cache-Control: no-store` so this should auto-resolve, but for the FIRST visit after the fix you need to bypass the existing cache.

## CSPE no-KEK page shows nothing (no records)

For older versions of the code: this was expected because the JSON-schema deserializer can't reconstruct an opaque CSPE-encrypted payload as JSON.

**The current code fixes this** by using the plain `confluent-kafka` Python client instead, which streams the raw bytes wrapped in `{"__raw__": "<base64>", "__schema_id__": NNN}` — matching how Confluent Cloud's console UI displays un-decryptable CSPE payloads.

If you still see nothing, check:
1. `pip3 install confluent-kafka` is installed (the wizard's preflight warns if not)
2. Records exist on the CSPE topic (run `make produce-cspe` or click Produce on `/produce/cspe`)
3. Server has been restarted to pick up the new `_stream_cspe_nokek_raw` handler

## Port 8893 already in use

The wizard binds 127.0.0.1:8893. If a previous instance didn't shut down cleanly:

```bash
lsof -ti:8893 | xargs kill -9
```

`startup.sh` does this automatically on boot — both `pkill -f` (for clean kills) and `lsof -ti:8893 | xargs kill -9` as a fallback. If you see "Address already in use" from `startup.sh`, wait 5 seconds and re-run (the kernel sometimes holds the socket briefly after the process dies).

## Subprocess shows `--producer-property is deprecated. Use --command-property instead.`

CP 8.2.0 deprecated `--producer-property` / `--consumer-property` / `--property` in favor of `--command-property` (Kafka client config) / `--reader-property` (producer-side serdes) / `--formatter-property` (consumer-side serdes). The current `web/server.py` and `Makefile` use the new names.

If you still see the warning, you're hitting a **stale Python server process** — restart with `bash startup.sh`.

## Schema rule registration fails with `feature requires Advanced`

Stream Governance is on Essentials. The encryption executor requires Advanced.

**Fix**: setup card 4 step 1 ("Stream Governance") auto-upgrades for you via the SRCM v3 PATCH. Or do it manually in Cloud UI: Environment → Stream Governance → Upgrade to Advanced.

⚠️ **Billing**: Advanced is a paid tier (typically ~$50/env/mo + per-schema fees). The wizard logs `BILLING: enables paid tier` when it does the upgrade.

## RBAC step fails with `cluster type needs org, env, and cloud-cluster`

You're running an old version of the bootstrap-rbac handler. The newer one passes BOTH `--cloud-cluster` and `--kafka-cluster` (same value) for Kafka-scoped bindings (Topic, Group, Cluster). Pull latest + restart.

## Service account creation fails with `service name "..." is already in use`

The CLI returns non-zero on duplicate name (different from "already exists" wording in older CLIs). The current `_cc_create_service_account` checks for both phrasings and looks up the existing SA by name as fallback. If you still see this error, you're on a stale server.

If the existing SA was created in a different env or you actually want to re-create it: `make clean-rbac` deletes all per-(topic,role) SAs.

## After deleting topics, producer fails with `decryption failed` on the new topic

You hit the AWS-creds-expired issue (see top of this file) AND the SR DEK Registry has stale wrapped DEKs from your earlier topic names — including for the new topic name if a prior produce attempt got partway through.

**Fix**: refresh AWS creds. Once they're valid, the cached DEK can be unwrapped successfully.

If you want a TRULY fresh start (delete all DEKs too):
```bash
SR_KEY=...; SR_SEC=...; SR_URL=https://...
for s in $(curl -s -u $SR_KEY:$SR_SEC $SR_URL/dek-registry/v1/keks/mortgage-csfle-kek/deks); do
  curl -X DELETE -u $SR_KEY:$SR_SEC $SR_URL/dek-registry/v1/keks/mortgage-csfle-kek/deks/$s
done
```
(Same for `mortgage-cspe-kek`.)

## I changed `CSFLE_TOPIC` after running the RBAC step — producer now fails

The producer SA's `DeveloperWrite Topic:<old-name>` binding doesn't apply to the new topic name. Three options:

1. **Re-run RBAC step** — bindings are idempotent, but the OLD bindings remain. Cleanest is `make clean-rbac` (deletes all 6 SAs + bindings) then re-run setup card 4 step 5. New bindings reference the new topic name.
2. **Manually add a binding for the new topic** — `confluent iam rbac role-binding create --principal User:<csfle-producer-sa-id> --role DeveloperWrite --resource Topic:<new-topic-name> --environment env-... --cloud-cluster lkc-... --kafka-cluster lkc-...`
3. **Use the OLD topic name** — easier than the above two if the rename was accidental.

## I see lots of `mortgage-csfle*-value` DEKs in SR — stale state

Each time you ran setup with a different topic name (e.g. `mortgage-csfle1612`, `mortgage-csfle1631`, ...), a fresh DEK was created. They accumulate in the DEK Registry under the SAME KEK. They don't break anything but are clutter.

To clean (after `make clean-rbac` so no SAs reference them):
```bash
SR_KEY=...; SR_SEC=...; SR_URL=https://...
curl -s -u $SR_KEY:$SR_SEC $SR_URL/dek-registry/v1/keks/mortgage-csfle-kek/deks \
  | python3 -c "import sys,json; [print(s) for s in json.load(sys.stdin)]" \
  | xargs -I{} curl -X DELETE -u $SR_KEY:$SR_SEC $SR_URL/dek-registry/v1/keks/mortgage-csfle-kek/deks/{}
```

## I get `ConnectionResetError [Errno 54]` in the server log

That's the stdlib HTTP server logging a closed keep-alive socket — happens every time a browser tab closes or navigates away. Suppressed by the wizard's `_Server.handle_error` override. If you still see it, you're on a stale Python process — restart.

## My .env got corrupted / I want to start fresh

```bash
make clean-rbac     # delete the 6 SAs + their API keys
make clean-keys     # schedule the 2 KMS keys for deletion
rm .env config/aws-session.env
cp .env.example .env
bash startup.sh     # walk the wizard from card 1
```

## I see "(OrgAdmin fallback — RBAC step not yet run)" on consumer pages

The per-(topic, role) SA's API keys aren't in `.env`. Either you haven't run setup card 4 step 5 (RBAC) yet, OR you cleaned the .env entries without re-running the step.

The fallback uses the OrgAdmin keys you minted in card 3 — works for the demo flow but doesn't enforce the per-KEK Confluent-layer boundary. The AWS-stripped env still enforces it at the AWS layer for no-KEK pages, so the visual demo still works; just less defense-in-depth.

**Fix**: run setup card 4 step 5 — the page rows will switch to showing the actual SA name and KEK binding.
