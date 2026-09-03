# Failure / Recovery record — no data loss

Scope: one live degraded/recovery cycle on the read path, plus the write-path
no-data-loss cases proved by the J4 integration journey.

## 1. Live dependency outage — Feast (non-mandatory)

| Step | Clock (UTC) | Command | Observed |
|---|---|---|---|
| Baseline | 2026-09-03T09:34:21Z | `lab28 inspect` | `feedback` v30 / 42 rows, `documents` v16 / 25 rows; all deps ready |
| Inject | 2026-09-03T09:35:31Z | `docker compose stop feast` | — |
| Probe | +10s | `GET /ready` | HTTP 503, `status: "degraded"`, `feast.ready=false`, every other component `true` |
| Serve | +12s | `lab28 ask "Kiến trúc nền tảng gồm mấy tầng?"` | **HTTP 200**, 1241-char grounded answer, `evidence.degraded=true`, reason `"feature store unavailable: Feast feature server unreachable: ConnectError"` |
| Recover | 2026-09-03T09:36:01Z | `docker compose start feast` | — |
| Confirm | +~10s | `GET /ready` | `status: "ready"` again, `feast.ready=true` |
| After | — | `lab28 inspect` | `feedback` v30 / 42 rows, `documents` v16 / 25 rows — **identical to baseline** |

Interpretation: Feast is a *degradable* dependency. Its outage flips readiness to
`degraded` (pod taken out of rotation by the K8s readiness probe) but the request
path still answers, flags the degradation in the response evidence and increments
`lab28_degraded_responses_total`. No write happens on the read path, so the
lakehouse is byte-for-byte unchanged across the outage.

## 2. Write-path no-data-loss — J4 integration journey (all passed in the suite run)

`integration-tests/test_j4_degraded_recovery.py`:

- `test_one_unparseable_message_does_not_fail_the_batch` — a poison Kafka record
  does not fail the Airflow DAG run (`state == "success"`).
- `test_the_unparseable_message_is_parked_rather_than_dropped` — the poison
  record lands on `data.raw.dlq` with topic/partition/offset/key, count strictly
  increases.
- `test_the_good_record_in_the_same_batch_still_reached_the_lakehouse` — the
  valid record in the same batch is present in Delta exactly once.
- `test_the_replayed_event_does_not_duplicate_the_row` — redelivering an event
  after a simulated crash MERGEs into the same row (Delta stays at one row per
  `idempotency_key`); the DAG run succeeds.
- `test_the_feature_store_outage_reads_as_degraded_not_broken` /
  `test_an_answer_is_still_served_without_features` — a Feast outage is served
  degraded (HTTP 200) and counted, not turned into a 503.

The mandatory-vs-degradable split is the contract: losing the vector store
(retrieval, mandatory) fails readiness *closed* (503 with the dependency named);
losing Feast (features, degradable) serves the request and says so.
