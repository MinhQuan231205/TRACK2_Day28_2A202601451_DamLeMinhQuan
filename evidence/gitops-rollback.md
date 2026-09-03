# GitOps & model rollback evidence

Two rollback paths the platform supports, both demonstrated live on 2026-09-03.

## A. Model rollback — MLflow champion alias

Rollback is an alias move, not a redeploy. `lab28 rollback` points the `champion`
alias at the highest registered version below the current one.

```
$ lab28 inspect                                   # champion now
lab28-rag-release v17 is champion

$ lab28 release --prompt-version v-rollback-demo   # register + promote a new release
registered v20 as champion (run_id 39da28e5832445acb145b22ed52e16de)

$ lab28 ask "Trước rollback" --via-gateway
evidence.mlflow_release_version = 20              # serving followed the alias

$ lab28 rollback
champion moved from v20 to v19

$ lab28 ask "Sau rollback" --via-gateway
evidence.mlflow_release_version = 19              # next request already on v19
trace_id = bf31e0f3098524c47a9c5710e207a4fb
```

The serving path resolves `champion` per request (`lab28.mlflow.resolve_release`
span, `evidence.mlflow_release_version` in every answer), so the request right
after the alias move serves the rolled-back release with no restart.

This is also covered automatically by
`integration-tests/test_j3_promotion_rollback.py` (9 passed): register a new
version → confirm serving and the `lab28_rag_release_info` metric change → roll
back → confirm serving reverts. The J3 module restores the champion it found on
entry, so after the suite the alias is back to the pre-test version.

`evidence/ip06-mlflow-release.json` is the provenance snapshot of one such
registration: version `v22`, `promoted_from: v21`, with the tags an incident
review needs (`lab28.prompt_version`, `lab28.vllm_model_id`,
`lab28.embedding_model_id`, `lab28.delta_version`, `lab28.qdrant_collection`,
`lab28.feature_service`). The current champion after the rollback demo is **v21**
(`evidence/integration-report.json` and `lab28 inspect` agree).

## B. Desired-state rollback — Kubernetes manifests via Argo CD

`gitops/application.yaml` pins Argo CD to a tag with `automated: {prune: true,
selfHeal: true}`:

```yaml
source:
  targetRevision: refs/tags/v3.0.0
  path: deploy/kubernetes/base
syncPolicy:
  automated: { prune: true, selfHeal: true }
```

`selfHeal: true` means manual drift on the cluster (e.g. `kubectl set image` or
`kubectl scale`) is reverted to what Git says on the next sync — the cluster
cannot hold a change that is not in Git. A rollback is therefore a Git operation:
move `targetRevision` to the previous tag (or `git revert` the change) and let
Argo sync.

Demonstrated with the static validator standing in for a live Argo sync:

```
$ python scripts/validate_manifests.py
Kubernetes and GitOps manifest contracts passed          # baseline

# desired-state change
$ sed -i 's#day28-platform-api:3.0.0#day28-platform-api:3.1.0#' deploy/kubernetes/base/api.yaml
$ git diff deploy/kubernetes/base/api.yaml
-          image: ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0
+          image: ghcr.io/vinuni-ai20k/day28-platform-api:3.1.0
$ python scripts/validate_manifests.py
Kubernetes and GitOps manifest contracts passed          # 3.1.0 still valid

# rollback = revert the desired state
$ git checkout -- deploy/kubernetes/base/api.yaml
$ grep image: deploy/kubernetes/base/api.yaml
          image: ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0
$ python scripts/validate_manifests.py
Kubernetes and GitOps manifest contracts passed          # back to 3.0.0
```

The validator enforces the contract that makes a rollback safe: every Deployment
pins an explicit image tag (never `:latest`), runs as non-root, and declares
`readinessProbe` + `livenessProbe` + `resources` + `securityContext`; Gateway API
objects are stable `v1`; and `targetRevision` is a pinned tag, never a moving
branch. `deploy/kubernetes/base/api.yaml` also sets `replicas: 2` with the
readiness probe on `/ready` and liveness on `/health`, so an unready pod is
removed from the Service endpoints while the other replica serves — the
multi-replica version of the single-instance behaviour the dev Envoy gateway
cannot provide.
