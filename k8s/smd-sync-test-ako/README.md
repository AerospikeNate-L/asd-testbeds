# SMD sync test (Aerospike Kubernetes Operator)

Kubernetes + [AKO](https://docs.aerospike.com/cloud/kubernetes/operator) variant of [`docker/smd-sync-test/`](../../docker/smd-sync-test/). It exercises the same SMD scenarios by driving an `AerospikeCluster` CR (scale up/down, PVC delete for rejoin) instead of Docker Compose.

## Differences vs Docker bed

- **Node IDs**: AKO sets `node-id` from the pod name (`0a0`, `0a1`, …), not fixed `a1`/`a2`/`a3`.
- **Staged membership**: Use `spec.size` (1 → 3, 2 → 3) instead of starting individual compose services.
- **Pull test topology**: A 2→3 scale adds the **highest** ordinal last; Docker starts the two higher IDs first, then the lowest. The test still checks that a **new empty member** receives SMD.
- **Custom `asd`**: Build an image that **copies** your binary (no bind mount). Rebuild and `kind load` when you switch branches.

## Timing parity (`timing`, `timing-rejoin`)

Large-SMD timing mirrors [`docker/smd-sync-test/test-smd-sync.sh`](../../docker/smd-sync-test/test-smd-sync.sh) **`timing`** and **`timing-rejoin`** modes:

- **Same generators** on the host: [`docker/smd-sync-test/gen-large-smd.py`](../../docker/smd-sync-test/gen-large-smd.py) and [`docker/smd-sync-test/gen-realistic-smd.py`](../../docker/smd-sync-test/gen-realistic-smd.py) (requires **`python3`** on the machine running the harness).
- **Pre-provisioned PVCs** — [`manifests/pvc-workdir-preprovision.yaml`](manifests/pvc-workdir-preprovision.yaml) creates `workdir-<cluster>-0-{0,1,2}` claims before the `AerospikeCluster` exists so the StatefulSet adopts them (same naming pattern AKO uses).
- **Seeding** — short-lived `busybox` pods mount each PVC at `/opt/aerospike`, reset `smd/`, and `kubectl cp` generated `.smd` files from the host staging dir (`SMD_DATA_DIR`, default `/tmp/smd-timing-k8s-data`).
- **`initMethod: none`** — [`manifests/aerospikecluster-timing.yaml`](manifests/aerospikecluster-timing.yaml) matches the normal cluster spec but sets `filesystemVolumePolicy.initMethod: none` so **`aerospike-init` does not delete pre-seeded files** on the workdir volume (the default bed uses `deleteFiles`, which would wipe staged SMD).

**Not ported** (still Docker-only in this repo): `timing-real`, `timing-conflict`, `show-limits` — same rationale as before (extra scenarios; add similarly if needed).

**Interpretation:** Wall-clock and `initial SMD sync wait done - elapsed … us` / `sync wait done …` lines match the Docker script’s parsing. Total time includes Kubernetes-specific costs (CSI attach, init containers, scheduling); compare runs on the same cluster image/config when benchmarking.

After the usual prerequisites (kind, operator, server image, secrets):

```bash
./test-smd-sync-k8s.sh timing
./test-smd-sync-k8s.sh timing-rejoin
```

Tune sweeps with the same-style env vars as Docker (see [`docker/smd-sync-test/README.md`](../../docker/smd-sync-test/README.md)), for example:

```bash
TIMING_ITEMS='10000 50000' TIMING_VALUE_SIZE=200 ./test-smd-sync-k8s.sh timing
TIMING_REJOIN_STALE_PCT=80 TIMING_REJOIN_SECURITY_ITEMS=100000 ./test-smd-sync-k8s.sh timing-rejoin
```

Results default to `./timing-results-k8s/` under this directory (`TIMING_RESULTS_DIR`). `./test-smd-sync-k8s.sh timing-cleanup` is an alias for `cleanup-full` after a failed timing run.

## Prerequisites

- `kubectl`, `docker`, and a Kubernetes cluster (**kind** is the default local path).
- **Go is not required** for [`scripts/install-operator.sh`](scripts/install-operator.sh): it downloads `kustomize` into `.tools/` and applies `config/default` from your AKO clone. Applies use **`kubectl apply --server-side`** so large Aerospike CRDs do not hit client-side `last-applied-configuration` annotation size limits (see Troubleshooting).
- **cert-manager** (installed by `scripts/install-operator.sh` if missing).
- Aerospike Kubernetes Operator deployed (`scripts/install-operator.sh`).
- EE **`asd`** build and valid **`features.conf`**.
- A **StorageClass** named `standard` (kind’s default) or edit `manifests/*.yaml` to set `storage.volumes[].source.persistentVolume.storageClass` to your class.

## Quick path (kind)

From this directory (`asd-testbeds/k8s/smd-sync-test-ako/`):

1. **Create cluster**

   ```bash
   ./scripts/setup-kind.sh
   ```

2. **Build operator image** (in your [`aerospike-kubernetes-operator`](https://github.com/aerospike/aerospike-kubernetes-operator) clone)

   The repo vendors JSON schemas via a **Git submodule** (`pkg/configschema/schemas`). Without it, `docker build` fails with `pattern schemas/json/aerospike: no matching files found`.

   ```bash
   cd /path/to/aerospike-kubernetes-operator
   git submodule update --init --recursive
   ```

   **Required for kind:** after `docker build`, load the image into the cluster node (otherwise pods hit **ImagePullBackOff** trying Docker Hub):

   ```bash
   export VERSION=local-dev
   docker build -t aerospike/aerospike-kubernetes-operator:${VERSION} --build-arg VERSION=${VERSION} .
   kind load docker-image aerospike/aerospike-kubernetes-operator:${VERSION} --name smd-sync-ako
   ```

   (`make docker-buildx` in that repo pushes multi-arch builds; use plain `docker build` for kind.)

   Alternatively skip building the operator and use a published image matching your AKO version in `OPERATOR_IMG` (then only step 3 needs the image tag, not a local `docker build`).

3. **Install operator**

   ```bash
   export OPERATOR_IMG=aerospike/aerospike-kubernetes-operator:local-dev
   export AKO_REPO=/path/to/aerospike-kubernetes-operator   # optional if not auto-detected
   ./scripts/install-operator.sh
   ```

   By default AKO’s deployment watches **only** namespace `aerospike`. This harness creates clusters in **`smd-sync-ako`**, so `install-operator.sh` sets `WATCH_NAMESPACE` to **`aerospike,smd-sync-ako`** (override with `OPERATOR_WATCH_NAMESPACES` if needed).

   Confirm CRDs exist before running tests:

   ```bash
   kubectl get crd aerospikeclusters.asdb.aerospike.com
   ```

4. **Server dev image** (embeds `ASD_BINARY`)

   ```bash
   export ASD_BINARY=/path/to/target/Linux-x86_64/bin/asd
   ./scripts/build-load-server-image.sh
   ```

   Image tag defaults to `smd-sync-asd-enterprise:8.1.2.0-dev` (must match `spec.image` in manifests). AKO requires the image **name** to contain `enterprise` or `federal`, and the **tag** must contain a dotted version substring (e.g. `8.1.2.0`) for `GetImageVersion`; pure tags like `:dev` fail validation.

5. **Secrets**

   ```bash
   export FEATURES_CONF=/path/to/features.conf
   ./scripts/create-secrets.sh
   ```

   For `./test-smd-sync-k8s.sh auth`, ensure `auth-secret` exists (script creates it with `ADMIN_PASSWORD`, default `admin123`).

6. **Run tests**

   ```bash
   chmod +x ./test-smd-sync-k8s.sh ./scripts/*.sh
   ./test-smd-sync-k8s.sh basic
   ./test-smd-sync-k8s.sh all
   ./test-smd-sync-k8s.sh auth
   ./test-smd-sync-k8s.sh timing
   ./test-smd-sync-k8s.sh timing-rejoin
   ```

## Bring your own cluster

- Point `kubectl` at your cluster (`KUBECONFIG`).
- Install cert-manager + AKO the same way (or Helm; see operator docs). Ensure the operator’s **`WATCH_NAMESPACE`** includes **`smd-sync-ako`** (or deploy the `AerospikeCluster` only inside a watched namespace).
- Push `smd-sync-asd-enterprise:<version>-dev` (name contains `enterprise` or `federal`; tag embeds `MAJOR.MINOR.PATCH.BUILD` ≥ operator minimum, typically ≥ `6.0.0.0`) to a registry your nodes can pull and set `spec.image` accordingly (and imagePullSecrets if needed).
- Ensure the StorageClass in the manifests exists.

## Scripts

| Script | Purpose |
|--------|---------|
| [`scripts/setup-kind.sh`](scripts/setup-kind.sh) | Create `kind` cluster `smd-sync-ako` |
| [`scripts/install-operator.sh`](scripts/install-operator.sh) | cert-manager + `make deploy` from AKO repo |
| [`scripts/build-load-server-image.sh`](scripts/build-load-server-image.sh) | `docker build` + `kind load` server image |
| [`scripts/create-secrets.sh`](scripts/create-secrets.sh) | Namespace + `aerospike-secret` + `auth-secret` |
| [`test-smd-sync-k8s.sh`](test-smd-sync-k8s.sh) | Test harness |

## Manifests

| File | Purpose |
|------|---------|
| [`manifests/namespace.yaml`](manifests/namespace.yaml) | `smd-sync-ako` |
| [`manifests/workload-operator-rbac.yaml`](manifests/workload-operator-rbac.yaml) | SA + `ClusterRoleBinding` for pod `serviceAccountName` (applied by scripts; uses `__TESTBED_NAMESPACE__`) |
| [`manifests/aerospikecluster.yaml`](manifests/aerospikecluster.yaml) | 3 nodes, **no** cluster security (open `asinfo`) |
| [`manifests/aerospikecluster-security.yaml`](manifests/aerospikecluster-security.yaml) | ACL + `security: {}` for `auth` test |
| [`manifests/aerospikecluster-size1.yaml`](manifests/aerospikecluster-size1.yaml) | `spec.size: 1` |
| [`manifests/aerospikecluster-size2.yaml`](manifests/aerospikecluster-size2.yaml) | `spec.size: 2` |
| [`manifests/pvc-workdir-preprovision.yaml`](manifests/pvc-workdir-preprovision.yaml) | Pre-create workdir PVCs for timing seeding (namespaced/cluster substituted by script) |
| [`manifests/aerospikecluster-timing.yaml`](manifests/aerospikecluster-timing.yaml) | 3-node cluster with `initMethod: none` for large-SMD timing |

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASD_BINARY` | — | Host path to EE `asd` (for `build-load-server-image.sh`) |
| `ASD_IMAGE` | `smd-sync-asd-enterprise:8.1.2.0-dev` | Server image ref (AKO: `enterprise`\|`federal` in name; dotted version in tag) |
| `KIND_CLUSTER_NAME` | `smd-sync-ako` | kind cluster name |
| `NAMESPACE` | `smd-sync-ako` | Namespace for CR / pods |
| `CLUSTER_NAME` | `smdsync` | `metadata.name` of `AerospikeCluster` |
| `OPERATOR_IMG` | — | Operator controller image for `install-operator.sh` |
| `AKO_REPO` | auto-guess | Path to operator git clone |
| `OPERATOR_NAMESPACE` | `aerospike` | Operator/Certificate namespace (AKO default overlay) |
| `OPERATOR_WATCH_NAMESPACES` | `aerospike,smd-sync-ako` | Comma list for operator `WATCH_NAMESPACE` (must include your `AerospikeCluster` namespace) |
| `SERVING_CERT_NAME` | `aerospike-operator-serving-cert` | cert-manager `Certificate` for webhook TLS |
| `FEATURES_CONF` | — | Host path for `create-secrets.sh` |
| `ADMIN_PASSWORD` | `admin123` | Admin password in `auth-secret` / `auth` test |
| `TIMEOUT` | `300` | Wait timeouts (seconds) |
| `CLEANUP_ON_SUCCESS` | `false` | If `true`, `all` deletes CR/PVCs after success |
| `SMD_DATA_DIR` | `/tmp/smd-timing-k8s-data` | Host staging for generated `.smd` before `kubectl cp` |
| `TIMING_RESULTS_DIR` | `<this-dir>/timing-results-k8s` | TSV + phase logs from `timing` / `timing-rejoin` |
| `TIMING_ITEMS` | (same sweep as Docker `test-smd-sync.sh`) | Space-separated counts for `timing` |
| `TIMING_VALUE_SIZE` | `200` | Bytes per synthetic value (`timing`) |
| `TIMING_MODULE` | `evict` | SMD module for synthetic payload (`timing`) |
| `TIMING_CLUSTER_TIMEOUT` | `300` | Seconds to wait for `cluster_size=3` (`timing`) |
| `TIMING_REJOIN_CLUSTER_TIMEOUT` | `600` | Same for `timing-rejoin` (heavier dataset) |
| `TIMING_REJOIN_STALE_PCT` | `80` | Stale fraction on ordinal 2 pod’s PVC (`timing-rejoin`) |
| `TIMING_REJOIN_SECURITY_ITEMS` | `100000` | Security module item count (`timing-rejoin`) |
| `TIMING_AC_MANIFEST` | `manifests/aerospikecluster-timing.yaml` | Override only if you fork the timing cluster spec |
| `TIMING_SEED_POD_TIMEOUT` | `300` | Seconds to wait for each PVC seed pod to schedule + become Ready |
| `TIMING_PVC_SETTLE_SEC` | `5` | Brief sleep after PVC objects exist (`WaitForFirstConsumer` binds per seed pod, not all upfront) |

## Cleanup

```bash
./test-smd-sync-k8s.sh cleanup-full
kubectl delete namespace smd-sync-ako   # optional
```

## Dockerfile context

[`Dockerfile.asd-dev`](Dockerfile.asd-dev) expects build context **`asd-testbeds`** (repository root of `docker/`, `k8s/`, …). `build-load-server-image.sh` passes that path automatically.

## Troubleshooting

### `kubectl` tries `localhost:8080` / connection refused

Your kubeconfig has no usable cluster context. After `./scripts/setup-kind.sh`, your kubeconfig should point at kind; the script runs `kind export kubeconfig` for you.

If you created the cluster earlier or use another terminal:

```bash
kind export kubeconfig --name smd-sync-ako
kubectl cluster-info
```

### `metadata.annotations: Too long: may not be more than 262144 bytes` on CRD apply

Client-side `kubectl apply` embeds the full object in an annotation; Aerospike `AerospikeCluster` CRDs exceed that limit. This harness uses **server-side apply** in `install-operator.sh`. If you apply manifests manually, use `kubectl apply --server-side --force-conflicts -f ...` or avoid client-side apply on those CRDs.

### Pods `Forbidden`: `serviceaccount "aerospike-operator-controller-manager" not found`

Database pods use that **service account name** in the **same namespace as the cluster** (see AKO `internal/controller/cluster/statefulset.go`). It is normally created only in the operator namespace.

[`create-secrets.sh`](scripts/create-secrets.sh) and [`test-smd-sync-k8s.sh`](test-smd-sync-k8s.sh) apply [`manifests/workload-operator-rbac.yaml`](manifests/workload-operator-rbac.yaml) (namespace substituted from `NAMESPACE`). If you apply YAML by hand, run the same sed pipe or re-run those scripts.

Requires ClusterRole `aerospike-operator-manager-role` (default AKO kustomize name); if your install renames it, edit the manifest’s `roleRef.name`.

### Init container `aerospike-init` `CrashLoopBackOff` / `Init:Error`

The workload manifest also applies a **namespace `Role` + `RoleBinding`** for leader election (`leases`, `configmaps`, `events`). AKO’s `ClusterRole` alone does not include `coordination.k8s.io/leases`; leader-election rules normally exist only in the **operator** namespace, so init pods in `smd-sync-ako` need the mirrored Role from [`manifests/workload-operator-rbac.yaml`](manifests/workload-operator-rbac.yaml).

Confirm with:

```bash
kubectl logs -n smd-sync-ako smdsync-0-0 -c aerospike-init --tail=80
```

Look for Kubernetes API **403** on `leases`, `configmaps`, or **`nodes`** (init lists nodes at cluster scope; [`workload-operator-rbac.yaml`](manifests/workload-operator-rbac.yaml) adds a small `ClusterRole` + binding for that).

### `AerospikeCluster` exists but no StatefulSet / pods / events in `smd-sync-ako`

The controller **only reconciles namespaces listed in its `WATCH_NAMESPACE` env** (AKO’s default manifest uses `aerospike` only). Clusters in `smd-sync-ako` are ignored until that list includes `smd-sync-ako`.

[`scripts/install-operator.sh`](scripts/install-operator.sh) applies **`WATCH_NAMESPACE=aerospike,smd-sync-ako`** after install. If you installed the operator another way, run:

```bash
kubectl set env deployment/aerospike-operator-controller-manager -n aerospike \
  WATCH_NAMESPACE=aerospike,smd-sync-ako
kubectl rollout status deployment/aerospike-operator-controller-manager -n aerospike --timeout=300s
```

Confirm with `kubectl describe deploy aerospike-operator-controller-manager -n aerospike | grep WATCH_NAMESPACE`.

### Webhook `x509: certificate signed by unknown authority`

Upstream `config/default` leaves `cert-manager.io/inject-ca-from: $(CERTIFICATE_NAMESPACE)/$(CERTIFICATE_NAME)` **unexpanded** under Kustomize v5, so cert-manager never injects `caBundle` into the Mutating/ValidatingWebhookConfiguration.

[`scripts/install-operator.sh`](scripts/install-operator.sh) rewrites that annotation to **`aerospike/aerospike-operator-serving-cert`** before apply and waits for that Certificate to be **Ready**. Re-run:

```bash
./scripts/install-operator.sh
```

### Operator pods `ImagePullBackOff` / `docker.io/... not found` for `:local-dev`

The Deployment references an image that exists on your **host Docker**, not on **Docker Hub**. kind nodes use their **own** image store.

1. Load the image (match cluster name from `kind get clusters`, usually `smd-sync-ako`):

   ```bash
   kind load docker-image aerospike/aerospike-kubernetes-operator:local-dev --name smd-sync-ako
   ```

2. Restart the controller pods so they retry with the local image:

   ```bash
   kubectl delete pods -n aerospike -l control-plane=controller-manager
   ```

Use a published operator image in `OPERATOR_IMG` instead if you prefer pulls from a registry.

### `no matches for kind \"AerospikeCluster\"` / missing CRD

The controller was not deployed (or deploy failed) so CRDs are absent. Run `./scripts/install-operator.sh` with `OPERATOR_IMG` set, then:

```bash
kubectl get crd aerospikeclusters.asdb.aerospike.com
```

### `docker build` for the operator: `pattern schemas/json/aerospike: no matching files found`

Initialize the operator repo submodule (schemas live under `pkg/configschema/schemas`):

```bash
cd /path/to/aerospike-kubernetes-operator
git submodule update --init --recursive
```

Confirm `pkg/configschema/schemas/json/aerospike` exists, then rebuild the image.

### Timing: seed pod stuck `Pending` / `kubectl exec`: pod does not have a host assigned

RWO volumes on kind often spend a while in **Pending** before the scheduler assigns a node and the volume attaches. The script waits up to **`TIMING_SEED_POD_TIMEOUT`** (default 300s) and prints progress every 30s; increase it if your cluster is slow.

If **`kubectl get pvc`** shows ordinals **1–2** stuck **Pending** while ordinal **0** is Bound, that is normal under **`WaitForFirstConsumer`**: those volumes bind only when the corresponding seed pod schedules (the harness never waited for all three Bound).

After an interrupted run, delete leftover seed pods:

```bash
kubectl delete pods -n smd-sync-ako -l smd-sync-test=seed --ignore-not-found
```
