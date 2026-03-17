# postgres-cdc — Déploiement Kubernetes

Ce dossier contient le chart Helm pour déployer `postgres-cdc` en production.

```
_deploy/
├── helm/
│   ├── Chart.yaml
│   ├── values.yaml          ← valeurs par défaut (à surcharger)
│   └── templates/
│       ├── _helpers.tpl     ← macros Helm
│       ├── configmap.yaml   ← config.yaml injecté dans le pod
│       ├── deployment.yaml  ← Deployment + PVC optionnel
│       ├── keda.yaml        ← ScaledObject KEDA (optionnel)
│       ├── rbac.yaml        ← Role/RoleBinding pour leader election
│       ├── service.yaml     ← ClusterIP health + metrics
│       └── serviceaccount.yaml
└── readme.md
```

---

## Prérequis

| Outil | Version minimale |
|-------|-----------------|
| Kubernetes | 1.25+ |
| Helm | 3.12+ |
| KEDA (optionnel) | 2.12+ |

---

## Installation rapide

### 1. Créer le Secret PostgreSQL

Le mot de passe ne doit **jamais** figurer dans la ConfigMap ni dans values.yaml.
Créer un Secret Kubernetes contenant la clé `password` :

```bash
kubectl create secret generic postgres-cdc-pg \
  --namespace=my-namespace \
  --from-literal=password='changeme'
```

Ou via un gestionnaire de secrets (Vault, External Secrets Operator, Sealed Secrets) :

```yaml
# example avec External Secrets Operator
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: postgres-cdc-pg
spec:
  secretStoreRef:
    name: vault-backend
    kind: SecretStore
  target:
    name: postgres-cdc-pg
  data:
    - secretKey: password
      remoteRef:
        key: secret/postgres-cdc
        property: password
```

### 2. Créer un fichier values de production

```yaml
# values-prod.yaml
postgres:
  host: postgres.my-namespace.svc.cluster.local
  port: 5432
  user: cdc
  dbname: app
  existingSecret: postgres-cdc-pg   # Secret créé à l'étape 1

redpanda:
  brokers:
    - redpanda-0.redpanda.redpanda.svc.cluster.local:9093
  compression: snappy
  topicAutoCreate:
    partitions: 3
    replicationFactor: 3

replication:
  slotName: cdc_slot
  publicationName: cdc_pub

persistence:
  enabled: true
  storageClass: fast-ssd
  size: 256Mi

resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi

logging:
  level: info
```

### 3. Installer le chart

```bash
helm upgrade --install postgres-cdc ./helm \
  --namespace my-namespace \
  --create-namespace \
  --values values-prod.yaml \
  --wait
```

---

## Configuration

### Paramètres clés

| Paramètre | Description | Défaut |
|-----------|-------------|--------|
| `postgres.host` | Hôte PostgreSQL | `""` (obligatoire) |
| `postgres.existingSecret` | Nom du Secret K8s contenant `password` | `""` |
| `redpanda.brokers` | Liste des brokers | `[]` (obligatoire) |
| `replication.slotName` | Nom du slot de réplication | `cdc_slot` |
| `replication.publicationName` | Nom de la publication PostgreSQL | `cdc_pub` |
| `topic.mode` | `per_table` ou `single` | `per_table` |
| `topic.prefix` | Préfixe des topics (`{prefix}.{db}.{schema}.{table}`) | `cdc` |
| `persistence.enabled` | PVC pour le checkpoint | `false` |
| `leaderElection.enabled` | Leader election via K8s Lease | `false` |
| `keda.enabled` | ScaledObject KEDA | `false` |

### Variables d'environnement

Toutes les valeurs de `config.yaml` peuvent être surchargées via des variables d'environnement préfixées `CDC_` avec `__` comme séparateur :

```
CDC_POSTGRES__HOST=db.example.com
CDC_POSTGRES__PASSWORD=secret
CDC_LOGGING__LEVEL=debug
CDC_REPLICATION__SLOT_NAME=my_slot
```

Injectez des variables supplémentaires via `extraEnv` :

```yaml
extraEnv:
  - name: CDC_LOGGING__LEVEL
    value: debug
  - name: CDC_REDPANDA__COMPRESSION
    value: zstd
```

---

## Persistence du checkpoint

Sans persistence, le checkpoint est perdu au redémarrage du pod. Le service reprend alors depuis le début du slot de réplication (plus de données à rejouer, pas de perte).

**Avec persistence activée** (`persistence.enabled: true`), un PVC est créé automatiquement pour stocker le fichier checkpoint.

**PVC existant** :

```yaml
persistence:
  enabled: true
  existingClaim: my-existing-pvc
```

---

## Haute disponibilité (actif/passif)

Le service ne peut avoir **qu'une seule instance active** par slot de réplication.
Le Deployment utilise `strategy: Recreate` pour éviter deux pods actifs simultanément.

Pour des environnements multi-répliques avec failover automatique, activer le leader election via Kubernetes Lease :

```yaml
replicaCount: 2
leaderElection:
  enabled: true
  leaseName: postgres-cdc-leader
```

Cela crée automatiquement le `Role` et `RoleBinding` nécessaires pour accéder à l'API `coordination.k8s.io/leases`.

---

## KEDA (scale-to-zero)

KEDA permet de scaler le pod vers 0 en environnement non-prod (nuit, weekend), et de le relancer automatiquement quand du lag est détecté.

```yaml
keda:
  enabled: true
  minReplicaCount: 0    # scale-to-zero autorisé
  maxReplicaCount: 1    # jamais plus d'un pod actif
  prometheus:
    serverAddress: http://prometheus-operated.monitoring.svc.cluster.local:9090
    query: cdc_commit_lag_seconds{namespace="my-namespace"}
    threshold: "30"           # scaler si lag > 30s
    activationThreshold: "0"  # scaler depuis 0 dès qu'il y a du lag
```

> **Note** : `maxReplicaCount` doit rester à `1`. Augmenter cette valeur provoquerait des conflits sur le slot de réplication PostgreSQL.

---

## Health probes

| Endpoint | Port | Description |
|----------|------|-------------|
| `GET /livez` | 8080 | Liveness — le processus est vivant |
| `GET /readyz` | 8080 | Readiness — connecté à PG et Redpanda, lag acceptable |
| `GET /metrics` | 9090 | Prometheus metrics |

---

## Métriques Prometheus clés

| Métrique | Type | Description |
|----------|------|-------------|
| `cdc_commit_lag_seconds` | Gauge | Délai entre commit PG et publication |
| `cdc_events_total` | Counter | Événements publiés par type d'opération |
| `cdc_last_checkpoint_lsn` | Gauge | Dernier LSN checkpointé |
| `cdc_queue_depth` | Gauge | Profondeur de la queue interne |
| `cdc_backpressure_seconds` | Counter | Temps bloqué en backpressure |
| `cdc_reconnects_total` | Counter | Reconnexions PostgreSQL |
| `cdc_publish_retries_total` | Counter | Tentatives de retry Redpanda |

---

## Mise à jour

```bash
helm upgrade postgres-cdc ./helm \
  --namespace my-namespace \
  --values values-prod.yaml \
  --wait
```

La stratégie `Recreate` arrête le pod existant avant d'en démarrer un nouveau.
La durée de downtime est typiquement < 10 secondes (shutdown gracieux 30s max).
