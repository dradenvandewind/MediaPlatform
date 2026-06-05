# OTT Pipeline — Manifests Kubernetes

Générés à partir du `docker-compose.yml` du pipeline OTT.

## Structure

```
k8s-ott/
├── base/
│   ├── 00-namespace-configmap.yaml   # Namespace, ConfigMap commun, Secret
│   ├── 01-postgres.yaml              # PostgreSQL + PVC
│   ├── 02-redis.yaml                 # Redis + PVC
│   ├── 03-minio.yaml                 # MinIO + Job d'init bucket
│   ├── 04-orchestrator.yaml          # Orchestrateur FastAPI
│   ├── 05-workers.yaml               # ingest / transcoder (HPA) / audio / packager
│   ├── 06-live-moq.yaml              # Live MoQ QUIC + Service UDP LoadBalancer
│   ├── 07-nginx.yaml                 # Edge cache + Player + nginx-exporter
│   └── 09-ingress.yaml               # Ingress nginx
└── monitoring/
    └── 08-monitoring.yaml            # Prometheus + Loki + Promtail (DaemonSet) + Grafana
```

## Prérequis

```bash
# nginx ingress controller (bare-metal)
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/baremetal/deploy.yaml

# (optionnel) MetalLB pour les services LoadBalancer sur bare-metal
# https://metallb.universe.tf/installation/

# (optionnel) cert-manager pour TLS automatique
# https://cert-manager.io/docs/installation/
```

## Déploiement

### 1. Builder et pusher tes images

```bash
# Remplacer "your-registry" par ton registry (Docker Hub, Harbor, GHCR…)
REGISTRY=your-registry

docker build -t $REGISTRY/media-orchestrator:latest ./media-orchestrator
docker build -t $REGISTRY/ott-ingest:latest         ./processing_pipeline -f ingest/Dockerfile
docker build -t $REGISTRY/ott-transcoder:latest      ./processing_pipeline -f transcoder/Dockerfile
docker build -t $REGISTRY/ott-audio:latest           ./processing_pipeline -f audio/Dockerfile
docker build -t $REGISTRY/ott-packager:latest        ./processing_pipeline -f packager/Dockerfile
docker build -t $REGISTRY/ott-live-moq:latest        ./processing_pipeline -f live_moq/Dockerfile

docker push $REGISTRY/media-orchestrator:latest
# ... idem pour les autres
```

Mettre à jour les champs `image:` dans les fichiers `04-orchestrator.yaml` et `05-workers.yaml`.

### 2. Adapter les secrets

Editer `00-namespace-configmap.yaml` et remplacer les valeurs par défaut, ou mieux :

```bash
# Utiliser Sealed Secrets ou External Secrets Operator en production
kubectl create secret generic ott-secrets -n ott \
  --from-literal=AWS_ACCESS_KEY_ID=<your-key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-secret> \
  --from-literal=POSTGRES_USER=ott \
  --from-literal=POSTGRES_PASSWORD=<strong-password> \
  --from-literal=POSTGRES_DB=ott \
  --from-literal=GRAFANA_USER=admin \
  --from-literal=GRAFANA_PASSWORD=<strong-password> \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 3. Appliquer les manifests

```bash
# Ordre recommandé
kubectl apply -f base/00-namespace-configmap.yaml
kubectl apply -f base/01-postgres.yaml
kubectl apply -f base/02-redis.yaml
kubectl apply -f base/03-minio.yaml

# Attendre que le Job minio-init soit terminé
kubectl wait --for=condition=complete job/minio-init -n ott --timeout=120s

kubectl apply -f base/04-orchestrator.yaml

# Attendre que l'orchestrateur soit ready
kubectl wait --for=condition=available deployment/orchestrator -n ott --timeout=120s

kubectl apply -f base/05-workers.yaml
kubectl apply -f base/06-live-moq.yaml
kubectl apply -f base/07-nginx.yaml
kubectl apply -f monitoring/08-monitoring.yaml
kubectl apply -f base/09-ingress.yaml
```

Ou en une commande :

```bash
kubectl apply -f base/ -f monitoring/
```

### 4. Vérifier

```bash
kubectl get all -n ott
kubectl get pvc -n ott
kubectl get ingress -n ott

# Logs d'un worker
kubectl logs -n ott -l app=transcoder -f

# Accès à Grafana (port-forward local)
kubectl port-forward svc/grafana 3000:3000 -n ott
```

## Notes importantes

| Sujet | Détail |
|-------|--------|
| **QUIC / MoQ** | Le service `live-moq-quic` est de type `LoadBalancer`. Sur bare-metal, MetalLB est nécessaire pour assigner une IP externe. QUIC (UDP) ne passe pas par l'ingress nginx classique. |
| **Images** | Toutes les images `your-registry/...` doivent être remplacées par tes images réelles. |
| **Grafana dashboards** | Le volume `grafana-provisioning` est vide par défaut. Copier tes fichiers de provisioning dans un ConfigMap ou utiliser un initContainer. |
| **Nginx cache config** | Le ConfigMap `nginx-cache-config` contient une conf minimale. Remplacer par le contenu de ton `nginx/nginx_cache.conf`. |
| **Prometheus config** | Idem, le ConfigMap `prometheus-config` est minimal. Utiliser ton `prometheus.yml` réel. |
| **TLS / certs MoQ** | Les certs pour moq-relay doivent être fournis via un Secret TLS (décommenter le volume dans `06-live-moq.yaml`). |
| **StorageClass** | Les PVCs utilisent la StorageClass par défaut. Préciser `storageClassName:` si ton cluster en a plusieurs. |
