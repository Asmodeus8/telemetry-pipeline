#!/usr/bin/env bash
# Creates the kind cluster, the two secrets Git must never hold, and Argo CD.
# From then on Argo CD installs everything else from this repository.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=telemetry
ARGOCD_CHART=10.19.1

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/cluster.yaml --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

for ns in argocd telemetry monitoring; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

random() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "${1:-32}"; }

if ! kubectl -n telemetry get secret telemetry-db >/dev/null 2>&1; then
  pw=$(random 32)
  kubectl -n telemetry create secret generic telemetry-db \
    --from-literal=POSTGRES_PASSWORD="$pw" \
    --from-literal=DATABASE_URL="postgres://pipeline:$pw@telemetry-db:5432/telemetry?sslmode=disable"
fi
if ! kubectl -n monitoring get secret grafana-admin >/dev/null 2>&1; then
  kubectl -n monitoring create secret generic grafana-admin \
    --from-literal=admin-user=admin --from-literal=admin-password="$(random 20)"
fi

helm upgrade --install argocd argo-cd --repo https://argoproj.github.io/argo-helm \
  --version "$ARGOCD_CHART" -n argocd -f deploy/argocd/argocd-values.yaml --wait --timeout 5m
kubectl apply -f deploy/argocd/root.yaml

argo_pw=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d)
grafana_pw=$(kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d)
cat <<EOF

Argo CD is installing the rest. Watch it at http://argocd.localhost (first sync takes a few minutes).

  Telemetry   http://telemetry.localhost
  Argo CD     http://argocd.localhost      admin / $argo_pw
  Grafana     http://grafana.localhost     admin / $grafana_pw
  Prometheus  http://prometheus.localhost
  MQTT        localhost:1884
EOF
