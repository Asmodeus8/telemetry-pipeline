# Windows version of bootstrap.sh: cluster, the two secrets Git must never hold, and Argo CD.
# Native tools write warnings to stderr; check exit codes instead of failing on any output.
$ErrorActionPreference = 'Continue'
function Assert-Ok($what) { if ($LASTEXITCODE -ne 0) { throw "$what failed (exit $LASTEXITCODE)" } }
Set-Location (Join-Path $PSScriptRoot '..')

$cluster = 'telemetry'
$argocdChart = '10.19.1'

if (-not ((kind get clusters) -contains $cluster)) {
  kind create cluster --config deploy/kind/cluster.yaml --wait 120s
  Assert-Ok 'kind create cluster'
}
kubectl config use-context "kind-$cluster" | Out-Null

foreach ($ns in 'argocd', 'telemetry', 'monitoring') {
  kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f - | Out-Null
}

function New-Password([int]$n = 32) {
  $chars = [char[]]'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
  $bytes = New-Object byte[] $n
  [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
  -join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] })
}

if (-not (kubectl -n telemetry get secret telemetry-db --ignore-not-found -o name)) {
  $pw = New-Password 32
  kubectl -n telemetry create secret generic telemetry-db `
    "--from-literal=POSTGRES_PASSWORD=$pw" `
    "--from-literal=DATABASE_URL=postgres://pipeline:$pw@telemetry-db:5432/telemetry?sslmode=disable" | Out-Null
}
if (-not (kubectl -n monitoring get secret grafana-admin --ignore-not-found -o name)) {
  kubectl -n monitoring create secret generic grafana-admin `
    --from-literal=admin-user=admin "--from-literal=admin-password=$(New-Password 20)" | Out-Null
}

helm upgrade --install argocd argo-cd --repo https://argoproj.github.io/argo-helm `
  --version $argocdChart -n argocd -f deploy/argocd/argocd-values.yaml --wait --timeout 5m
Assert-Ok 'helm install argocd'
kubectl apply -f deploy/argocd/root.yaml 2>&1 | Out-Null
Assert-Ok 'apply root application'

function Get-SecretValue($ns, $name, $key) {
  $b64 = kubectl -n $ns get secret $name -o "jsonpath={.data.$key}"
  [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64))
}
$argoPw = Get-SecretValue argocd argocd-initial-admin-secret password
$grafanaPw = Get-SecretValue monitoring grafana-admin admin-password

@"

Argo CD is installing the rest. Watch it at http://argocd.localhost (first sync takes a few minutes).

  Telemetry   http://telemetry.localhost
  Argo CD     http://argocd.localhost      admin / $argoPw
  Grafana     http://grafana.localhost     admin / $grafanaPw
  Prometheus  http://prometheus.localhost
  MQTT        localhost:1884
"@
