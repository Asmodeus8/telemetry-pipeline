#!/usr/bin/env bash
# Starts Mosquitto with the chart's config under one security setting at a time,
# to find which one stops it from reading its config on this host.
set -u
ns=telemetry
img=$(kubectl -n $ns get deploy telemetry-mqtt -o jsonpath='{.spec.template.spec.containers[0].image}')
declare -A ctx=(
  [none]='{}'
  [seccomp]='{"seccompProfile":{"type":"RuntimeDefault"}}'
  [dropcaps]='{"capabilities":{"drop":["ALL"]}}'
  [readonly]='{"readOnlyRootFilesystem":true}'
  [noescalate]='{"allowPrivilegeEscalation":false}'
)
for name in "${!ctx[@]}"; do
  kubectl -n $ns run "probe-$name" --restart=Never --image="$img" --overrides="{
    \"spec\": {
      \"securityContext\": {\"runAsUser\": 1883, \"runAsGroup\": 1883},
      \"containers\": [{
        \"name\": \"m\", \"image\": \"$img\",
        \"command\": [\"sh\", \"-c\", \"timeout 5 /usr/sbin/mosquitto -c /config/mosquitto.conf; echo exit=\$?\"],
        \"securityContext\": ${ctx[$name]},
        \"volumeMounts\": [{\"name\": \"cfg\", \"mountPath\": \"/config\"}]
      }],
      \"volumes\": [{\"name\": \"cfg\", \"configMap\": {\"name\": \"telemetry-mqtt\"}}]
    }}" >/dev/null
done
sleep 20
for name in "${!ctx[@]}"; do
  echo "=== $name"
  kubectl -n $ns logs "probe-$name" 2>&1 | grep -E "Error|exit=|Opening|running" | head -4
done
