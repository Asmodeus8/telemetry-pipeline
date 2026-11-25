#!/usr/bin/env bash
# Narrows down why Mosquitto can't open its ConfigMap-mounted config on this host.
set -u
ns=telemetry
run() { # name image user script
  kubectl -n $ns run "probe-$1" --restart=Never --image="$2" --overrides="{
    \"spec\": {
      \"securityContext\": {\"runAsUser\": $3},
      \"containers\": [{
        \"name\": \"m\", \"image\": \"$2\",
        \"command\": [\"sh\", \"-c\", \"$4\"],
        \"volumeMounts\": [{\"name\": \"cfg\", \"mountPath\": \"/config\"}]
      }],
      \"volumes\": [{\"name\": \"cfg\", \"configMap\": {\"name\": \"telemetry-mqtt\"}}]
    }}" >/dev/null
}
M='timeout 5 /usr/sbin/mosquitto -c'
run v2-0-23  eclipse-mosquitto:2.0.23     1883 "$M /config/mosquitto.conf; echo exit=\$?"
run v2-1-4   eclipse-mosquitto:2.1.4-alpine 1883 "$M /config/mosquitto.conf; echo exit=\$?"
run nanomq   emqx/nanomq:0.25.7-slim      0    "timeout 5 nanomq start; echo exit=\$?"
sleep 30
for p in v2-0-23 v2-1-4 nanomq; do
  echo "=== $p"
  kubectl -n $ns logs "probe-$p" 2>&1 | grep -vE "^\s*$" | head -4
done
uname -a
