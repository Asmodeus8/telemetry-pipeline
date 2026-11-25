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
run cat        eclipse-mosquitto:2.0.22 1883 "head -c 40 /config/mosquitto.conf && echo && echo cat-ok"
run copy       eclipse-mosquitto:2.0.22 1883 "cp /config/mosquitto.conf /tmp/m.conf && $M /tmp/m.conf; echo exit=\$?"
run realpath   eclipse-mosquitto:2.0.22 1883 "$M \$(readlink -f /config/mosquitto.conf); echo exit=\$?"
run root       eclipse-mosquitto:2.0.22 0    "$M /config/mosquitto.conf; echo exit=\$?"
run v2-0-21    eclipse-mosquitto:2.0.21 1883 "$M /config/mosquitto.conf; echo exit=\$?"
run v2-0-20    eclipse-mosquitto:2.0.20 1883 "$M /config/mosquitto.conf; echo exit=\$?"
sleep 30
for p in cat copy realpath root v2-0-21 v2-0-20; do
  echo "=== $p"
  kubectl -n $ns logs "probe-$p" 2>&1 | grep -vE "^\s*$" | head -4
done
uname -a
