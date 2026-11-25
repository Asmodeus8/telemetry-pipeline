{{- define "tp.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- else if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "tp.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "tp.tag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* call with (dict "ctx" $ "component" "ingest") */}}
{{- define "tp.selector" -}}
app.kubernetes.io/name: {{ .ctx.Chart.Name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "tp.tag" -}}
{{- .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{- define "tp.image" -}}
{{- printf "%s:%s" .Values.image.repository (include "tp.tag" .) -}}
{{- end -}}

{{- define "tp.dbSecret" -}}
{{- .Values.database.existingSecret | default (printf "%s-db" (include "tp.fullname" .)) -}}
{{- end -}}

{{- define "tp.mqttUrl" -}}
{{- if .Values.mqtt.internal -}}
{{- printf "tcp://%s-mqtt:1883" (include "tp.fullname" .) -}}
{{- else -}}
{{- required "mqtt.url is required when mqtt.internal is false" .Values.mqtt.url -}}
{{- end -}}
{{- end -}}
