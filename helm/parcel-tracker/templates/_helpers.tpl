{{/*
A teljes név a release nevéből jön (ArgoCD-nél ez az Application neve,
pl. "kafka-demo"), így a szolgáltatásnevek kiszámíthatók:
kafka-demo-api, kafka-demo-kafka, kafka-demo-redis, ...
*/}}
{{- define "parcel-tracker.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Közös címkék minden erőforrásra. */}}
{{- define "parcel-tracker.labels" -}}
app.kubernetes.io/part-of: {{ include "parcel-tracker.fullname" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* A Kafka broker(ek) elérési címe a klaszteren belül. */}}
{{- define "parcel-tracker.kafkaBootstrap" -}}
{{ include "parcel-tracker.fullname" . }}-kafka:9092
{{- end -}}

{{/* A Kafka pod stabil DNS neve (StatefulSet + headless service). */}}
{{- define "parcel-tracker.kafkaPodFqdn" -}}
{{ include "parcel-tracker.fullname" . }}-kafka-0.{{ include "parcel-tracker.fullname" . }}-kafka-headless.{{ .Release.Namespace }}.svc.cluster.local
{{- end -}}
