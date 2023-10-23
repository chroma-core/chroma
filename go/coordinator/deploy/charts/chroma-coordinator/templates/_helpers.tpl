{{/*
Coordinator labels
*/}}
{{- define "chroma-cluster.coordinator.labels" -}}
{{ include "chroma-cluster.coordinator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: chroma
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Coordinator selector labels
*/}}
{{- define "chroma-cluster.coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: coordinator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
