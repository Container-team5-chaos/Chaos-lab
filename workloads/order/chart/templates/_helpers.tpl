{{- define "order.labels" -}}
app: order
app.kubernetes.io/name: order
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "order.selectorLabels" -}}
app: order
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
