{{- define "adapter.labels" -}}
app: adapter
app.kubernetes.io/name: adapter
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- define "adapter.env" -}}
- name: PGHOST
  value: {{ .Values.database.host | quote }}
- name: PGPORT
  value: {{ .Values.database.port | quote }}
- name: BROKER_URL
  value: {{ .Values.config.brokerUrl | quote }}
- name: BROKER_RPS
  value: {{ .Values.config.brokerRps | quote }}
- name: BROKER_TIMEOUT_SECONDS
  value: {{ .Values.config.brokerTimeoutSeconds | quote }}
- name: DB_POOL_SIZE
  value: {{ .Values.config.dbPoolSize | quote }}
- name: MAX_ATTEMPTS
  value: {{ .Values.config.maxAttempts | quote }}
- name: RETRY_BASE_SECONDS
  value: {{ .Values.config.retryBaseSeconds | quote }}
- name: RETRY_MAX_SECONDS
  value: {{ .Values.config.retryMaxSeconds | quote }}
- name: LEASE_SECONDS
  value: {{ .Values.config.leaseSeconds | quote }}
- name: POLL_SECONDS
  value: {{ .Values.config.pollSeconds | quote }}
{{- end }}
