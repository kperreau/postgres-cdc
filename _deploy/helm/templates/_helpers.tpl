{{/*
Expand the name of the chart.
*/}}
{{- define "postgres-cdc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this
(by the DNS naming spec).
*/}}
{{- define "postgres-cdc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label.
*/}}
{{- define "postgres-cdc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "postgres-cdc.labels" -}}
helm.sh/chart: {{ include "postgres-cdc.chart" . }}
{{ include "postgres-cdc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (subset used for Service / Deployment selectors).
*/}}
{{- define "postgres-cdc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "postgres-cdc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "postgres-cdc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "postgres-cdc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference (repository:tag).
*/}}
{{- define "postgres-cdc.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Name of the Secret holding the PostgreSQL password.
Returns the existingSecret name when set, otherwise the chart-managed secret name.
*/}}
{{- define "postgres-cdc.pgSecretName" -}}
{{- if .Values.postgres.existingSecret }}
{{- .Values.postgres.existingSecret }}
{{- else }}
{{- printf "%s-pg" (include "postgres-cdc.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Name of the PVC used for checkpoint persistence.
*/}}
{{- define "postgres-cdc.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-checkpoint" (include "postgres-cdc.fullname" .) }}
{{- end }}
{{- end }}

{{/*
ConfigMap name.
*/}}
{{- define "postgres-cdc.configmapName" -}}
{{- printf "%s-config" (include "postgres-cdc.fullname" .) }}
{{- end }}
