{{- define "tripml.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tripml.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "tripml.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "tripml.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "tripml.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tripml
{{- end -}}

{{- define "tripml.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tripml.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "tripml.airflowEnvironment" -}}
- name: AIRFLOW_HOME
  value: /opt/airflow
- name: AIRFLOW_CONFIG
  value: /opt/airflow/config/airflow.cfg
- name: AIRFLOW__CORE__EXECUTOR
  value: LocalExecutor
- name: AIRFLOW__CORE__LOAD_EXAMPLES
  value: "False"
- name: AIRFLOW__CORE__PARALLELISM
  value: "2"
- name: AIRFLOW__CORE__EXECUTION_API_SERVER_URL
  value: http://127.0.0.1:8080/execution/
- name: AIRFLOW__CORE__FERNET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.credentialsSecret }}
      key: airflow-fernet-key
- name: AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS
  value: admin:admin
- name: AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE
  value: /opt/airflow/config/simple-auth-passwords.json
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.credentialsSecret }}
      key: airflow-database-url
- name: AIRFLOW__API_AUTH__JWT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.credentialsSecret }}
      key: airflow-jwt-secret
- name: AIRFLOW__API_AUTH__JWT_AUDIENCE
  value: tripml-airflow
- name: AIRFLOW__API_AUTH__JWT_ISSUER
  value: tripml-airflow
- name: AIRFLOW__API__BASE_URL
  value: http://localhost:8080
- name: AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK
  value: "True"
- name: AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES
  value: "1"
- name: PYTHONDONTWRITEBYTECODE
  value: "1"
- name: TRIPML_INGESTION__DATA_ROOT
  value: /opt/airflow/data
- name: TRIPML_TRAINING__ARTIFACT_ROOT
  value: /opt/airflow/data/artifacts/training
- name: TRIPML_TRACKING__LOCAL_ARTIFACT_ROOT
  value: /opt/airflow/data/artifacts/mlflow/runs
- name: TRIPML_TRACKING__TRACKING_URI
  value: http://{{ include "tripml.fullname" . }}-mlflow:5000
- name: TRIPML_LINEAGE__DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.credentialsSecret }}
      key: lineage-database-url
{{- end -}}

{{- define "tripml.airflowContainerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 50000
runAsGroup: 0
{{- end -}}
