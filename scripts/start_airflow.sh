#!/usr/bin/env bash
# Start Airflow (scheduler + API/UI) for this project.
# Requires: .venv-airflow installed, ETL .venv for pipeline runs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export AIRFLOW_HOME="${ROOT}/airflow"
# standalone spawns child processes that call `airflow` by name — must be on PATH
export PATH="${ROOT}/.venv-airflow/bin:${PATH}"
exec airflow standalone
