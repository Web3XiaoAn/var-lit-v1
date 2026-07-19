#!/usr/bin/env bash
set -euo pipefail

deploy_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
project_dir="$(cd -- "${deploy_dir}/.." && pwd -P)"
python_bin="${project_dir}/venv/bin/python"

if [[ ! -x "${python_bin}" ]]; then
  echo "Project virtual environment is missing: ${python_bin}" >&2
  exit 1
fi
if [[ ! -f "${project_dir}/.env" ]]; then
  echo "Runtime configuration is missing: ${project_dir}/.env" >&2
  exit 1
fi

runtime_args=("${project_dir}/main.py" --lang "${VARIATIONAL_LANG:-zh}")
if [[ "${VARIATIONAL_SHOW_DASHBOARD:-0}" != "1" ]]; then
  runtime_args+=(--no-dashboard)
fi

cd -- "${project_dir}"
exec "${python_bin}" "${runtime_args[@]}" "$@"
