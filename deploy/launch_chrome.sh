#!/usr/bin/env bash
set -euo pipefail

deploy_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
project_dir="$(cd -- "${deploy_dir}/.." && pwd -P)"
extension_dir="${project_dir}/chrome_extension"
platform_name="$(uname -s)"
if [[ "${platform_name}" == "Darwin" ]]; then
  state_root="${HOME}/Library/Application Support"
else
  state_root="${XDG_STATE_HOME:-${HOME}/.local/state}"
fi
profile_dir="${VARIATIONAL_CHROME_PROFILE_DIR:-${state_root}/var-lit-v1/chrome-profile}"
debug_port="${VARIATIONAL_CHROME_DEBUG_PORT:-9222}"

if [[ -n "${VARIATIONAL_CHROME_BIN:-}" ]]; then
  chrome_bin="${VARIATIONAL_CHROME_BIN}"
else
  chrome_bin=""
  for candidate in \
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    /usr/bin/google-chrome-stable \
    /usr/bin/google-chrome \
    /usr/bin/chromium; do
    if [[ -x "${candidate}" ]]; then
      chrome_bin="${candidate}"
      break
    fi
  done
fi

if [[ -z "${chrome_bin}" || ! -x "${chrome_bin}" ]]; then
  echo "Chrome executable not found; set VARIATIONAL_CHROME_BIN" >&2
  exit 1
fi
if [[ "${platform_name}" != "Darwin" && -z "${DISPLAY:-}" ]]; then
  echo "DISPLAY is not set; start Xvfb before Chrome" >&2
  exit 1
fi
if [[ ! -f "${extension_dir}/manifest.json" ]]; then
  echo "Project extension is missing: ${extension_dir}/manifest.json" >&2
  exit 1
fi

mkdir -p "${profile_dir}"
chmod 700 "${profile_dir}"

chrome_args=(
  "--user-data-dir=${profile_dir}" \
  "--remote-debugging-address=127.0.0.1" \
  "--remote-debugging-port=${debug_port}" \
  --no-first-run \
  --no-default-browser-check \
  --disable-background-timer-throttling \
  --disable-renderer-backgrounding \
  --disable-backgrounding-occluded-windows \
  --window-size=1280,900
)

chrome_name="$(basename "${chrome_bin}")"
if [[ "${chrome_name}" == "Google Chrome" || "${chrome_name}" == google-chrome* ]]; then
  echo "Official Google Chrome requires one-time manual Load unpacked for ${extension_dir}" >&2
else
  chrome_args+=("--load-extension=${extension_dir}")
fi

exec "${chrome_bin}" \
  "${chrome_args[@]}" \
  https://omni.variational.io/perpetual/BTC \
  "$@"
