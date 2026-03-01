#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: ./scripts/compat.sh <regen|check|all> [compattool options]

Examples:
  ./scripts/compat.sh regen
  ./scripts/compat.sh check -smoke
  ./scripts/compat.sh all -out testdata
EOF
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

if [[ ! -f "${repo_root}/go.mod" ]]; then
  echo "compat.sh: could not locate repository root from ${script_dir}" >&2
  exit 1
fi

command="$1"
shift

case "$command" in
  regen|check|all)
    (
      cd "${repo_root}"
      go run ./cmd/compattool "$command" "$@"
    )
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac
