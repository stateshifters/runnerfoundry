#!/usr/bin/env bash
set -euo pipefail

postgres_root="${E2E_POSTGRES_ROOT:-$(pwd)/build/e2e/postgres}"
backend="${E2E_BACKEND:-all}"
requested_versions="${E2E_POSTGRES_VERSION:-17.5.0 18.3.0}"
go_operating_system="$(go env GOOS)"
go_architecture="$(go env GOARCH)"
read -r -a e2e_packages <<< "${E2E_PACKAGES:-./internal/daemon}"

normalize_version() {
  case "$1" in
    17|17.5|17.5.0) printf '%s\n' '17.5.0' ;;
    18|18.3|18.3.0) printf '%s\n' '18.3.0' ;;
    *)
      echo "unsupported E2E PostgreSQL version: $1 (supported: 17, 18)" >&2
      return 1
      ;;
  esac
}

run_e2e() {
  go test -tags=e2e "${e2e_packages[@]}" -count=1 -timeout=10m
}

run_sqlite() {
  echo 'Running product-contract E2E tests with SQLite'
  E2E_BACKEND=sqlite run_e2e
}

run_postgres() {
  local version="$1"
  local binaries="${postgres_root}/${version}/${go_operating_system}-${go_architecture}"
  local executable='pg_ctl'
  if [[ "$go_operating_system" == 'windows' ]]; then
    executable='pg_ctl'
  fi

  if [[ ! -x "${binaries}/bin/${executable}" ]]; then
    echo "PostgreSQL ${version} binaries are not prepared at ${binaries}" >&2
    echo "Run: make prepare-e2e E2E_POSTGRES_VERSION=${version}" >&2
    return 1
  fi

  echo "Running product-contract E2E tests with PostgreSQL ${version}"
  E2E_BACKEND=postgres \
    E2E_POSTGRES_VERSION="$version" \
    E2E_POSTGRES_BINARIES="$binaries" \
    run_e2e
}

case "$backend" in
  sqlite)
    run_sqlite
    ;;
  postgres)
    for requested_version in ${requested_versions//,/ }; do
      normalized_version="$(normalize_version "$requested_version")"
      run_postgres "$normalized_version"
    done
    ;;
  all|'')
    run_sqlite
    for requested_version in ${requested_versions//,/ }; do
      normalized_version="$(normalize_version "$requested_version")"
      run_postgres "$normalized_version"
    done
    ;;
  *)
    echo "unsupported E2E_BACKEND: ${backend} (supported: all, sqlite, postgres)" >&2
    exit 1
    ;;
esac
