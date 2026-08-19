#!/usr/bin/env bash
set -euo pipefail

postgres_root="${E2E_POSTGRES_ROOT:-$(pwd)/build/e2e/postgres}"
requested_versions="${E2E_POSTGRES_VERSION:-17.5.0 18.3.0}"
repository_url="https://repo1.maven.org/maven2"

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

expected_checksum() {
  case "$1|$2|$3" in
    '17.5.0|linux|amd64') printf '%s\n' '646f3269075d1a23904127374e3cdbcef96beb32979fedee25d835877400fcbe' ;;
    '17.5.0|linux|arm64v8') printf '%s\n' 'e02f27ff50ff2d90b6541d54f3f108505aacd84ea53c2890e976a684bfea2752' ;;
    '17.5.0|linux|amd64-alpine') printf '%s\n' '7122386c7f7a089568f05720ada63448093d4065abbc0a89e6dc660851a53e3c' ;;
    '17.5.0|linux|arm64v8-alpine') printf '%s\n' 'f9cf1d22dd98bfdb63738846c330f2b9f18adc39c927b3a082d0ccbe51a0efa1' ;;
    '17.5.0|darwin|amd64') printf '%s\n' 'fb70f783928e77c3d6ef5ca8ed644f1ac502f740fcfc032a375c8347820f2bdd' ;;
    '17.5.0|windows|amd64') printf '%s\n' '06ced3b3267c569ffb3f85544aac0075926dd81bf78bd5247abb1aa9fe994d6b' ;;
    '18.3.0|linux|amd64') printf '%s\n' '1974c3a59c3ae010e9818e81637e425aad94eaac26e4ef533b42e081fe7ca345' ;;
    '18.3.0|linux|arm64v8') printf '%s\n' '1fb555a6602c13a113ab1f0cc9224d599948e85e51ac5f001393605505eaf78e' ;;
    '18.3.0|linux|amd64-alpine') printf '%s\n' '510fd314da39f1eeccef774a92428474168fedf55403342548e2adeaed66b483' ;;
    '18.3.0|linux|arm64v8-alpine') printf '%s\n' 'e8204cd252eced653bacf680f7f0f3db2d71fb87186ffeaa97dddb4c9b9697d6' ;;
    '18.3.0|darwin|amd64') printf '%s\n' 'cb68c57232945f5e74db946e6077d70cec56bfed35959b21acc750cc80ca0977' ;;
    '18.3.0|darwin|arm64v8') printf '%s\n' 'a18ec9ec6a905e04f5a08a8f7870bddadd635feb54dee332a3daf80fada3cb1a' ;;
    '18.3.0|windows|amd64') printf '%s\n' 'e1e2bb484f775c0b70fbde39c69451a7a4925629a85d622f557fe6f60de37ba1' ;;
    *)
      echo "no pinned checksum for PostgreSQL artifact $1/$2/$3" >&2
      return 1
      ;;
  esac
}

ensure_windows_pg_ctl_launcher() {
  local operating_system="$1"
  local destination="$2"

  if [[ "$operating_system" == 'windows' && ! -f "${destination}/bin/pg_ctl" && -f "${destination}/bin/pg_ctl.exe" ]]; then
    cp "${destination}/bin/pg_ctl.exe" "${destination}/bin/pg_ctl"
  fi
}

validate_installation() {
  local operating_system="$1"
  local destination="$2"
  local executable_suffix=''
  local share_directory='share/postgresql'
  if [[ "$operating_system" == 'windows' ]]; then
    executable_suffix='.exe'
    share_directory='share'
  fi

  local required_path
  for required_path in \
    "bin/pg_ctl${executable_suffix}" \
    "bin/initdb${executable_suffix}" \
    "bin/postgres${executable_suffix}" \
    "${share_directory}/postgres.bki" \
    "${share_directory}/postgresql.conf.sample"; do
    if [[ ! -f "${destination}/${required_path}" ]]; then
      return 1
    fi
  done

  if [[ "$operating_system" == 'windows' && ! -f "${destination}/bin/pg_ctl" ]]; then
    return 1
  fi
  if [[ "$operating_system" != 'windows' ]]; then
    for required_path in 'bin/pg_ctl' 'bin/initdb' 'bin/postgres'; do
      if [[ ! -x "${destination}/${required_path}" ]]; then
        return 1
      fi
    done
  fi
}

artifact_architecture() {
  local operating_system="$1"
  local architecture="$2"
  local version="$3"

  case "${operating_system}/${architecture}" in
    linux/amd64) printf '%s\n' 'amd64' ;;
    linux/arm64) printf '%s\n' 'arm64v8' ;;
    darwin/amd64) printf '%s\n' 'amd64' ;;
    darwin/arm64)
      if [[ "$version" == 17.* ]]; then
        printf '%s\n' 'amd64'
      else
        printf '%s\n' 'arm64v8'
      fi
      ;;
    windows/amd64) printf '%s\n' 'amd64' ;;
    *)
      echo "unsupported E2E PostgreSQL platform: ${operating_system}/${architecture}" >&2
      return 1
      ;;
  esac
}

checksum_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi

  shasum -a 256 "$1" | awk '{print $1}'
}

prepare_version() (
  local version="$1"
  local operating_system="$2"
  local go_architecture="$3"
  local artifact_arch
  artifact_arch="$(artifact_architecture "$operating_system" "$go_architecture" "$version")"

  if [[ "$operating_system" == 'linux' && -f /etc/alpine-release ]]; then
    artifact_arch="${artifact_arch}-alpine"
  fi

  local destination="${postgres_root}/${version}/${operating_system}-${go_architecture}"
  ensure_windows_pg_ctl_launcher "$operating_system" "$destination"
  if validate_installation "$operating_system" "$destination"; then
    echo "PostgreSQL ${version} binaries already prepared at ${destination}"
    return
  fi

  local artifact="embedded-postgres-binaries-${operating_system}-${artifact_arch}-${version}"
  local download_url="${repository_url}/io/zonky/test/postgres/embedded-postgres-binaries-${operating_system}-${artifact_arch}/${version}/${artifact}.jar"
  local temporary_directory
  temporary_directory="$(mktemp -d)"
  local destination_parent="${postgres_root}/${version}"
  mkdir -p "$destination_parent"
  local staging_directory
  staging_directory="$(mktemp -d "${destination}.prepare.XXXXXX")"
  local replaced_directory=''
  cleanup_preparation() {
    rm -rf -- "$temporary_directory"
    if [[ -n "$staging_directory" ]]; then
      rm -rf -- "$staging_directory"
    fi
    if [[ -n "$replaced_directory" && ! -e "$destination" ]]; then
      mv -- "$replaced_directory" "$destination"
      replaced_directory=''
    fi
  }
  trap cleanup_preparation EXIT

  echo "Preparing PostgreSQL ${version} binaries for ${operating_system}/${go_architecture}"
  curl --fail --location --silent --show-error --connect-timeout 15 --max-time 300 \
    --output "${temporary_directory}/postgres.jar" "$download_url"

  local expected_checksum
  expected_checksum="$(expected_checksum "$version" "$operating_system" "$artifact_arch")"
  local actual_checksum
  actual_checksum="$(checksum_file "${temporary_directory}/postgres.jar" | tr '[:upper:]' '[:lower:]')"
  if [[ "$actual_checksum" != "$expected_checksum" ]]; then
    echo "checksum mismatch for PostgreSQL ${version} binary archive" >&2
    return 1
  fi

  unzip -p "${temporary_directory}/postgres.jar" '*.txz' > "${temporary_directory}/postgres.txz"
  tar -xJf "${temporary_directory}/postgres.txz" -C "$staging_directory"
  ensure_windows_pg_ctl_launcher "$operating_system" "$staging_directory"

  if ! validate_installation "$operating_system" "$staging_directory"; then
    echo "prepared PostgreSQL ${version} archive is incomplete" >&2
    return 1
  fi

  if [[ -e "$destination" ]]; then
    replaced_directory="${destination}.invalid.$$.$RANDOM"
    mv -- "$destination" "$replaced_directory"
  fi
  mv -- "$staging_directory" "$destination"
  staging_directory=''
  if [[ -n "$replaced_directory" ]]; then
    rm -rf -- "$replaced_directory"
    replaced_directory=''
  fi

  echo "Prepared PostgreSQL ${version} binaries at ${destination}"
)

go_operating_system="$(go env GOOS)"
go_architecture="$(go env GOARCH)"
for requested_version in ${requested_versions//,/ }; do
  normalized_version="$(normalize_version "$requested_version")"
  prepare_version "$normalized_version" "$go_operating_system" "$go_architecture"
done
