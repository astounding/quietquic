#!/usr/bin/env bash
set -euo pipefail

network=(--offline)
case "${1:---offline}" in
  --offline) ;;
  --online) network=() ;;
  *) echo 'Usage: scripts/check-packages.sh [--offline|--online]' >&2; exit 2 ;;
esac

repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$repo/target"
work=$(mktemp -d "$repo/target/package-validation.XXXXXX")
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/source"
(cd "$repo" && tar --exclude=.git --exclude=target -cf - .) | tar -xf - -C "$work/source"
cd "$work/source"
version=$(awk -F'"' '/^version = / { print $2; exit }' Cargo.toml)
proto_archive="quietquic-proto-$version"
wrapper_archive="quietquic-$version"

cargo package --allow-dirty --no-verify "${network[@]}" -p quietquic-proto
mkdir -p "$work/proto" "$work/wrapper"
tar -xzf "target/package/$proto_archive.crate" -C "$work/proto"

cargo package --allow-dirty --no-verify "${network[@]}" -p quietquic \
  --config "patch.crates-io.quietquic-proto.path='$work/proto/$proto_archive'"
tar -xzf "target/package/$wrapper_archive.crate" -C "$work/wrapper"

mkdir -p "$work/core-consumer/src" "$work/wrapper-consumer/src"
printf '%s\n' \
  '[package]' 'name="core-package-consumer"' 'version="0.0.0"' 'edition="2021"' \
  '[workspace]' '[dependencies]' \
  "quietquic-proto={path=\"$work/proto/$proto_archive\"}" \
  > "$work/core-consumer/Cargo.toml"
printf '%s\n' \
  'use quietquic_proto::{config::EndpointConfig, endpoint::Endpoint};' \
  'fn main(){assert!(Endpoint::new(EndpointConfig::dial()).unwrap().is_idle());}' \
  > "$work/core-consumer/src/main.rs"

printf '%s\n' \
  '[package]' 'name="wrapper-package-consumer"' 'version="0.0.0"' 'edition="2021"' \
  '[workspace]' '[dependencies]' \
  "quietquic={path=\"$work/wrapper/$wrapper_archive\"}" \
  '[patch.crates-io]' \
  "quietquic-proto={path=\"$work/proto/$proto_archive\"}" \
  > "$work/wrapper-consumer/Cargo.toml"
printf '%s\n' \
  'use quietquic::{Capability, EndpointConfig};' \
  'fn main(){assert_eq!(EndpointConfig::dial().capability, Capability::Dial);}' \
  > "$work/wrapper-consumer/src/main.rs"

CARGO_TARGET_DIR="$work/core-target" cargo check "${network[@]}" --manifest-path "$work/core-consumer/Cargo.toml"
CARGO_TARGET_DIR="$work/wrapper-target" cargo check "${network[@]}" --manifest-path "$work/wrapper-consumer/Cargo.toml"
cargo tree "${network[@]}" --manifest-path "$work/wrapper-consumer/Cargo.toml" -i quietquic-proto

tar -tzf "target/package/$proto_archive.crate"
tar -tzf "target/package/$wrapper_archive.crate"
