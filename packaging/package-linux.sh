#!/usr/bin/env bash
# Package a release build of Velox for Linux (tar.gz with .desktop + assets).
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=$(cargo metadata --no-deps --format-version 1 | python3 -c "import json,sys; print(json.load(sys.stdin)['packages'][0]['version'])")
OUT="dist/velox-${VERSION}-linux-x86_64"
rm -rf "$OUT" && mkdir -p "$OUT"

cargo build --release -p velox-gui -p velox-cli
cp target/release/velox-gui target/release/velox "$OUT/"
cp packaging/linux/velox.desktop "$OUT/"
cp README.md LICENSE "$OUT/"
cp -r extension "$OUT/extension"
mkdir -p "$OUT/docs" && cp docs/*.md "$OUT/"

tar -czf "dist/velox-${VERSION}-linux-x86_64.tar.gz" -C dist "velox-${VERSION}-linux-x86_64"
echo "packaged: dist/velox-${VERSION}-linux-x86_64.tar.gz"
