#!/usr/bin/env bash
# Build and package the Worker Bridge VS Code extension.
#   - installs dependencies
#   - compiles src/extension.ts -> dist/extension.js (tsc)
#   - packages a .vsix via @vscode/vsce
set -euo pipefail

cd "$(dirname "$0")"

echo "==> npm install"
npm install

echo "==> compile (tsc -> dist/extension.js)"
npm run compile

echo "==> package (.vsix)"
# --no-dependencies: devDependencies only; the extension bundles no runtime deps.
npx --no-install vsce package --no-dependencies

echo "==> done"
ls -1 ./*.vsix
