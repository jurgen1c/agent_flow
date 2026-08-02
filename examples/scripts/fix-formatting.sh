#!/bin/sh
set -eu

mkdir -p ci

if [ -x bin/rubocop ]; then
  bin/rubocop -A > ci/formatting-fix.log 2>&1
elif command -v bun >/dev/null 2>&1 && [ -f package.json ]; then
  bun run lint --fix > ci/formatting-fix.log 2>&1
else
  printf '%s\n' 'No supported formatter was found (bin/rubocop or Bun lint script).' > ci/formatting-fix.log
  exit 1
fi
