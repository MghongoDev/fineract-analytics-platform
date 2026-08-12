#!/usr/bin/env bash
# =====================================================================
# render.sh - render every .mmd diagram in this directory to .svg.
#
# The .mmd files are the source of truth, not the .svg output: Mermaid
# source is plain text, so it diffs cleanly in code review (a one-line
# change to a label produces a one-line diff), whereas an SVG re-render
# changes on every run even when nothing meaningful moved. Regenerate
# the .svg files with this script when you need to look at a rendered
# diagram; do not hand-edit or commit-review the .svg output as if it
# were the artifact under change.
#
# Requires @mermaid-js/mermaid-cli (npx -y @mermaid-js/mermaid-cli). If
# it is not installed, this script says so and exits without touching
# anything - it does not attempt a fallback renderer.
# =====================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MMDC="npx -y @mermaid-js/mermaid-cli"

if ! command -v npx >/dev/null 2>&1; then
  echo "[render] npx not found - install Node.js to render diagrams." >&2
  echo "[render] the .mmd files remain valid on their own; this is only for producing .svg previews." >&2
  exit 0
fi

for mmd in "${DIR}"/*.mmd; do
  svg="${mmd%.mmd}.svg"
  echo "[render] ${mmd} -> ${svg}"
  ${MMDC} -i "${mmd}" -o "${svg}" -b transparent
done

echo "[render] done."
