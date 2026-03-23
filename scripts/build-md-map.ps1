param(
    [switch]$Check,
    [switch]$VerboseOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

<##
.SYNOPSIS
Scaffold builder for the Markdown control-plane map.

.DESCRIPTION
Intended end state:
- read topology/md-index.yaml
- read topology/load-graph.yaml
- regenerate docs/generated/START-HERE.md
- regenerate docs/generated/MD-MAP.md
- regenerate docs/generated/ORPHANS.md

Current state:
- scaffold only
- validates expected files exist
- prints TODO guidance

Notes:
- Keep topology/*.yaml as the source of truth.
- Do not guess machine-specific runtime paths; preserve TODO placeholders.
- ORPHANS should be a candidate queue, not a silent drop-bucket.
##>

$repoRoot = Split-Path -Parent $PSScriptRoot
$topologyDir = Join-Path $repoRoot 'topology'
$generatedDir = Join-Path $repoRoot 'docs/generated'

$requiredFiles = @(
    (Join-Path $topologyDir 'md-index.yaml'),
    (Join-Path $topologyDir 'load-graph.yaml'),
    (Join-Path $generatedDir 'START-HERE.md'),
    (Join-Path $generatedDir 'MD-MAP.md'),
    (Join-Path $generatedDir 'ORPHANS.md')
)

$missing = @($requiredFiles | Where-Object { -not (Test-Path $_) })
if ($missing.Count -gt 0) {
    throw "Missing required control-plane files:`n - $($missing -join "`n - ")"
}

if ($VerboseOutput) {
    Write-Host "[md-map] repoRoot=$repoRoot"
    Write-Host "[md-map] topologyDir=$topologyDir"
    Write-Host "[md-map] generatedDir=$generatedDir"
}

Write-Host '[md-map] Scaffold OK.'
Write-Host '[md-map] TODO: parse YAML, materialize generated docs, and discover orphan Markdown candidates.'
Write-Host '[md-map] Source of truth remains topology/md-index.yaml + topology/load-graph.yaml.'

if ($Check) {
    Write-Host '[md-map] Check mode: no files written.'
}
