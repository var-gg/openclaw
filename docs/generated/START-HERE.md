# START HERE

> 3-minute orientation for the OpenClaw MD control-plane.

This repo has lots of Markdown, but not all Markdown is equal.

If you want to answer **"which MD is canonical, and who reads it?"**, start here:

1. **`topology/md-index.yaml`**
   - canonical inventory of important Markdown/control-plane docs
   - declares scope, class, load mode, source of truth, and owning consumer/session

2. **`topology/load-graph.yaml`**
   - canonical consumer graph
   - shows which session / cron / subagent reads what, whether that read is automatic, and what it writes back

3. **`docs/generated/MD-MAP.md`**
   - human-readable map distilled from the topology files

4. **`docs/generated/ORPHANS.md`**
   - candidate Markdown docs not yet registered in the canonical topology

## Mental model

Treat this repo as having **two different MD planes**:

- **Canonical topology / architecture docs**
  - stable, repo-level, intended to explain system structure and doc loading relationships
  - source of truth: `topology/*.yaml`

- **Machine-specific operations docs**
  - host/device/operator-specific notes and runtime paths
  - should stay separate from canonical topology
  - if the exact runtime path is unknown, keep a TODO/placeholder instead of guessing

## What is canonical right now?

- **Canonical inventory:** `topology/md-index.yaml`
- **Canonical consumer/load graph:** `topology/load-graph.yaml`
- **Generated human entry docs:** `docs/generated/MD-MAP.md`, `docs/generated/START-HERE.md`, `docs/generated/ORPHANS.md`

## Fast path by role

### If you are a maintainer or coding agent
Read in this order:

1. `AGENTS.md`
2. `topology/md-index.yaml`
3. `topology/load-graph.yaml`
4. `docs/generated/MD-MAP.md`

### If you are trying to understand workspace bootstrap files
Check these topology entries / references:

- `docs/reference/AGENTS.default.mdx`
- `docs/reference/templates/AGENTS.mdx`
- `docs/reference/templates/BOOTSTRAP.mdx`
- `docs/reference/templates/IDENTITY.mdx`
- `docs/reference/templates/SOUL.mdx`
- `docs/reference/templates/TOOLS.mdx`
- `docs/reference/templates/USER.mdx`

### If you are working on bundled runtime hooks
Check:

- `src/hooks/bundled/README.md`
- `src/hooks/bundled/*/HOOK.md`

## Current limitations

- This generated set is currently **scaffolded from a hand-maintained topology**, not fully auto-built yet.
- Machine-specific operational Markdown paths are intentionally left as TODO when unknown.
- `ORPHANS.md` is a **candidate queue**, not a final verdict.

## Next command

Planned builder scaffold:

```powershell
pwsh ./scripts/build-md-map.ps1
```
