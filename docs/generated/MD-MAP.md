# MD MAP

This document is the human-readable view of the Markdown/control-plane topology.
The canonical source of truth is still:

- `topology/md-index.yaml`
- `topology/load-graph.yaml`

## 1) Canonical control-plane sources

### Topology source of truth

- `topology/md-index.yaml`
  - canonical inventory of important Markdown/control-plane files
- `topology/load-graph.yaml`
  - canonical consumer/load/write graph

### Generated views

- `docs/generated/START-HERE.md`
  - 3-minute orientation
- `docs/generated/MD-MAP.md`
  - this file
- `docs/generated/ORPHANS.md`
  - unregistered Markdown candidate list

## 2) Repo-level canonical docs

| ID | Path | Scope | Class | Load mode | Notes |
| --- | --- | --- | --- | --- | --- |
| `repo-agents` | `AGENTS.md` | `shared` | `managed-shared` | `auto` | Root maintainer/agent operating rules |
| `repo-readme` | `README.md` | `shared` | `reference` | `referenced` | Public entry page; now links into generated map |
| `repo-security` | `SECURITY.md` | `shared` | `reference` | `referenced` | Security trust-boundary reference |
| `repo-contributing` | `CONTRIBUTING.md` | `shared` | `reference` | `referenced` | Human contributor workflow |
| `repo-changelog` | `CHANGELOG.md` | `shared` | `reference` | `referenced` | Release history |
| `repo-vision` | `VISION.md` | `shared` | `reference` | `referenced` | Product narrative / direction |
| `repo-claude` | `CLAUDE.md` | `shared` | `managed-shared` | `referenced` | Claude compatibility shim |

## 3) Localized/session-specific Markdown

| ID | Path | Why it exists |
| --- | --- | --- |
| `gateway-server-methods-agents` | `src/gateway/server-methods/AGENTS.md` | Local rules for server-methods work |
| `gateway-server-methods-claude` | `src/gateway/server-methods/CLAUDE.md` | Claude-compatible local companion |

These are **project-scoped** and should not be mistaken for global repo entry points.

## 4) Runtime-adjacent hook docs

| ID | Path | Load behavior |
| --- | --- | --- |
| `hook-bundled-readme` | `src/hooks/bundled/README.md` | Human reference |
| `hook-boot-md` | `src/hooks/bundled/boot-md/HOOK.md` | Runtime-only/reference |
| `hook-bootstrap-extra-files` | `src/hooks/bundled/bootstrap-extra-files/HOOK.md` | Runtime-only/reference |
| `hook-command-logger` | `src/hooks/bundled/command-logger/HOOK.md` | Runtime-only/reference |
| `hook-session-memory` | `src/hooks/bundled/session-memory/HOOK.md` | Runtime-only/reference |

These document runtime behavior, but they are **not** themselves the canonical topology layer.

## 5) Workspace-template references

These are canonical **template/reference docs**, not repo runtime auto-load docs:

- `docs/reference/AGENTS.default.mdx`
- `docs/reference/templates/AGENTS.mdx`
- `docs/reference/templates/BOOTSTRAP.mdx`
- `docs/reference/templates/IDENTITY.mdx`
- `docs/reference/templates/SOUL.mdx`
- `docs/reference/templates/TOOLS.mdx`
- `docs/reference/templates/USER.mdx`

## 6) Consumer load graph summary

### Session consumers

- `maintainer-agent-root`
  - auto-loads: `AGENTS.md`
  - references topology + generated docs
- `gateway-server-methods-agent`
  - auto-loads: `AGENTS.md`, `src/gateway/server-methods/AGENTS.md`
- `claude-compatible-session`
  - auto-loads: `AGENTS.md`
  - references `CLAUDE.md`
- `workspace-template-author`
  - references workspace template docs
- `bundled-hook-maintainer`
  - references hook docs under `src/hooks/bundled/`

### Cron consumer

- `repo-md-map-regenerator`
  - planned job
  - reads `topology/*.yaml`
  - writes `docs/generated/*`
  - runtime scheduler registration path still TODO

### Subagent consumer

- `md-map-builder`
  - reads topology + root context
  - writes generated docs

## 7) Classification guide

### Scope

- `session`: session-specific or consumer-specific working context
- `project`: subtree or project-area specific
- `shared`: cross-session shared reference
- `runtime-ref`: runtime path / machine-specific / builder reference

### Class

- `guarded-bootstrap`: bootstrap or first-run sensitive guidance
- `memory`: persistent memory-like doc class
- `reference`: stable explanatory reference
- `scratch`: temporary / queue / staging surface
- `managed-shared`: shared file intended to be actively maintained as part of control-plane behavior

### Load mode

- `auto`: expected to be automatically loaded by a consumer
- `referenced`: consulted when needed
- `runtime-only`: meaningful mainly via runtime/hook behavior
- `generated`: produced from topology rather than authored as source of truth

## 8) Boundary rule: canonical vs machine-specific

Use this split consistently:

- **Canonical:** topology, architecture, template references, repo-wide agent guidance
- **Machine-specific:** host notes, real operator paths, local service quirks, device-bound operational docs

If a path or runtime location is not known with certainty, leave:

- `TODO(...)`
- placeholder notes

Do **not** guess PC-specific locations.

## 9) Builder status

Planned scaffold:

- `scripts/build-md-map.ps1`

Current status:

- scaffold only
- no full topology parser/generator implementation yet
