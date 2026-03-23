# ORPHANS

This file lists **Markdown candidates not yet registered in `topology/md-index.yaml`**.

Important:

- This is a **triage queue**, not a declaration that these files are unimportant.
- Some files are intentionally excluded because they are third-party/vendor docs, generated docs, app-specific docs, or non-canonical references.
- A file should leave this list only after being either:
  1. added to `topology/md-index.yaml`, or
  2. explicitly marked as intentionally out-of-scope for the MD control-plane.

## Candidate buckets

### Likely canonical or near-canonical candidates

- `docs.acp.md`
- `docs/testing.md`
- `docs/reference/RELEASING.md`
- `docs/gateway/*.mdx` (needs selective registration, not blanket import)
- `docs/concepts/architecture.mdx` (or equivalent current path; verify exact filename before registration)

### Project / app / extension local docs that may deserve selective registration later

- `Swabble/README.md`
- `Swabble/CHANGELOG.md`
- `Swabble/docs/spec.md`
- extension-level `README.md` files where the extension itself is a maintained control-plane surface

### Skills and local agent docs not yet topology-registered

- `skills/*/SKILL.md`
- `.agents/**/README.md` or `.agents/**/PR_WORKFLOW.md`-style process docs

These may warrant a second topology layer (for example `topology/skill-index.yaml`) instead of bloating the main MD control-plane map.

### Intentionally out-of-scope by default unless promoted

- `vendor/**/README.md`
- vendor `GEMINI.md` files
- generated translation docs
- third-party package docs

## Registration heuristics

Add a Markdown file to `topology/md-index.yaml` when at least one is true:

- a session/consumer auto-loads it
- maintainers rely on it as canonical reference
- it explains a control-plane boundary or runtime behavior
- it owns a stable workflow that other docs defer to

Keep it out when it is mainly:

- third-party/vendor material
- package-local readme with no control-plane significance
- generated output with another canonical source
- machine-specific operator note that belongs outside canonical topology

## TODO

- Replace wildcard/path-family bullets with verified per-file entries.
- Optionally split orphans into:
  - `unregistered-canonical-candidates`
  - `intentionally-out-of-scope`
  - `vendor/reference-only`
- Teach `scripts/build-md-map.ps1` to discover candidates automatically.
