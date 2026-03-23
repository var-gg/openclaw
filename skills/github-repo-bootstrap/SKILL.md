---
name: github-repo-bootstrap
description: Create or publish a Git repository to GitHub with the safest available path. Use when Codex needs to create a new GitHub repo, attach a local repo to GitHub, recover a missing `gh` workflow on Windows, or choose between `gh`, stored credentials/API calls, and browser fallback. Trigger on requests like 'create this repo on GitHub', 'publish this local repo', 'set origin and push', 'gh isn't working', or 'figure out how we usually create repos here'.
---

# GitHub Repo Bootstrap

Publish repos in this order of preference:

1. **`gh` CLI** if available and authenticated
2. **Stored GitHub credentials + GitHub API** if `gh` is missing but Git Credential Manager already has usable credentials
3. **Browser fallback** only when CLI/API paths are unavailable or require user interaction anyway

Keep the workflow short, auditable, and reversible.

## Quick workflow

1. Inspect the local repo state.
2. Decide the best publish path.
3. Create or confirm the GitHub repo.
4. Attach/update `origin`.
5. Push the current branch.
6. Report the repo URL, visibility, remote URL, and pushed branch.

## 1) Inspect the local repo first

Run these checks before creating anything:

- `git remote -v`
- `git branch --show-current`
- `git status --short`
- `git log --oneline -n 5`

Confirm:

- whether `origin` already exists
- which branch should be pushed (`main` vs `master` vs current branch)
- whether the repo already looks published
- whether there are obvious secrets/runtime dumps that should not be pushed

If the user asked for a "full copy", prefer a sanitized/meta copy unless they explicitly want raw private runtime state.

## 2) Choose the publish path

### Path A — `gh` CLI

Use this if `gh` exists and `gh auth status` succeeds.

Useful checks:

```powershell
Get-Command gh -ErrorAction SilentlyContinue
gh --version
gh auth status
```

If `gh` is installed outside PATH on Windows, probe common locations such as:

- `C:\Program Files\GitHub CLI\gh.exe`
- `C:\Program Files (x86)\GitHub CLI\gh.exe`
- `%LOCALAPPDATA%\Programs\GitHub CLI\gh.exe`
- `%LOCALAPPDATA%\Microsoft\WinGet\Links\gh.exe`
- custom local tool dirs such as `C:\Users\<user>\tools\...`

If needed, use the discovered absolute path instead of assuming PATH is correct.

Create the repo with `gh`, then add/set `origin` and push.

### Path B — Stored credentials + GitHub API

Use this when:

- `gh` is missing or unauthenticated
- browser attach is flaky or slower
- Git Credential Manager may already hold working GitHub credentials

On Windows, probe for stored credentials with Git Credential Manager:

```powershell
$input = "protocol=https`nhost=github.com`n`n"
$cred = $input | git credential-manager get
```

Parse returned `username=` and `password=` lines. If both are present, verify them against:

- `https://api.github.com/user`

Then create the repo with:

- `POST https://api.github.com/user/repos`

Recommended defaults:

- private repo unless the user asked for public
- short description if useful

Handle `422` as “already exists or validation issue”; in that case, fetch the repo instead of blindly failing.

This path is excellent for recovering “we used to be able to publish repos here” situations.

### Path C — Browser fallback

Use this only if CLI/API routes are blocked.

Prefer:

- `profile=user` if normal user-browser attach works
- `profile=chrome-relay` if the Browser Relay extension is the intended path

Use browser fallback when:

- the user is already logged in and can click prompts if needed
- GitHub requires interactive approval/captcha/2FA
- `gh` and stored-credential routes are not viable

## 3) Windows-specific recovery notes

If `gh` is missing:

- do **not** assume it was never installed
- check PATH issues before concluding anything
- if package managers are unavailable, a portable fallback is acceptable

Reliable portable recovery pattern:

1. Query `https://api.github.com/repos/cli/cli/releases/latest`
2. Download the `windows_amd64.zip` asset
3. Extract into a stable local tools directory
4. run `gh --version`
5. run `gh auth status`

This avoids blocking on missing `winget`.

## 4) Attach remote and push

After the repo exists, wire the local repo to GitHub:

```powershell
$remote = 'https://github.com/<owner>/<repo>.git'
$existing = git remote
if ($existing -match '^origin$') {
  git remote set-url origin $remote
} else {
  git remote add origin $remote
}
$branch = git branch --show-current
if (-not $branch) { $branch = 'main' }
git push -u origin $branch
```

If the repo’s default local branch is `master`, do not rename it unless the user asked. Push the current branch cleanly and report what happened.

## 5) Reporting format

At minimum report:

- GitHub repo URL
- visibility
- remote URL
- pushed branch
- which creation path was used (`gh`, API with stored credentials, or browser)
- any follow-up recommendation, such as standardizing the flow into a skill

## 6) Guardrails

- Prefer **private** when visibility is not specified.
- Do not publish secrets, tokens, raw `.openclaw` runtime state, browser/session state, or machine-local dumps unless explicitly asked and risk-checked.
- Do not overwrite an existing `origin` silently if it looks like a different intended repo; report and confirm if ambiguous.
- Keep fallback logic pragmatic: success first, but explain which path succeeded so the workflow can be standardized later.

## 7) Example trigger phrases

- "이 로컬 레포 깃헙에 만들어줘"
- "repo create 하고 origin 연결해"
- "gh 안 되는데 예전엔 됐잖아"
- "우리가 원래 어떻게 GitHub 레포 만들었는지 찾아봐"
- "publish this repo to GitHub"
