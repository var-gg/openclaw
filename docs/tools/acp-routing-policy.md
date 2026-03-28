# ACP Routing Policy

Last updated: 2026-03-29 (Asia/Seoul)

## Hard rule

OpenClaw must default delegated work to `subagent`.

OpenClaw may use ACP only when the user's message literally contains `ACP`.

## Allowed ACP examples

- `ACP codex`
- `ACP claude`
- `ACP gemini`
- `ACP로 돌려`

## Not ACP examples

These must route to `subagent` unless the same request literally contains `ACP`:

- `codex로 해봐`
- `claude로 돌려`
- `gemini 써`
- `코딩 에이전트 써`
- `백그라운드 에이전트`
- `에이전트로 돌려`

## Interpretation notes

1. Naming a harness alone does not authorize ACP.
2. Generic agent wording does not authorize ACP.
3. If the request is ambiguous and lacks literal `ACP`, choose `subagent`.
4. Do not promote a request to ACP based on habit or context.

## Canonical operational location

The primary routing instruction for ACP harness requests lives in:

- `extensions/acpx/skills/acp-router/SKILL.md`

This document exists as a global reference so the rule is visible outside the skill file too.
