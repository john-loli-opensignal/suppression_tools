Agent Workflow and Branching

Summary
- Always work on and push to the `codex-agent` branch.
- Keep commits small and focused (one logical change per commit).
- Use clear, conventional commit messages to enable easy reverts.

Branches
- `main`: stable baseline, reviewed merges only.
- `codex-agent`: active development by the agent. Default working branch.

Commit Style
- Format: `<type>(scope): short description`
  - Examples: `feat(dashboard): add carrier multiselect`, `chore(paths): update suppressions dir`
- One change per commit to simplify `git revert`.

Working Rules
- Do not commit directly to `main`.
- Open PRs from `codex-agent` to `main` for review/merge.
- If a change spans multiple files, split into logically separate commits.

Suggested Local Config
- To default to pushing to `codex-agent` from this repo:
  - `git config branch.codex-agent.merge refs/heads/codex-agent`
  - `git config push.default current`

