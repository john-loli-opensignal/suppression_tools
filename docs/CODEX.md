Codex Agent Workflow and Branching

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
- Open PRs from `codex-agent` to `main` for review/merge (or merge locally if no host).
- If a change spans multiple files, split into logically separate commits.

Local-only Remote Setup (optional)
1) Create a local bare remote and push branches:
   - `mkdir -p ~/repos`
   - `git init --bare ~/repos/codebase-comparison.git`
   - `git -C suppression_tools remote add origin ~/repos/codebase-comparison.git`
   - `git -C suppression_tools branch -f main codex-agent`  (align `main` to current baseline)
   - `git -C suppression_tools push -u origin codex-agent`
   - `git -C suppression_tools push -u origin main`
2) Day-to-day:
   - Commit on `codex-agent` and `git push` (defaults to current branch).
   - Merge to `main` locally when stable: `git -C suppression_tools checkout main && git merge --no-ff codex-agent && git push`.

Move to Hosted Later (GitHub/Bitbucket)
- `git -C suppression_tools remote set-url origin <hosted-url>`
- `git -C suppression_tools push -u origin codex-agent`
- `git -C suppression_tools push -u origin main`
- Open PRs from `codex-agent` to `main` on the host.

Suggested Local Config
- Default to pushing current branch: `git -C suppression_tools config push.default current`
- Optionally set default upstream for `codex-agent` after adding a remote: `git -C suppression_tools push -u origin codex-agent`

Reverts
- Revert a single change: `git -C suppression_tools revert <sha>`
- Revert a range: `git -C suppression_tools revert <old>^..<new>`
