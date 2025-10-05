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

## Critical Project Rules

### Database Management
- **SINGLE DATABASE ONLY**: `data/databases/duck_suppression.db`
- ⚠️ **NEVER** create databases in project root or other locations
- Always use `tools.db.get_db_path()` to get the database path
- All scripts must validate database path before operations
- If you find multiple .db files, DELETE extras immediately

### Analysis and Temporary Files
- **ALL analysis outputs go in `analysis/` directory**:
  - Images → `analysis/images/`
  - Output files → `analysis/outputs/`
  - Test scripts → `analysis/`
  - Markdown reports → `analysis/`
- **NEVER** create temporary files in project root
- Clean up temporary files on exit or crash

### Project Organization
- Keep project root minimal: only essential config files and main dashboards
- Use subdirectories for organization:
  - `tools/` - Core utility modules
  - `scripts/` - Operational scripts
  - `data/` - All data files
  - `analysis/` - All analysis work
  - `tests/` - Test files

