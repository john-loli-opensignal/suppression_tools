Title: feat(simulator): wire to shared modules with guarded fallback

Base: codex-agent
Head: feature/simulator-shared-T5
Merge: e4ab7da (merged into codex-agent)

Summary
- Refactor carrier_suppression_dashboard.py to call suppression_tools.src.metrics and suppression_tools.src.outliers when only ds/mover_ind filters are active.
- Preserve guarded fallback to legacy SQL when additional UI filters (e.g., state/dma) are applied.
- Maintain existing UX: “Other” handling, smoothing, plan preview columns and formatting.

Acceptance
- Simulator uses shared modules for national/competitor paths when only ds/mover_ind are set.
- Guarded fallback preserved for extra filters.
- No behavioral regressions in outputs/columns.

Validation
- Unit tests: uv run --python .venv pytest -q (passed).
- Smoke: make smoke (passes after smoke fix merge e325aec).

Repro
- uv run --python .venv pytest -q
- make smoke

Rollback
- Revertable merge commit: `git revert e4ab7da`.

Notes
- This PR depends on the smoke fix PR (e325aec) for green smoke on codex-agent.

