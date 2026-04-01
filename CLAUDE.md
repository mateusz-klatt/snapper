# Snapper - Project Instructions

Project standards and guidelines: @.github/copilot-instructions.md

## Claude Code specific

- Use `make fix-all` for auto-fixing before manual fixes
- Use `make check-all` as final verification
- When exploring codebase, prefer Task tool with Explore agent
- Backend: Python (FastAPI, SQLAlchemy, Alembic)
- Frontend: TypeScript (React, Vite)

## Memory & Plans

All project memory (feedback, plans, project notes, references) lives in `proprietary/memory/`.
The index is `proprietary/memory/MEMORY.md` — read it at the start of every session.
Write new memory files to `proprietary/memory/`, not to `~/.claude/projects/*/memory/`.

Implementation plans go to `proprietary/plans/`, not to `~/.claude/plans/`.
