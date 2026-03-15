# Snapper — agent instructions (Gemini / Antigravity)

## Source of truth

Project standards live in `.github/copilot-instructions.md`.

This file is an index; it does not auto-include other files. When working on code or docs, explicitly read
`.github/copilot-instructions.md` first (and re-read it if you suspect it changed).

## Gemini specific

- Follow the workflow and quality gate defined in `.github/copilot-instructions.md`.
- Use `make fix-all` for auto-fixing before manual fixes.
- Use `make check-all` as final verification before finishing tasks or creating a PR.
- Include `Co-authored-by: Google Deepmind Antigravity <noreply@google.com>` trailer in any AI-authored commits.
- Backend: Python (FastAPI, SQLAlchemy, Alembic)
- Frontend: TypeScript (React, Vite)
