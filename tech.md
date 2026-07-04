1. uv
    - modern tool for python packaging (10-100x times faster than pip)
    - install packages, provide virtual envs, dependency locking (uv.lock), project management (pyproject.toml), fast installs (rust-based), python version management, sync environment
    - so pip + venv + pip-tools + pipenv + pyenv ==> uv


2. Typer 
    - if you building a cli agent/developer tool, typer is the best choice.
    - lets you write normal python functions and automatically turns them into CLIs with almost zero boilerplate.
    - use *Python type hints* as the source of truth.
    - uses *Click* underneath, provides colors, prompts, dialogs, progress bars, spinners.