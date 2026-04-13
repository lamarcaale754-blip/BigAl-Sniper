BigAl — one-command launcher (separate from core sources)
=========================================================

This folder does NOT duplicate the whole codebase. It only contains:

  run_bot.py          Loads .env from the parent directory, then runs ../bigalbot_customizable.py
  requirements.txt    Python dependencies for pip install
  RUN_WINDOWS.cmd     Windows: venv + deps + optional Rust build + start bot
  RUN_LINUX.sh        Linux/macOS: same (chmod +x RUN_LINUX.sh once)

The parent directory (..) is the real project: bigalbot_customizable.py, dashboard HTML, rust/, sidecar/, deploy/, etc.
See ../REPO_LAYOUT.txt for a full folder map and upload/sync checklist.

ONE COMMAND
-----------

Windows (double-click or from cmd.exe):

  one-command-launcher\RUN_WINDOWS.cmd

Linux / AWS (from repo root):

  chmod +x one-command-launcher/RUN_LINUX.sh
  ./one-command-launcher/RUN_LINUX.sh

Optional: put secrets in a .env file next to bigalbot_customizable.py (parent folder). run_bot.py loads it automatically if python-dotenv is installed.

WHAT GETS AUTOMATED
-------------------

- Creates venv in the project root if missing
- pip install -r one-command-launcher/requirements.txt
- First-time: cargo build --release for rust/bigal-executor if the release binary is missing
- Starts the bot with environment variables from .env (when present)

DASHBOARD FIELDS (after mode selection)
---------------------------------------

The server sends "startup_hints" so the modal explains which boxes are optional:

- Private key: optional if BIGAL_PRIVATE_KEY or keypair.txt is already on the machine
- Helius API key: optional if HELIUS_RPC_URL already contains a real key (not YOUR_API_KEY_HERE)
- gRPC / Jito: optional if already set in the environment; fields override for that session when filled

Pasting values always overrides environment for that connection session where the bot applies it.
