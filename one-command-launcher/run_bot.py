"""
Bootstrap entry: chdir to repo root, load .env into the environment, run bigalbot_customizable.py.
Keep this file in one-command-launcher/ — the main bot sources stay in the parent folder unchanged.
"""
from __future__ import annotations

import os
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.normpath(os.path.join(_HERE, ".."))


def main() -> int:
    os.chdir(_ROOT)
    env_path = os.path.join(_ROOT, ".env")
    try:
        from dotenv import load_dotenv

        if os.path.isfile(env_path):
            load_dotenv(env_path)
    except ImportError:
        pass
    target = os.path.join(_ROOT, "bigalbot_customizable.py")
    if not os.path.isfile(target):
        print(f"Cannot find bot at {target}", file=sys.stderr)
        return 1
    return subprocess.call([sys.executable, target, *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
