@echo off
setlocal EnableExtensions
cd /d "%~dp0.." || exit /b 1

where py >nul 2>&1 && (set "PY=py -3") || (set "PY=python")

if not exist "venv\Scripts\python.exe" (
  echo [BigAl] Creating Python venv...
  %PY% -m venv venv || exit /b 1
)

call "venv\Scripts\activate.bat"
echo [BigAl] Installing / updating dependencies...
pip install -q -r "%~dp0requirements.txt"

if not exist "rust\bigal-executor\target\release\bigal-executor.exe" (
  if exist "rust\bigal-executor\Cargo.toml" (
    echo [BigAl] Building Rust executor ^(first run may take several minutes^)...
    pushd rust\bigal-executor && cargo build --release && popd
  ) else (
    echo [BigAl] Warning: Rust sources not found — Live ^(Rust^) mode will need the binary built manually.
  )
)

echo [BigAl] Starting dashboard + bot...
python "%~dp0run_bot.py"
set "EC=%ERRORLEVEL%"
echo.
echo [BigAl] Exited with code %EC%
pause
exit /b %EC%
