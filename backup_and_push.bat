@echo off
:: ============================================================
::  Pehnava — Backup & Push
::  Runs connection.py which handles export + git push.
::  NOTE: Keep this file private — token is inside connection.py
:: ============================================================

set CONNECTION_PY="%~dp0connection.py"

echo.
echo  Pehnava Backup ^& Push
echo  ----------------------
echo.

python %CONNECTION_PY%
if errorlevel 1 (
    echo.
    echo [ERROR] Backup failed. See output above.
    pause
    exit /b 1
)

pause
