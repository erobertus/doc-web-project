@echo off
rem ============================================================
rem  CPSO scrape agent - one-shot deployment
rem  Usage (ELEVATED cmd):  deploy_agent.bat "Clinic-Name"
rem  Prerequisites: classic Python (all users) + git, repo cloned
rem  to a machine-wide path (this script lives inside the repo).
rem ============================================================
setlocal
cd /d %~dp0

echo.
echo === CPSO agent deployment ===

rem --- 0. must be elevated -----------------------------------
net session >nul 2>&1
if errorlevel 1 (
    echo [FAIL] This window is not elevated. Right-click cmd,
    echo        "Run as administrator", and re-run this script.
    exit /b 1
)
echo [ ok ] elevated prompt

rem --- 1. python must be the all-users install ----------------
set "PYEXE="
for /f "delims=" %%p in ('where python 2^>nul') do (
    if not defined PYEXE set "PYEXE=%%p"
)
if not defined PYEXE (
    echo [FAIL] python not found on PATH. Install Python with the
    echo        CLASSIC installer, "Install for all users" +
    echo        "Add python.exe to PATH", then re-run.
    exit /b 1
)
echo %PYEXE% | findstr /i /c:"Program Files" >nul
if errorlevel 1 (
    echo [FAIL] python resolves to "%PYEXE%" - a per-user install
    echo        the SYSTEM task cannot see. Uninstall it and use
    echo        the classic installer with "Install for all users".
    exit /b 1
)
echo [ ok ] python: %PYEXE%

rem --- 2. dependencies into the global site-packages ----------
python -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo [FAIL] pip install failed. If the error mentions mariadb,
    echo        install "Microsoft Visual C++ Redistributable x64"
    echo        and re-run.
    exit /b 1
)
python -c "import sys, mariadb; sys.exit(0 if 'Program Files' in mariadb.__file__ else 1)"
if errorlevel 1 (
    echo [FAIL] packages resolve to a per-user location - caused
    echo        by an earlier pip run without elevation. Fix:
    echo          rmdir /s /q "%%APPDATA%%\Python"
    echo        then re-run this script.
    exit /b 1
)
echo [ ok ] dependencies in global site-packages

rem --- 3. ACL hardening: clinic users read-only ---------------
icacls "%~dp0." /inheritance:d >nul
icacls "%~dp0." /remove:g "Authenticated Users" /t >nul 2>&1
echo [ ok ] permissions tightened (users read-only)

rem --- 4. per-clinic agent name -------------------------------
if "%~1"=="" (
    echo [warn] no clinic name given - the central log will show
    echo        "hostname @ connection-origin" instead. To name it
    echo        later:  setx CPSO_AGENT_NAME "Clinic-X" /M
) else (
    setx CPSO_AGENT_NAME "%~1" /M >nul
    echo [ ok ] agent name: %~1
)

rem --- 5. scheduled task (SYSTEM, at boot, start now) ---------
schtasks /end /tn "CPSO scrape agent" >nul 2>&1
schtasks /create /f /tn "CPSO scrape agent" /sc onstart ^
    /tr "%~dp0run_agent.bat" /ru SYSTEM >nul
if errorlevel 1 (
    echo [FAIL] could not create the scheduled task.
    exit /b 1
)
schtasks /run /tn "CPSO scrape agent" >nul
echo [ ok ] scheduled task created and started

rem --- 6. smoke check: agent must be alive and polling --------
echo      waiting 15 s for the agent to come up...
ping -n 16 127.0.0.1 >nul
if not exist "%~dp0agent.log" (
    echo [FAIL] agent.log did not appear - check Task Scheduler
    echo        history for "CPSO scrape agent".
    exit /b 1
)
powershell -NoProfile -Command "Get-Content '%~dp0agent.log' -Tail 5"
tasklist /fi "imagename eq python.exe" | findstr /i python >nul
if errorlevel 1 (
    echo [FAIL] no python.exe process - agent crashed on start;
    echo        see agent.log above.
    exit /b 1
)
echo.
echo [ ok ] agent is running. Final check from your desk:
echo        SELECT host, MAX(log_time) FROM MD_scrape_log GROUP BY host;
echo === deployment complete ===
exit /b 0
