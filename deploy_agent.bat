@echo off
rem ============================================================
rem  CPSO scrape agent - deployment and lifecycle
rem  Usage (ELEVATED cmd):
rem    deploy_agent.bat ["Clinic-Name"] ["Knock-Seq"]
rem                                      full deployment; asks for
rem                                      the name and knock
rem                                      sequence when omitted.
rem                                      QUOTE the knock sequence
rem                                      - it contains commas.
rem    deploy_agent.bat disable          stop agent + disable the
rem                                      task, keep installation
rem    deploy_agent.bat enable           re-enable + start agent
rem    deploy_agent.bat remove           total cleanup: task, agent
rem                                      name, and this folder
rem                                      - Python and git stay
rem  Prerequisite: git (to clone the repo this script lives in).
rem  Python is found automatically, in order of preference:
rem    1. runtime bundled in the repo at .\python\
rem       - e.g.  py install 3.14 --target C:\cpso\python
rem    2. an existing all-users install in Program Files
rem    3. none of the above: DOWNLOADED from python.org and
rem       silently installed for all users - PY_VERSION below
rem ============================================================
setlocal
cd /d %~dp0

rem used only if discovering the latest release fails
set "PY_FALLBACK=3.13.1"

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

rem --- lifecycle modes ----------------------------------------
if /i "%~1"=="disable" goto :mode_disable
if /i "%~1"=="enable"  goto :mode_enable
if /i "%~1"=="remove"  goto :mode_remove

rem --- 1. locate python: bundled runtime, else Program Files,
rem        else download + silent all-users install -------------
set "PYEXE="
if exist "%~dp0python\python.exe" set "PYEXE=%~dp0python\python.exe"
if defined PYEXE goto :python_ok

for /f "delims=" %%p in ('where python 2^>nul') do (
    if not defined PYEXE set "PYEXE=%%p"
)
if defined PYEXE (
    echo %PYEXE% | findstr /i /c:"Program Files" >nul
    if not errorlevel 1 goto :python_ok
)

echo [    ] no SYSTEM-visible python - discovering latest release...
set "PY_VERSION="
for /f "usebackq delims=" %%v in (`powershell -NoProfile -Command "try{$c=(Invoke-WebRequest -UseBasicParsing 'https://www.python.org/downloads/').Content;if($c -match 'Download Python (3\.[0-9]+\.[0-9]+)'){$Matches[1]}}catch{}"`) do set "PY_VERSION=%%v"
if not defined PY_VERSION (
    echo [warn] could not discover the latest release - using
    echo        fallback %PY_FALLBACK%
    set "PY_VERSION=%PY_FALLBACK%"
)
for /f "tokens=1,2 delims=." %%a in ("%PY_VERSION%") do set "PY_DIRVER=%%a%%b"
set "PY_HOME=C:\Program Files\Python%PY_DIRVER%"
echo [    ] installing python %PY_VERSION% for all users...
curl -L -s -o "%TEMP%\cpso_pysetup.exe" "https://www.python.org/ftp/python/%PY_VERSION%/python-%PY_VERSION%-amd64.exe"
if errorlevel 1 (
    echo [FAIL] download from python.org failed - check internet
    echo        access, or install Python manually and re-run.
    exit /b 1
)
"%TEMP%\cpso_pysetup.exe" /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
if errorlevel 1 (
    echo [FAIL] silent Python install failed - install manually
    echo        and re-run this script.
    exit /b 1
)
del "%TEMP%\cpso_pysetup.exe" >nul 2>&1
set "PYEXE=%PY_HOME%\python.exe"
if not exist "%PYEXE%" (
    echo [FAIL] expected "%PYEXE%" after the install - adjust
    echo        PY_VERSION / PY_HOME at the top of this script.
    exit /b 1
)
:python_ok
echo [ ok ] python: %PYEXE%

rem --- 2. dependencies into a SYSTEM-visible site-packages ----
"%PYEXE%" -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo [FAIL] pip install failed. If the error mentions mariadb,
    echo        install "Microsoft Visual C++ Redistributable x64"
    echo        and re-run.
    exit /b 1
)
"%PYEXE%" -c "import sys, mariadb; sys.exit(1 if 'roaming' in mariadb.__file__.lower() else 0)"
if errorlevel 1 (
    echo [FAIL] packages resolve to a per-user location - caused
    echo        by an earlier pip run without elevation. Fix:
    echo          rmdir /s /q "%%APPDATA%%\Python"
    echo        then re-run this script.
    exit /b 1
)
echo [ ok ] dependencies in a SYSTEM-visible location

rem --- 2b. port-knock sequence --------------------------------
rem  Opens the DB firewall for this clinic's dynamic IP; a
rem  fleet-wide shared secret. Sourced in order: arg 2 (quoted),
rem  the CPSO_KNOCK env var, the value already stored on this
rem  machine (kept as-is on a re-deploy), else an interactive
rem  prompt. Format "[proto:]p1,p2,p3" e.g. "tcp:7001,8002,9003"
rem  (ports may be separated by ',' or ';').
set "EXISTING_KNOCK="
for /f "tokens=2,*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v CPSO_KNOCK 2^>nul ^| find /i "CPSO_KNOCK"') do set "EXISTING_KNOCK=%%b"

set "KNOCK=%~2"
if not defined KNOCK if defined CPSO_KNOCK set "KNOCK=%CPSO_KNOCK%"
if not defined KNOCK if defined EXISTING_KNOCK (
    set "KNOCK=%EXISTING_KNOCK%"
    echo [ ok ] keeping the port-knock sequence already on this machine
)
if not defined KNOCK (
    echo.
    echo Port-knock sequence opens the DB firewall for this
    echo clinic's dynamic IP - a shared fleet-wide value.
    echo Example: tcp:7001,8002,9003
    set /p KNOCK=Knock sequence [Enter = no knocking]:
)
if defined KNOCK (
    setx CPSO_KNOCK "%KNOCK%" /M >nul
    echo [ ok ] port-knock sequence stored machine-wide
) else (
    echo [warn] no port knocking - agents assume the DB firewall
    echo        already allows this machine. Set it later with:
    echo          setx CPSO_KNOCK "tcp:7001,8002,9003" /M
)

rem --- 3. ACL hardening: clinic users read-only ---------------
icacls "%~dp0." /inheritance:d >nul
icacls "%~dp0." /remove:g "Authenticated Users" /t >nul 2>&1
echo [ ok ] permissions tightened - users read-only

rem --- 4. per-clinic agent name -------------------------------
set "AGENT_NAME=%~1"
if not defined AGENT_NAME (
    set /p AGENT_NAME=Clinic name for this machine [Enter = use hostname]:
)
if not defined AGENT_NAME (
    echo [warn] no clinic name - the central log will show
    echo        "hostname @ connection-origin" instead. To name it
    echo        later:  setx CPSO_AGENT_NAME "Clinic-X" /M
) else (
    setx CPSO_AGENT_NAME "%AGENT_NAME%" /M >nul
    echo [ ok ] agent name: %AGENT_NAME%
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

rem ============================================================
:mode_disable
schtasks /end /tn "CPSO scrape agent" >nul 2>&1
schtasks /change /tn "CPSO scrape agent" /disable >nul 2>&1
if errorlevel 1 (
    echo [FAIL] task "CPSO scrape agent" not found - nothing to
    echo        disable. Run a full deployment first.
    exit /b 1
)
echo [ ok ] agent stopped; task disabled - it will NOT start at
echo        boot. Installation kept. Re-enable with:
echo          deploy_agent.bat enable
exit /b 0

:mode_enable
schtasks /change /tn "CPSO scrape agent" /enable >nul 2>&1
if errorlevel 1 (
    echo [FAIL] task "CPSO scrape agent" not found - run a full
    echo        deployment first.
    exit /b 1
)
schtasks /run /tn "CPSO scrape agent" >nul
echo [ ok ] task enabled and agent started.
exit /b 0

:mode_remove
schtasks /end /tn "CPSO scrape agent" >nul 2>&1
schtasks /delete /tn "CPSO scrape agent" /f >nul 2>&1
echo [ ok ] scheduled task removed
reg delete "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v CPSO_AGENT_NAME /f >nul 2>&1
reg delete "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v CPSO_KNOCK /f >nul 2>&1
echo [ ok ] agent name and knock sequence removed
set "TARGET=%~dp0"
set "TARGET=%TARGET:~0,-1%"
echo [ ok ] deleting %TARGET% in a few seconds...
echo        Python and git are left installed.
echo === removal complete ===
start "" /min cmd /c ping -n 4 127.0.0.1 ^>nul ^& rd /s /q "%TARGET%"
exit /b 0
