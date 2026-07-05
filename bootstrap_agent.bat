@echo off
rem ============================================================
rem  CPSO scrape agent - machine bootstrap, fully self-contained
rem  Copy JUST THIS FILE to a new machine and run it in an
rem  ELEVATED cmd:    bootstrap_agent.bat ["Clinic-Name"]
rem  Installs git if missing, clones/updates the repo to C:\cpso
rem  and hands over to deploy_agent.bat, which does the rest
rem  including installing Python.
rem ============================================================
setlocal
set "REPO_URL=https://github.com/erobertus/doc-web-project.git"
set "REPO_BRANCH=geocode_on_the_fly"
set "REPO_DIR=C:\cpso"
rem used only if discovering the latest release fails
set "GIT_FALLBACK=2.47.1"

echo.
echo === CPSO agent bootstrap ===

rem --- 0. must be elevated -----------------------------------
net session >nul 2>&1
if errorlevel 1 (
    echo [FAIL] This window is not elevated. Right-click cmd,
    echo        "Run as administrator", and re-run this script.
    exit /b 1
)
echo [ ok ] elevated prompt

rem --- 1. git: PATH, known location, else download + install --
set "GIT_EXE=git"
where git >nul 2>&1
if not errorlevel 1 goto :git_ok

if exist "%ProgramFiles%\Git\cmd\git.exe" (
    set "GIT_EXE=%ProgramFiles%\Git\cmd\git.exe"
    goto :git_ok
)

echo [    ] git not found - discovering latest release...
set "GIT_URL="
for /f "usebackq delims=" %%u in (`powershell -NoProfile -Command "try{(Invoke-RestMethod 'https://api.github.com/repos/git-for-windows/git/releases/latest').assets | Where-Object { $_.name -like 'Git-*-64-bit.exe' } | Select-Object -First 1 -ExpandProperty browser_download_url}catch{}"`) do set "GIT_URL=%%u"
if not defined GIT_URL (
    echo [warn] could not discover the latest release - using
    echo        fallback %GIT_FALLBACK%
    set "GIT_URL=https://github.com/git-for-windows/git/releases/download/v%GIT_FALLBACK%.windows.1/Git-%GIT_FALLBACK%-64-bit.exe"
)
echo [    ] downloading git...
curl -L -s -o "%TEMP%\cpso_gitsetup.exe" "%GIT_URL%"
if errorlevel 1 (
    echo [FAIL] download failed - check internet access, or
    echo        install git manually from git-scm.com and re-run.
    exit /b 1
)
"%TEMP%\cpso_gitsetup.exe" /VERYSILENT /NORESTART /NOCANCEL /SP-
if errorlevel 1 (
    echo [FAIL] silent git install failed - install manually
    echo        from git-scm.com and re-run this script.
    exit /b 1
)
del "%TEMP%\cpso_gitsetup.exe" >nul 2>&1
set "GIT_EXE=%ProgramFiles%\Git\cmd\git.exe"
if not exist "%GIT_EXE%" (
    echo [FAIL] git missing at "%GIT_EXE%" after the install.
    exit /b 1
)
:git_ok
echo [ ok ] git: %GIT_EXE%

rem --- 2. clone the repo, or fast-forward an existing one -----
if exist "%REPO_DIR%\.git" (
    echo [    ] repo already present - updating...
    "%GIT_EXE%" -C "%REPO_DIR%" pull --ff-only
    if errorlevel 1 (
        echo [FAIL] could not update the existing repo at
        echo        %REPO_DIR% - resolve manually and re-run.
        exit /b 1
    )
) else (
    "%GIT_EXE%" clone -b %REPO_BRANCH% %REPO_URL% "%REPO_DIR%"
    if errorlevel 1 (
        echo [FAIL] clone failed - check internet access and
        echo        that %REPO_DIR% is writable.
        exit /b 1
    )
)
echo [ ok ] repo at %REPO_DIR%

rem --- 3. hand over to the deployment script ------------------
call "%REPO_DIR%\deploy_agent.bat" %*
exit /b %ERRORLEVEL%
