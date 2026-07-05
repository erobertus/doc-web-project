@echo off
rem CPSO scrape agent for fleet machines.
rem Keeps the agent alive (restart after ~60 s if it exits) and
rem appends all output to agent.log next to this file.
rem -u: unbuffered stdout so agent.log is live when redirected.
rem ping is used as the delay because 'timeout' does not work in
rem non-interactive (Task Scheduler / SYSTEM) sessions.
rem A runtime bundled at .\python\ (py install --target) wins
rem over the machine-wide python on PATH.
rem
rem Local log rotation: Windows will not let the file be rotated
rem while it is held open here, so the agent exits with code 42
rem when agent.log passes the cap; we then rotate (keeping N
rem generations) and restart at once. Cap/keep are overridable
rem machine-wide:  setx CPSO_LOG_MAX_MB 50 /M   (default 20)
rem                setx CPSO_LOG_KEEP 5 /M       (default 3)
setlocal enabledelayedexpansion
cd /d %~dp0
set "PYEXE=python"
if exist "%~dp0python\python.exe" set "PYEXE=%~dp0python\python.exe"
set "CPSO_AGENT_LOG=%~dp0agent.log"
if not defined CPSO_LOG_MAX_MB set "CPSO_LOG_MAX_MB=20"
if not defined CPSO_LOG_KEEP set "CPSO_LOG_KEEP=3"
set /a MAXBYTES=%CPSO_LOG_MAX_MB% * 1048576

:loop
call :rotate
echo ===== agent (re)start %date% %time% =====>> "%CPSO_AGENT_LOG%"
"%PYEXE%" -u main.py --agent >> "%CPSO_AGENT_LOG%" 2>&1
set "EC=%ERRORLEVEL%"
rem code 42 = agent asked for a local-log rotation: restart now
if "%EC%"=="42" (
    echo ===== rotating agent.log, restarting =====>> "%CPSO_AGENT_LOG%"
    goto loop
)
echo Agent exited with code %EC%. Restarting in 60 seconds...>> "%CPSO_AGENT_LOG%"
ping -n 61 127.0.0.1 >nul
goto loop

:rotate
if not exist "%CPSO_AGENT_LOG%" exit /b
for %%A in ("%CPSO_AGENT_LOG%") do set "SZ=%%~zA"
if !SZ! LSS %MAXBYTES% exit /b
if exist "%CPSO_AGENT_LOG%.%CPSO_LOG_KEEP%" del "%CPSO_AGENT_LOG%.%CPSO_LOG_KEEP%"
for /l %%i in (%CPSO_LOG_KEEP%,-1,2) do (
    set /a prev=%%i-1
    if exist "%CPSO_AGENT_LOG%.!prev!" move /y "%CPSO_AGENT_LOG%.!prev!" "%CPSO_AGENT_LOG%.%%i" >nul
)
move /y "%CPSO_AGENT_LOG%" "%CPSO_AGENT_LOG%.1" >nul
exit /b
