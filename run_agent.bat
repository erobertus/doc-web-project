@echo off
rem CPSO scrape agent for fleet machines.
rem Keeps the agent alive (restart after ~60 s if it exits) and
rem appends all output to agent.log next to this file.
rem -u: unbuffered stdout so agent.log is live when redirected.
rem ping is used as the delay because 'timeout' does not work in
rem non-interactive (Task Scheduler / SYSTEM) sessions.
cd /d %~dp0
:loop
echo ===== agent (re)start %date% %time% =====>> "%~dp0agent.log"
python -u main.py --agent >> "%~dp0agent.log" 2>&1
echo Agent exited with code %ERRORLEVEL%. Restarting in 60 seconds...>> "%~dp0agent.log"
ping -n 61 127.0.0.1 >nul
goto loop
