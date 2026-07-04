@echo off
rem CPSO scrape agent for fleet machines.
rem Keeps the agent alive (restart after 60 s if it exits) and
rem appends all output to agent.log next to this file.
cd /d %~dp0
:loop
echo ===== agent (re)start %date% %time% =====>> "%~dp0agent.log"
python main.py --agent >> "%~dp0agent.log" 2>&1
echo Agent exited with code %ERRORLEVEL%. Restarting in 60 seconds...>> "%~dp0agent.log"
timeout /t 60 /nobreak >nul
goto loop
