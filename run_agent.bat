@echo off
rem CPSO scrape agent for fleet machines.
rem Keeps the agent alive: if the script exits for any reason
rem (crash, network loss beyond retries), restarts it after 60 s.
cd /d %~dp0
:loop
python main.py --agent
echo Agent exited with code %ERRORLEVEL%. Restarting in 60 seconds...
timeout /t 60 /nobreak >nul
goto loop
