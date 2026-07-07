@echo off
ping -n 4 127.0.0.1 >nul
echo === LISTEN 5151 ===
netstat -ano | findstr "0.0.0.0:5151"
echo === PID FILE ===
if exist D:\developer\taibai\logs\server.pid type D:\developer\taibai\logs\server.pid
echo === ERR ===
if exist D:\developer\taibai\logs\server.err type D:\developer\taibai\logs\server.err
echo === TAIBAI LOG TAIL ===
if exist D:\developer\taibai\logs\taibai.log powershell -Command "Get-Content -Tail 20 D:\developer\taibai\logs\taibai.log"
