@echo off
setlocal enabledelayedexpansion
rem perf_verify: end-to-end SQL INSERT throughput measurement.
rem Usage: perf_verify.bat [rows]   (default 100000)
set ROOT=%TEMP%\taibai_bench
set BIN=%~dp0..\..\target\release
set ROWS=%1
if "%ROWS%"=="" set ROWS=100000
rem Random port: a previous run may have left a server process that only an
rem elevated shell can kill, so we never assume 5153 is free.
set /a PORT=%RANDOM% %% 2000 + 5000

if not exist "%BIN%\taibai.exe" (
    echo taibai.exe not found. Run first:
    echo   cargo build --release --workspace
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0gen_bench_env.ps1" -Rows %ROWS% -Root "%ROOT%" -Port %PORT%
if errorlevel 1 goto fail

echo === server start on port %PORT% ===
pushd "%ROOT%"
start "" /b "%BIN%\taibai.exe" --config "%ROOT%\bench.yaml" > "%ROOT%\server.log" 2>&1
popd

set /a tries=0
:wait_ready
timeout /t 1 /nobreak >nul
netstat -ano | findstr ":%PORT%" | findstr "LISTENING" >nul 2>&1
if errorlevel 1 (
    set /a tries+=1
    if !tries! lss 15 goto wait_ready
    echo server did not start listening on %PORT%; see %ROOT%\server.log
    goto fail
)
echo server is listening.

echo === run 1: realtime shape, batch=100 rows per INSERT ===
"%BIN%\multi_insert.exe" 127.0.0.1:!PORT! "%ROOT%\ticks.csv" --batch-size=100 --clients=8 --no-bulk --database=main 2>&1

echo === run 2: import shape, batch=10000 rows per INSERT ===
"%BIN%\multi_insert.exe" 127.0.0.1:!PORT! "%ROOT%\ticks.csv" --batch-size=10000 --clients=8 --no-bulk --database=main 2>&1

echo === run 3: bulk import mode ===
"%BIN%\multi_insert.exe" 127.0.0.1:!PORT! "%ROOT%\ticks.csv" --batch-size=10000 --clients=8 --bulk --database=main 2>&1

taskkill /f /im taibai.exe >nul 2>&1
echo === note: if a taibai.exe survives, kill it with an elevated shell:
echo ===   taskkill /f /im taibai.exe
echo === data stays in %ROOT% (delete manually if not needed)
echo === DONE ===
exit /b 0

:fail
taskkill /f /im taibai.exe >nul 2>&1
echo BENCH SETUP FAILED
exit /b 1

