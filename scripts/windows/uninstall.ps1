param(
    [string]$InstallDir = "C:\Program Files\Taibai",
    [string]$ServiceName = "TaibaiDB",
    [switch]$RemoveFiles,
    [switch]$RemoveData,
    [string]$DataDir = "C:\ProgramData\Taibai\data",
    [string]$ConfigDir = "C:\ProgramData\Taibai\conf"
)

$ErrorActionPreference = "Stop"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Administrator privileges are required"
}

$exe = Join-Path $InstallDir "bin\taibai.exe"

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Stop-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if (Test-Path $exe) {
        & $exe --uninstall-service --service-name $ServiceName
    } else {
        sc.exe delete $ServiceName | Out-Null
    }
}

if ($RemoveFiles -and (Test-Path $InstallDir)) {
    Remove-Item -Recurse -Force $InstallDir
}

if ($RemoveData -and (Test-Path $DataDir)) {
    Remove-Item -Recurse -Force $DataDir
}

if ($RemoveData -and (Test-Path $ConfigDir)) {
    Remove-Item -Recurse -Force $ConfigDir
}

Write-Host "Taibai service $ServiceName uninstalled"
