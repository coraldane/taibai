$ErrorActionPreference = "Continue"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Please run this script from an elevated Administrator PowerShell."
}

Stop-Service TaibaiDB -ErrorAction SilentlyContinue
sc.exe delete TaibaiDB | Out-Host

$apps = Get-ItemProperty `
    "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*", `
    "HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*" `
    -ErrorAction SilentlyContinue |
    Where-Object { $_.DisplayName -eq "Taibai Database" }

foreach ($app in $apps) {
    Write-Host "Uninstalling MSI product $($app.PSChildName)"
    Start-Process msiexec.exe -ArgumentList "/x $($app.PSChildName) /qn /L*V `"$env:TEMP\taibai-uninstall.log`"" -Wait
}

Remove-Item "C:\Program Files (x86)\Taibai" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "C:\Program Files\Taibai" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "C:\ProgramData\Taibai" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Taibai cleanup completed."
