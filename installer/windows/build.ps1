param(
    [switch]$AcceptWixEula
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$wixCommand = Get-Command wix -ErrorAction SilentlyContinue
$wixPath = $null

if ($wixCommand) {
    $wixPath = $wixCommand.Source
} else {
    $defaultWixPath = "C:\Program Files\WiX Toolset v7.0\bin\wix.exe"
    if (Test-Path $defaultWixPath) {
        $wixPath = $defaultWixPath
    }
}

if (-not $wixPath) {
    throw "wix.exe not found. Install WiX Toolset CLI or add wix.exe to PATH."
}

if ($AcceptWixEula) {
    & $wixPath eula accept wix7
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to accept WiX v7 EULA with exit code $LASTEXITCODE"
    }
}

$uiExtensionInstalled = & $wixPath extension list --global | Select-String "WixToolset.UI.wixext"
if (-not $uiExtensionInstalled) {
    & $wixPath extension add --global WixToolset.UI.wixext
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install WixToolset.UI.wixext with exit code $LASTEXITCODE. If WiX reports WIX7015, accept the WiX v7 OSMF EULA first or rerun this script with -AcceptWixEula."
    }
}

Push-Location $root
try {
    cargo build --release
    & $wixPath build -arch x64 -ext WixToolset.UI.wixext installer\windows\taibai.wxs -o target\release\Taibai-0.1.0.msi
    if ($LASTEXITCODE -ne 0) {
        throw "WiX build failed with exit code $LASTEXITCODE"
    }
    Write-Host "MSI created: target\release\Taibai-0.1.0.msi"
} finally {
    Pop-Location
}
