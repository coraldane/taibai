# Windows installation

Taibai can be installed on Windows in the same style as MySQL, Oracle, and DB2: the installer copies the server files, writes an instance configuration file, registers a Windows service, and sets the service to start automatically with Windows.

## Build

```powershell
cargo build --release
```

## Script installation

Run PowerShell as Administrator:

```powershell
.\scripts\windows\install.ps1 `
  -InstallDir "D:\Program Files\Taibai" `
  -ServiceName "TaibaiDB" `
  -Port 5151
```

The script creates:

- `D:\Program Files\Taibai\bin\taibai.exe`
- `D:\Program Files\Taibai\conf\windows.yaml`
- `D:\Program Files\Taibai\data`
- `D:\Program Files\Taibai\logs`
- `D:\Program Files\Taibai\logs\taibai.log`
- the `TaibaiDB` Windows service

The service command line contains:

```text
taibai.exe --service --config "D:\Program Files\Taibai\conf\windows.yaml"
```

After installation, Taibai reads the data directory from `windows.yaml`, so the user does not need to pass install or data directories again. If `-DataDir` or `-ConfigDir` is omitted, the script uses the selected installation directory.

## Service management

```powershell
Start-Service TaibaiDB
Stop-Service TaibaiDB
Restart-Service TaibaiDB
Get-Service TaibaiDB
```

The service is registered with automatic startup, so it starts with Windows.
Service logs are written to the `log.file` path in `conf\windows.yaml`; for the default installer layout this is `[INSTALLFOLDER]\logs\taibai.log`.

## Uninstall

```powershell
.\scripts\windows\uninstall.ps1 -ServiceName "TaibaiDB" -InstallDir "D:\Program Files\Taibai" -RemoveFiles
```

Data is preserved by default. Add `-RemoveData` only when the database files and configuration should be deleted from the installation directory.

## MSI installer

The WiX project is under `installer/windows`. Build it with:

```powershell
.\installer\windows\build.ps1
```

This requires WiX Toolset on PATH. The MSI shows an installation folder selection page. The data directory can still be set from the command line with the public `DATADIR` property:

```powershell
$version = (Select-String -Path .\Cargo.toml -Pattern '^version\s*=\s*"([^"]+)"').Matches[0].Groups[1].Value
msiexec /i ".\target\release\Taibai-$version.msi" INSTALLFOLDER="D:\Program Files\Taibai"
```

If `DATADIR` is not specified, the installer uses `[INSTALLFOLDER]data`. The MSI writes the service configuration to `[INSTALLFOLDER]conf\windows.yaml`, so installing Taibai on `D:` also keeps the default configuration, data, and logs under the selected installation directory.
