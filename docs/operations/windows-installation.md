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
  -InstallDir "C:\Program Files\Taibai" `
  -DataDir "D:\TaibaiData" `
  -ServiceName "TaibaiDB" `
  -Port 5151
```

The script creates:

- `C:\Program Files\Taibai\bin\taibai.exe`
- `C:\ProgramData\Taibai\conf\windows.yaml`
- the selected data directory
- the `TaibaiDB` Windows service

The service command line contains:

```text
taibai.exe --service --config "C:\ProgramData\Taibai\conf\windows.yaml"
```

After installation, Taibai reads the data directory from `windows.yaml`, so the user does not need to pass install or data directories again.

## Service management

```powershell
Start-Service TaibaiDB
Stop-Service TaibaiDB
Restart-Service TaibaiDB
Get-Service TaibaiDB
```

The service is registered with automatic startup, so it starts with Windows.

## Uninstall

```powershell
.\scripts\windows\uninstall.ps1 -ServiceName "TaibaiDB" -InstallDir "C:\Program Files\Taibai" -RemoveFiles
```

Data is preserved by default. Add `-RemoveData -DataDir "D:\TaibaiData"` only when the database files should be deleted.

## MSI installer

The WiX project is under `installer/windows`. Build it with:

```powershell
.\installer\windows\build.ps1
```

This requires WiX Toolset on PATH. The MSI shows an installation folder selection page. The data directory can be set from the command line with the public `DATADIR` property:

```powershell
msiexec /i .\target\release\Taibai-0.1.0.msi INSTALLFOLDER="C:\Program Files\Taibai" DATADIR="D:\TaibaiData"
```

If `DATADIR` is not specified, the installer uses `C:\ProgramData\Taibai\data`. The MSI writes the service configuration to `C:\ProgramData\Taibai\conf\windows.yaml`, so the service can keep the selected data directory independently from the installation directory.
