; Velox — Inno Setup script (Windows installer)
; Build with: ISCC.exe packaging\inno\velox.iss   (after `cargo build --release`)

#define AppName "Velox"
#define AppVersion "1.0.0.0"
#define AppPublisher "Velox Engineering"
#define AppExe "velox-gui.exe"

[Setup]
AppId={{8C21B0E4-3A6B-4B0F-9E39-VELOX00000001}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\Velox
DefaultGroupName=Velox
UninstallDisplayIcon={app}\{#AppExe}
OutputBaseFilename=VeloxSetup-1.0.0
OutputDir=..\..\dist
Compression=lzma2/max
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=lowest
WizardStyle=modern

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Shortcuts:"

[Files]
Source: "..\..\target\release\velox-gui.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\target\release\velox.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\extension\*"; DestDir: "{app}\extension"; Flags: ignoreversion recursesubdirs
Source: "..\..\README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\LICENSE"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Velox"; Filename: "{app}\{#AppExe}"
Name: "{autodesktop}\Velox"; Filename: "{app}\{#AppExe}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExe}"; Description: "Launch Velox"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; user downloads are intentionally preserved
Type: filesandordirs; Name: "{app}\extension"
