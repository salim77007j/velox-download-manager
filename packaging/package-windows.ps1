# Package a release build of Velox for Windows (portable zip).
# Run from repo root in PowerShell:  powershell -File packaging\package-windows.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$VERSION = (cargo metadata --no-deps --format-version 1 | ConvertFrom-Json).packages[0].version
$OUT = "dist\velox-$VERSION-windows-x86_64"
if (Test-Path $OUT) { Remove-Item -Recurse -Force $OUT }
New-Item -ItemType Directory -Force -Path $OUT | Out-Null

cargo build --release -p velox-gui -p velox-cli
Copy-Item target\release\velox-gui.exe, target\release\velox.exe $OUT\
Copy-Item README.md, LICENSE $OUT\
Copy-Item extension $OUT\extension -Recurse
New-Item -ItemType Directory -Force -Path "$OUT\docs" | Out-Null
Copy-Item docs\*.md "$OUT\docs\"

Compress-Archive -Path $OUT -DestinationPath "dist\velox-$VERSION-windows-x86_64.zip" -Force
Write-Host "packaged: dist\velox-$VERSION-windows-x86_64.zip"
Write-Host "installer: build with Inno Setup -> packaging\inno\velox.iss"
