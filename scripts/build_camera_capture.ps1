[CmdletBinding()]
param(
  [string]$Python = 'python',
  [string]$Source = (Join-Path $PSScriptRoot 'camera_capture.py'),
  [string]$OutputDir = $PSScriptRoot
)

if (-not (Test-Path $Source)) {
  Write-Error "Source introuvable: $Source"
  exit 1
}

& $Python -m pip install --upgrade pip pyinstaller | Out-Null
& $Python -m PyInstaller --clean --onefile --name camera_capture --distpath $OutputDir --workpath (Join-Path $PSScriptRoot 'build') $Source
