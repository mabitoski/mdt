[CmdletBinding()]
param(
  [string]$Python = 'python',
  [string]$Source = (Join-Path $PSScriptRoot 'camera_capture.py'),
  [string]$SetupScript = (Join-Path $PSScriptRoot 'setup_py2exe.py'),
  [string]$DistDir = (Join-Path $PSScriptRoot 'dist-py2exe'),
  [string]$OutputExe = (Join-Path $PSScriptRoot 'camera_capture.exe'),
  [switch]$Clean
)

if (-not (Test-Path $Source)) {
  Write-Error "Source introuvable: $Source"
  exit 1
}

if (-not (Test-Path $SetupScript)) {
  Write-Error "Setup introuvable: $SetupScript"
  exit 1
}

if ($Clean) {
  if (Test-Path $DistDir) {
    Remove-Item -Recurse -Force -Path $DistDir
  }
  $buildDir = Join-Path $PSScriptRoot 'build'
  if (Test-Path $buildDir) {
    Remove-Item -Recurse -Force -Path $buildDir
  }
}

& $Python -m pip install --upgrade pip py2exe opencv-python | Out-Null

$current = Get-Location
Set-Location $PSScriptRoot
try {
  & $Python $SetupScript py2exe --dist-dir $DistDir
} finally {
  Set-Location $current
}

$builtExe = Join-Path $DistDir 'camera_capture.exe'
if (-not (Test-Path $builtExe)) {
  Write-Error "Exe introuvable: $builtExe"
  exit 1
}

Copy-Item -Force -Path $builtExe -Destination $OutputExe
Write-Host "Exe genere: $OutputExe"
