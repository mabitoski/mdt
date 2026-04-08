[CmdletBinding()]
param(
  [string]$Version = '1.0.0',
  [string]$OutputDir = (Join-Path $PSScriptRoot 'dist')
)

$payloadRoot = Join-Path $OutputDir 'payload'
$scriptsRoot = Join-Path $payloadRoot 'scripts'

if (Test-Path -Path $OutputDir) {
  Remove-Item -Path $OutputDir -Recurse -Force
}

New-Item -Path $scriptsRoot -ItemType Directory -Force | Out-Null

$root = Split-Path -Parent $PSScriptRoot

Copy-Item -Path (Join-Path $PSScriptRoot 'MmaMdtRunner.ps1') -Destination $payloadRoot -Force
Copy-Item -Path (Join-Path $PSScriptRoot 'MmaMdtRunner.Common.ps1') -Destination $payloadRoot -Force
Copy-Item -Path (Join-Path $PSScriptRoot 'config.sample.json') -Destination $payloadRoot -Force
Copy-Item -Path (Join-Path $PSScriptRoot 'config.sample.json') -Destination (Join-Path $payloadRoot 'config.json') -Force
Copy-Item -Path (Join-Path $PSScriptRoot 'technicians.json') -Destination $payloadRoot -Force
Copy-Item -Path (Join-Path $PSScriptRoot 'Register-MmaMdtRunnerSyncTask.ps1') -Destination $payloadRoot -Force

$scriptFiles = @(
  'mdt-report.ps1',
  'mdt-laptop.ps1',
  'mdt-desktop.ps1',
  'mdt-stress.ps1',
  'mdt-outbox-sync.ps1',
  'keyboard_capture.ps1',
  'camera.exe'
)

foreach ($file in $scriptFiles) {
  Copy-Item -Path (Join-Path $root "scripts\$file") -Destination $scriptsRoot -Force
}

$wix = Get-Command wix -ErrorAction SilentlyContinue
if (-not $wix) {
  throw 'WiX v4 introuvable. Installe wix puis relance build-msi.ps1.'
}

$wxsPath = Join-Path $PSScriptRoot 'installer\MmaMdtRunner.wxs'
$msiPath = Join-Path $OutputDir ("MmaMdtRunner-{0}.msi" -f $Version)

& $wix.Source build `
  $wxsPath `
  -d Version=$Version `
  -d PayloadRoot=$payloadRoot `
  -o $msiPath

Write-Output $msiPath
