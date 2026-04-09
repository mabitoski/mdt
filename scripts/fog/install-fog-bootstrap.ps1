[CmdletBinding()]
param(
  [string]$DestinationRoot = (Join-Path $env:ProgramData 'MMA\FogBootstrap'),
  [string]$ApiUrl = 'http://10.1.10.27:3000/api/ingest',
  [string]$Technician = 'Remi',
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode = 'quick',
  [switch]$EnableSetupComplete
)

Set-StrictMode -Version Latest

function Ensure-Directory {
  param([string]$Path)
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Copy-RequiredFile {
  param(
    [string]$Source,
    [string]$Destination
  )

  if (-not (Test-Path -Path $Source)) {
    throw "Required file not found: $Source"
  }
  $destinationDir = Split-Path -Path $Destination -Parent
  Ensure-Directory -Path $destinationDir
  Copy-Item -Path $Source -Destination $Destination -Force
}

$repoScriptsRoot = Split-Path -Path $PSScriptRoot -Parent
$destinationScriptsRoot = Join-Path $DestinationRoot 'scripts'
$destinationFogRoot = Join-Path $destinationScriptsRoot 'fog'
$destinationLogsRoot = Join-Path $DestinationRoot 'Logs'
$destinationStateRoot = Join-Path $DestinationRoot 'state'
$destinationArtifactsRoot = Join-Path $DestinationRoot 'Artifacts'
$destinationOutboxRoot = Join-Path $DestinationRoot 'Outbox'

Ensure-Directory -Path $DestinationRoot
Ensure-Directory -Path $destinationScriptsRoot
Ensure-Directory -Path $destinationFogRoot
Ensure-Directory -Path $destinationLogsRoot
Ensure-Directory -Path $destinationStateRoot
Ensure-Directory -Path $destinationArtifactsRoot
Ensure-Directory -Path $destinationOutboxRoot

$filesToCopy = @{
  (Join-Path $repoScriptsRoot 'mdt-report.ps1') = (Join-Path $destinationScriptsRoot 'mdt-report.ps1')
  (Join-Path $repoScriptsRoot 'mdt-outbox-sync.ps1') = (Join-Path $destinationScriptsRoot 'mdt-outbox-sync.ps1')
  (Join-Path $repoScriptsRoot 'keyboard_capture.ps1') = (Join-Path $destinationScriptsRoot 'keyboard_capture.ps1')
  (Join-Path $repoScriptsRoot 'camera.exe') = (Join-Path $destinationScriptsRoot 'camera.exe')
  (Join-Path $PSScriptRoot 'fog-common.ps1') = (Join-Path $destinationFogRoot 'fog-common.ps1')
  (Join-Path $PSScriptRoot 'fog-report.ps1') = (Join-Path $destinationFogRoot 'fog-report.ps1')
  (Join-Path $PSScriptRoot 'fog-desktop.ps1') = (Join-Path $destinationFogRoot 'fog-desktop.ps1')
  (Join-Path $PSScriptRoot 'fog-laptop.ps1') = (Join-Path $destinationFogRoot 'fog-laptop.ps1')
  (Join-Path $PSScriptRoot 'fog-stress.ps1') = (Join-Path $destinationFogRoot 'fog-stress.ps1')
  (Join-Path $PSScriptRoot 'fog-firstboot.ps1') = (Join-Path $destinationFogRoot 'fog-firstboot.ps1')
}

foreach ($source in $filesToCopy.Keys) {
  Copy-RequiredFile -Source $source -Destination $filesToCopy[$source]
}

$configPath = Join-Path $DestinationRoot 'fog-bootstrap.config.psd1'
$configContent = @"
@{
  ApiUrl = '$ApiUrl'
  Category = '$Category'
  TestMode = '$TestMode'
  Technician = '$Technician'
  QueueOnUploadFailure = `$true
  SkipKeyboardCapture = `$true
  SkipDebugWinRM = `$false
  ArtifactRoot = '$destinationArtifactsRoot'
  OutboxRoot = '$destinationOutboxRoot'
  ReportTag = 'En cours'
}
"@
Set-Content -Path $configPath -Value $configContent -Encoding ASCII

if ($EnableSetupComplete) {
  $setupScriptsRoot = Join-Path $env:WINDIR 'Setup\Scripts'
  Ensure-Directory -Path $setupScriptsRoot
  $setupCompletePath = Join-Path $setupScriptsRoot 'SetupComplete.cmd'
  if (Test-Path -Path $setupCompletePath) {
    Copy-Item -Path $setupCompletePath -Destination ($setupCompletePath + '.bak') -Force
  }
  $cmd = @"
@echo off
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$DestinationRoot\scripts\fog\fog-firstboot.ps1" -ConfigPath "$configPath"
exit /b %ERRORLEVEL%
"@
  Set-Content -Path $setupCompletePath -Value $cmd -Encoding ASCII
}

Write-Host "FOG bootstrap installed in $DestinationRoot"
Write-Host "Config: $configPath"
if ($EnableSetupComplete) {
  Write-Host 'SetupComplete enabled.'
}
