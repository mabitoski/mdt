[CmdletBinding()]
param(
  [ValidateSet('prod', 'dev')][string]$Target = 'prod',
  [string]$ApiUrl,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode = 'quick',
  [string]$Technician = 'Admin',
  [string]$ArtifactRoot,
  [switch]$SkipRawUpload,
  [switch]$SkipElevation,
  [switch]$SkipKeyboardCapture,
  [switch]$SkipWinSatDataStore,
  [switch]$SkipGpuAssessment,
  [switch]$SkipTlsValidation,
  [Parameter(ValueFromRemainingArguments = $true)][object[]]$ExtraArgs
)

$scriptPath = Join-Path $PSScriptRoot 'mdt-report.ps1'

if (-not $ApiUrl) {
  $ApiUrl = if ($Target -eq 'dev') {
    'http://10.1.10.27:3001/api/ingest'
  } else {
    'http://10.1.10.27:3000/api/ingest'
  }
}

if (-not $ArtifactRoot) {
  $ArtifactRoot = $PSScriptRoot
}
if (-not (Test-Path $ArtifactRoot)) {
  New-Item -ItemType Directory -Path $ArtifactRoot -Force | Out-Null
}
$logPath = Join-Path $ArtifactRoot 'mdt-report.log'
try { New-Item -ItemType File -Path $logPath -Force | Out-Null } catch { }
try { New-Item -ItemType File -Path $logPath -Force | Out-Null } catch { }

$params = @{
  ApiUrl = $ApiUrl
  Category = $Category
  TestMode = $TestMode
  Technician = $Technician
  ArtifactRoot = $ArtifactRoot
  LogPath = $logPath
  SkipRawUpload = $SkipRawUpload
  SkipElevation = $SkipElevation
  SkipKeyboardCapture = $SkipKeyboardCapture
  SkipWinSatDataStore = $SkipWinSatDataStore
  SkipGpuAssessment = $SkipGpuAssessment
}
if ($SkipTlsValidation) { $params.SkipTlsValidation = $true }

& $scriptPath @params @ExtraArgs
