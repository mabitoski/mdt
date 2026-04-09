[CmdletBinding()]
param(
  [string]$ConfigPath,
  [switch]$Force
)

Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'fog-common.ps1')

function Ensure-Directory {
  param([string]$Path)
  if (-not $Path) { return }
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Write-FirstBootLog {
  param(
    [string]$Path,
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  Add-Content -Path $Path -Value "[$timestamp][$Level] $Message"
}

$bootstrapRoot = if ($env:FOG_BOOTSTRAP_ROOT) {
  $env:FOG_BOOTSTRAP_ROOT
} else {
  Join-Path $env:ProgramData 'MMA\FogBootstrap'
}
$stateDir = Join-Path $bootstrapRoot 'state'
$logsDir = Join-Path $bootstrapRoot 'Logs'
$artifactsDir = Join-Path $bootstrapRoot 'Artifacts'
$outboxDir = Join-Path $bootstrapRoot 'Outbox'

Ensure-Directory -Path $bootstrapRoot
Ensure-Directory -Path $stateDir
Ensure-Directory -Path $logsDir
Ensure-Directory -Path $artifactsDir
Ensure-Directory -Path $outboxDir

$markerPath = Join-Path $stateDir 'firstboot.done'
$firstbootLogPath = Join-Path $logsDir 'fog-firstboot.log'
$reportLogPath = Join-Path $logsDir ("fog-report-{0}.log" -f (Get-Date).ToString('yyyyMMdd-HHmmss'))
$payloadOutputPath = Join-Path $stateDir 'last-payload.json'

Write-FirstBootLog -Path $firstbootLogPath -Message "Starting FOG first boot runner (force=$Force)."

if ((Test-Path -Path $markerPath) -and -not $Force) {
  Write-FirstBootLog -Path $firstbootLogPath -Message "Marker already present: $markerPath. Skipping."
  return
}

$config = Import-FogConfig -Path $ConfigPath
$explicitParameters = @{
  LogPath = $reportLogPath
  ArtifactRoot = $artifactsDir
  OutboxRoot = $outboxDir
  PayloadOutputPath = $payloadOutputPath
}

if (-not $config.Values.ContainsKey('QueueOnUploadFailure')) {
  $explicitParameters.QueueOnUploadFailure = $true
}
if (-not $config.Values.ContainsKey('SkipKeyboardCapture')) {
  $explicitParameters.SkipKeyboardCapture = $true
}

try {
  Invoke-FogReportCore -ConfigPath $ConfigPath -ExplicitParameters $explicitParameters
  Set-Content -Path $markerPath -Value (Get-Date).ToString('o') -Encoding ASCII
  Write-FirstBootLog -Path $firstbootLogPath -Message 'FOG first boot runner completed successfully.'
} catch {
  Write-FirstBootLog -Path $firstbootLogPath -Message $_.Exception.Message -Level 'ERROR'
  throw
}
