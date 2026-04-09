[CmdletBinding()]
param(
  [string]$ConfigPath,
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode,
  [string]$ApiUrl,
  [string]$Technician,
  [string]$LogPath,
  [string]$ArtifactRoot,
  [string]$OutboxRoot,
  [string]$PayloadOutputPath,
  [string]$ReportTag,
  [string]$ReportTagId,
  [switch]$QueueOnUploadFailure,
  [switch]$SkipRawUpload,
  [switch]$SkipTlsValidation,
  [switch]$SkipDebugWinRM,
  [switch]$SkipKeyboardCapture,
  [switch]$FactoryReset,
  [string]$FactoryResetConfirm,
  [switch]$SkipFactoryResetPrompt
)

. (Join-Path $PSScriptRoot 'fog-common.ps1')

$explicitParameters = ConvertTo-FogHashtable -BoundParameters $PSBoundParameters -Exclude @('ConfigPath')
Invoke-FogReportCore -ConfigPath $ConfigPath -FixedParameters @{ Category = 'desktop' } -ExplicitParameters $explicitParameters
