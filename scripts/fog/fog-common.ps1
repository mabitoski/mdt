[CmdletBinding()]
param()

Set-StrictMode -Version Latest

function Get-FogCoreScriptPath {
  $scriptsRoot = Split-Path -Path $PSScriptRoot -Parent
  return (Join-Path $scriptsRoot 'mdt-report.ps1')
}

function ConvertTo-FogHashtable {
  param(
    [System.Collections.IDictionary]$BoundParameters,
    [string[]]$Exclude = @()
  )

  $result = @{}
  if (-not $BoundParameters) {
    return $result
  }

  foreach ($entry in $BoundParameters.GetEnumerator()) {
    if ($Exclude -contains [string]$entry.Key) {
      continue
    }
    if ($null -eq $entry.Value) {
      continue
    }
    $result[[string]$entry.Key] = $entry.Value
  }

  return $result
}

function Get-FogConfigCandidates {
  $candidates = @()
  foreach ($value in @(
    $env:FOG_CONFIG_PATH,
    (Join-Path $PSScriptRoot 'fog-bootstrap.config.psd1'),
    (Join-Path (Split-Path -Path $PSScriptRoot -Parent) 'fog-bootstrap.config.psd1'),
    (Join-Path (Split-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -Parent) 'fog-bootstrap.config.psd1'),
    (Join-Path $PSScriptRoot 'fog-bootstrap.config.sample.psd1')
  )) {
    if ([string]::IsNullOrWhiteSpace($value)) {
      continue
    }
    if ($candidates -contains $value) {
      continue
    }
    $candidates += $value
  }

  return $candidates
}

function Import-FogConfig {
  param([string]$Path)

  $candidates = @()
  if (-not [string]::IsNullOrWhiteSpace($Path)) {
    $candidates += $Path
  }
  $candidates += Get-FogConfigCandidates

  foreach ($candidate in $candidates) {
    if ([string]::IsNullOrWhiteSpace($candidate)) {
      continue
    }
    if (-not (Test-Path -Path $candidate)) {
      continue
    }
    $data = Import-PowerShellDataFile -Path $candidate
    if ($data -is [System.Collections.IDictionary]) {
      return @{
        Path = $candidate
        Values = @{} + $data
      }
    }
    throw "FOG config file must return a hashtable: $candidate"
  }

  return @{
    Path = $null
    Values = @{}
  }
}

function Convert-FogBoolean {
  param([object]$Value)

  if ($null -eq $Value) { return $null }
  if ($Value -is [bool]) { return [bool]$Value }
  $text = ([string]$Value).Trim().ToLowerInvariant()
  if (-not $text) { return $null }
  if ($text -in @('1', 'true', 'yes', 'on')) { return $true }
  if ($text -in @('0', 'false', 'no', 'off')) { return $false }
  return $null
}

function Convert-FogStringArray {
  param([object]$Value)

  if ($null -eq $Value) { return $null }
  if ($Value -is [string[]]) {
    return @($Value | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  }
  if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
    return @($Value | ForEach-Object { [string]$_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  }

  $text = ([string]$Value).Trim()
  if (-not $text) {
    return $null
  }

  if ($text.StartsWith('[')) {
    try {
      $json = ConvertFrom-Json -InputObject $text -ErrorAction Stop
      if ($json -is [System.Collections.IEnumerable] -and -not ($json -is [string])) {
        return @($json | ForEach-Object { [string]$_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
      }
    } catch {
    }
  }

  $items = @($text -split '[,\r\n]+' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
  if ($items.Count -eq 0) {
    return @($text)
  }
  return $items
}

function Get-FogEnvironmentParameters {
  $definitions = @(
    @{ Param = 'ApiUrl'; Names = @('FOG_API_URL', 'MDT_API_URL'); Type = 'string' }
    @{ Param = 'Category'; Names = @('FOG_CATEGORY'); Type = 'string' }
    @{ Param = 'TestMode'; Names = @('FOG_TEST_MODE'); Type = 'string' }
    @{ Param = 'Technician'; Names = @('FOG_TECHNICIAN', 'MDT_TECHNICIAN'); Type = 'string' }
    @{ Param = 'ArtifactRoot'; Names = @('FOG_ARTIFACT_ROOT', 'MDT_ARTIFACT_ROOT'); Type = 'string' }
    @{ Param = 'OutboxRoot'; Names = @('FOG_OUTBOX_ROOT', 'MDT_OUTBOX_ROOT'); Type = 'string' }
    @{ Param = 'PayloadOutputPath'; Names = @('FOG_PAYLOAD_OUTPUT_PATH'); Type = 'string' }
    @{ Param = 'ReportTag'; Names = @('FOG_REPORT_TAG', 'MDT_REPORT_TAG'); Type = 'string' }
    @{ Param = 'ReportTagId'; Names = @('FOG_REPORT_TAG_ID', 'MDT_REPORT_TAG_ID'); Type = 'string' }
    @{ Param = 'ObjectStorageEndpoint'; Names = @('FOG_OBJECT_STORAGE_ENDPOINT', 'MDT_OBJECT_STORAGE_ENDPOINT'); Type = 'string' }
    @{ Param = 'ObjectStorageBucket'; Names = @('FOG_OBJECT_STORAGE_BUCKET', 'MDT_OBJECT_STORAGE_BUCKET'); Type = 'string' }
    @{ Param = 'ObjectStorageAccessKey'; Names = @('FOG_OBJECT_STORAGE_ACCESS_KEY', 'MDT_OBJECT_STORAGE_ACCESS_KEY'); Type = 'string' }
    @{ Param = 'ObjectStorageSecretKey'; Names = @('FOG_OBJECT_STORAGE_SECRET_KEY', 'MDT_OBJECT_STORAGE_SECRET_KEY'); Type = 'string' }
    @{ Param = 'ObjectStoragePrefix'; Names = @('FOG_OBJECT_STORAGE_PREFIX', 'MDT_OBJECT_STORAGE_PREFIX'); Type = 'string' }
    @{ Param = 'ObjectStorageMcPath'; Names = @('FOG_OBJECT_STORAGE_MC_PATH', 'MDT_OBJECT_STORAGE_MC_PATH'); Type = 'string' }
    @{ Param = 'CameraTestPath'; Names = @('FOG_CAMERA_TEST_PATH', 'MDT_CAMERA_TEST_PATH'); Type = 'string' }
    @{ Param = 'CameraTestArguments'; Names = @('FOG_CAMERA_TEST_ARGS', 'MDT_CAMERA_TEST_ARGS'); Type = 'array' }
    @{ Param = 'KeyboardCapturePath'; Names = @('FOG_KEYBOARD_CAPTURE_PATH'); Type = 'string' }
    @{ Param = 'KeyboardCaptureLogPath'; Names = @('FOG_KEYBOARD_CAPTURE_LOG_PATH'); Type = 'string' }
    @{ Param = 'QueueOnUploadFailure'; Names = @('FOG_QUEUE_ON_UPLOAD_FAILURE', 'MDT_QUEUE_ON_UPLOAD_FAILURE'); Type = 'bool' }
    @{ Param = 'SkipRawUpload'; Names = @('FOG_SKIP_RAW_UPLOAD', 'MDT_SKIP_RAW_UPLOAD'); Type = 'bool' }
    @{ Param = 'SkipTlsValidation'; Names = @('FOG_SKIP_TLS_VALIDATION'); Type = 'bool' }
    @{ Param = 'SkipDebugWinRM'; Names = @('FOG_SKIP_DEBUG_WINRM', 'MDT_SKIP_DEBUG_WINRM'); Type = 'bool' }
    @{ Param = 'SkipKeyboardCapture'; Names = @('FOG_SKIP_KEYBOARD_CAPTURE'); Type = 'bool' }
    @{ Param = 'FailTaskSequenceOnError'; Names = @('FOG_FAIL_ON_REPORT_ERROR', 'MDT_FAIL_TS_ON_REPORT_ERROR'); Type = 'bool' }
    @{ Param = 'CpuTestArguments'; Names = @('FOG_CPU_TEST_ARGS', 'MDT_CPU_TEST_ARGS'); Type = 'array' }
    @{ Param = 'GpuTestArguments'; Names = @('FOG_GPU_TEST_ARGS', 'MDT_GPU_TEST_ARGS'); Type = 'array' }
    @{ Param = 'NetworkTestExtraArgs'; Names = @('FOG_IPERF_ARGS', 'MDT_IPERF_ARGS'); Type = 'array' }
  )

  $result = @{}

  foreach ($definition in $definitions) {
    $rawValue = $null
    foreach ($name in $definition.Names) {
      $candidate = [Environment]::GetEnvironmentVariable($name)
      if ([string]::IsNullOrWhiteSpace($candidate)) {
        continue
      }
      $rawValue = $candidate
      break
    }

    if ($null -eq $rawValue) {
      continue
    }

    switch ($definition.Type) {
      'bool' {
        $value = Convert-FogBoolean -Value $rawValue
        if ($null -ne $value) {
          $result[$definition.Param] = $value
        }
      }
      'array' {
        $value = Convert-FogStringArray -Value $rawValue
        if ($value) {
          $result[$definition.Param] = $value
        }
      }
      default {
        $result[$definition.Param] = [string]$rawValue
      }
    }
  }

  return $result
}

function Invoke-FogReportCore {
  param(
    [string]$ConfigPath,
    [hashtable]$FixedParameters,
    [hashtable]$ExplicitParameters
  )

  $coreScriptPath = Get-FogCoreScriptPath
  if (-not (Test-Path -Path $coreScriptPath)) {
    throw "Core MDT report script not found: $coreScriptPath"
  }

  $config = Import-FogConfig -Path $ConfigPath
  $parameters = @{}

  foreach ($source in @(
      $config.Values,
      (Get-FogEnvironmentParameters),
      $ExplicitParameters,
      $FixedParameters
    )) {
    if (-not $source) {
      continue
    }
    foreach ($key in $source.Keys) {
      $value = $source[$key]
      if ($null -eq $value) {
        continue
      }
      $parameters[$key] = $value
    }
  }

  & $coreScriptPath @parameters
}
