Set-StrictMode -Version Latest

function Expand-MdtRunnerPath {
  param(
    [Parameter(Mandatory = $true)][string]$Value,
    [string]$BasePath = $PSScriptRoot
  )

  $expanded = $Value
  try {
    $expanded = [Environment]::ExpandEnvironmentVariables($Value)
  } catch { }

  if ([System.IO.Path]::IsPathRooted($expanded)) {
    return $expanded
  }
  return (Join-Path $BasePath $expanded)
}

function Ensure-MdtRunnerDirectory {
  param([Parameter(Mandatory = $true)][string]$Path)

  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
  return $Path
}

function Get-MdtRunnerConfig {
  param([string]$RootPath = $PSScriptRoot)

  $configPath = Join-Path $RootPath 'config.json'
  if (-not (Test-Path -Path $configPath)) {
    $configPath = Join-Path $RootPath 'config.sample.json'
  }

  if (-not (Test-Path -Path $configPath)) {
    throw "Config introuvable: $configPath"
  }

  $raw = Get-Content -Path $configPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  $config = [ordered]@{
    apiUrl = [string]$raw.apiUrl
    scriptsDir = Expand-MdtRunnerPath -Value ([string]$raw.scriptsDir) -BasePath $RootPath
    reportScript = [string]$raw.reportScript
    syncScript = [string]$raw.syncScript
    outboxRoot = Expand-MdtRunnerPath -Value ([string]$raw.outboxRoot) -BasePath $RootPath
    logsRoot = Expand-MdtRunnerPath -Value ([string]$raw.logsRoot) -BasePath $RootPath
    defaultCategory = if ($raw.defaultCategory) { [string]$raw.defaultCategory } else { 'auto' }
    defaultTestMode = if ($raw.defaultTestMode) { [string]$raw.defaultTestMode } else { 'quick' }
    skipTlsValidation = [bool]$raw.skipTlsValidation
    autoSyncOnStart = [bool]$raw.autoSyncOnStart
    syncTimeoutSec = if ($raw.syncTimeoutSec) { [int]$raw.syncTimeoutSec } else { 20 }
    title = if ($raw.title) { [string]$raw.title } else { 'MMA MDT Runner' }
  }

  $config.reportScriptPath = Join-Path $config.scriptsDir $config.reportScript
  $config.syncScriptPath = Join-Path $config.scriptsDir $config.syncScript

  Ensure-MdtRunnerDirectory -Path $config.outboxRoot | Out-Null
  Ensure-MdtRunnerDirectory -Path $config.logsRoot | Out-Null
  Ensure-MdtRunnerDirectory -Path (Join-Path $config.outboxRoot 'pending') | Out-Null
  Ensure-MdtRunnerDirectory -Path (Join-Path $config.outboxRoot 'sent') | Out-Null
  Ensure-MdtRunnerDirectory -Path (Join-Path $config.outboxRoot 'failed') | Out-Null

  return [pscustomobject]$config
}

function Get-MdtRunnerTechnicians {
  param([string]$RootPath = $PSScriptRoot)

  $path = Join-Path $RootPath 'technicians.json'
  if (-not (Test-Path -Path $path)) {
    return @('Aedan', 'Antoine', 'Antony', 'Lana', 'Mathis', 'Melisse', 'Remi', 'Tom')
  }

  $content = Get-Content -Path $path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  $list = @($content.technicians) | Where-Object { $_ } | ForEach-Object { [string]$_ }
  return $list | Sort-Object -Unique
}

function Get-MdtRunnerOutboxItems {
  param([Parameter(Mandatory = $true)][string]$OutboxRoot)

  $pending = Join-Path $OutboxRoot 'pending'
  if (-not (Test-Path -Path $pending)) { return @() }

  $items = @()
  foreach ($dir in Get-ChildItem -Path $pending -Directory -ErrorAction SilentlyContinue | Sort-Object Name) {
    $metaPath = Join-Path $dir.FullName 'meta.json'
    $meta = $null
    if (Test-Path -Path $metaPath) {
      try {
        $meta = Get-Content -Path $metaPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      } catch { }
    }
    $items += [pscustomobject]@{
      Name = $dir.Name
      Technician = if ($meta) { [string]$meta.technician } else { '' }
      Hostname = if ($meta) { [string]$meta.hostname } else { '' }
      QueuedAt = if ($meta) { [string]$meta.queuedAt } else { '' }
      Attempts = if ($meta -and $meta.attempts -ne $null) { [int]$meta.attempts } else { 0 }
      LastError = if ($meta) { [string]$meta.lastError } else { '' }
      Path = $dir.FullName
    }
  }

  return $items
}

function Get-MdtRunnerOutboxStats {
  param([Parameter(Mandatory = $true)][string]$OutboxRoot)

  $countDirs = {
    param($Path)
    if (-not (Test-Path -Path $Path)) { return 0 }
    return @(Get-ChildItem -Path $Path -Directory -ErrorAction SilentlyContinue).Count
  }

  return [pscustomobject]@{
    Pending = & $countDirs (Join-Path $OutboxRoot 'pending')
    Sent = & $countDirs (Join-Path $OutboxRoot 'sent')
    Failed = & $countDirs (Join-Path $OutboxRoot 'failed')
  }
}

function New-MdtRunnerLogPath {
  param(
    [Parameter(Mandatory = $true)][string]$LogsRoot,
    [Parameter(Mandatory = $true)][string]$Prefix
  )

  Ensure-MdtRunnerDirectory -Path $LogsRoot | Out-Null
  $name = '{0}-{1}.log' -f $Prefix, (Get-Date).ToString('yyyyMMdd-HHmmss')
  return Join-Path $LogsRoot $name
}

function New-MdtRunnerProcessArguments {
  param(
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [Parameter(Mandatory = $true)][string]$ApiUrl,
    [Parameter(Mandatory = $true)][string]$Technician,
    [Parameter(Mandatory = $true)][string]$Category,
    [Parameter(Mandatory = $true)][string]$TestMode,
    [Parameter(Mandatory = $true)][string]$LogPath,
    [Parameter(Mandatory = $true)][string]$OutboxRoot,
    [switch]$SkipTlsValidation,
    [switch]$FactoryReset
  )

  $args = @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $ScriptPath,
    '-ApiUrl', $ApiUrl,
    '-Technician', $Technician,
    '-Category', $Category,
    '-TestMode', $TestMode,
    '-LogPath', $LogPath,
    '-OutboxRoot', $OutboxRoot,
    '-QueueOnUploadFailure'
  )

  if ($SkipTlsValidation) {
    $args += '-SkipTlsValidation'
  }
  if ($FactoryReset) {
    $args += @('-FactoryReset', '-FactoryResetConfirm', 'RESET', '-SkipFactoryResetPrompt')
  }

  return $args
}
