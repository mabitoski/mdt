[CmdletBinding()]
param(
  [string]$ApiUrl = $env:MDT_API_URL,
  [string]$OutboxRoot = $env:MDT_OUTBOX_ROOT,
  [int]$TimeoutSec = 20,
  [int]$MaxAttempts = 10,
  [string]$LogPath,
  [switch]$SkipTlsValidation
)

function Ensure-Directory {
  param([string]$Path)
  if (-not $Path) { return $null }
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
  return $Path
}

function Resolve-OutboxRoot {
  param([string]$Value)

  $root = $Value
  if (-not $root) {
    if ($env:ProgramData) {
      $root = Join-Path $env:ProgramData 'MMA\MdtRunner\Outbox'
    } elseif ($env:TEMP) {
      $root = Join-Path $env:TEMP 'MMA\MdtRunner\Outbox'
    } else {
      $root = Join-Path $PSScriptRoot 'outbox'
    }
  }

  try {
    $root = [Environment]::ExpandEnvironmentVariables($root)
  } catch { }

  return Ensure-Directory -Path $root
}

function Write-JsonFile {
  param(
    [string]$Path,
    [object]$Object,
    [int]$Depth = 10
  )

  if (-not $Path) { return $false }
  try {
    $dir = Split-Path -Path $Path -Parent
    if ($dir) { Ensure-Directory -Path $dir | Out-Null }
    $Object | ConvertTo-Json -Depth $Depth | Out-File -FilePath $Path -Encoding UTF8 -Force
    return $true
  } catch {
    Write-Warning "Write JSON failed ($Path): $($_.Exception.Message)"
    return $false
  }
}

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $line = "[{0}][{1}] {2}" -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'), $Level, $Message
  Write-Host $line
  if ($LogPath) {
    try {
      $dir = Split-Path -Path $LogPath -Parent
      if ($dir) { Ensure-Directory -Path $dir | Out-Null }
      Add-Content -Path $LogPath -Value $line
    } catch { }
  }
}

function Invoke-JsonPost {
  param(
    [string]$Url,
    [string]$Json,
    [int]$Timeout
  )

  $bytes = [System.Text.Encoding]::UTF8.GetBytes($Json)
  return Invoke-RestMethod -Uri $Url -Method Post -ContentType 'application/json; charset=utf-8' -Body $bytes -TimeoutSec $Timeout
}

if ($SkipTlsValidation) {
  try {
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
  } catch { }
}

$resolvedOutbox = Resolve-OutboxRoot -Value $OutboxRoot
$pendingDir = Ensure-Directory -Path (Join-Path $resolvedOutbox 'pending')
$sentDir = Ensure-Directory -Path (Join-Path $resolvedOutbox 'sent')
$failedDir = Ensure-Directory -Path (Join-Path $resolvedOutbox 'failed')

$entries = Get-ChildItem -Path $pendingDir -Directory -ErrorAction SilentlyContinue | Sort-Object Name
$summary = [ordered]@{
  outboxRoot = $resolvedOutbox
  scanned = @($entries).Count
  sent = 0
  keptPending = 0
  failed = 0
}

foreach ($entry in $entries) {
  $payloadPath = Join-Path $entry.FullName 'payload.json'
  $metaPath = Join-Path $entry.FullName 'meta.json'
  if (-not (Test-Path -Path $payloadPath)) {
    Write-Log "Entree invalide sans payload: $($entry.Name)" 'WARN'
    Move-Item -Path $entry.FullName -Destination (Join-Path $failedDir $entry.Name) -Force
    $summary.failed++
    continue
  }

  $meta = @{}
  if (Test-Path -Path $metaPath) {
    try {
      $meta = Get-Content -Path $metaPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    } catch {
      $meta = @{}
    }
  }

  $targetApiUrl = if ($ApiUrl) { $ApiUrl } elseif ($meta.apiUrl) { [string]$meta.apiUrl } else { $null }
  if (-not $targetApiUrl) {
    Write-Log "Aucune ApiUrl disponible pour $($entry.Name)." 'WARN'
    $summary.keptPending++
    continue
  }

  $json = Get-Content -Path $payloadPath -Raw -ErrorAction Stop
  try {
    Write-Log "Envoi de l'entree $($entry.Name) vers $targetApiUrl"
    $response = Invoke-JsonPost -Url $targetApiUrl -Json $json -Timeout $TimeoutSec
    $meta.status = 'sent'
    $meta.sentAt = (Get-Date).ToUniversalTime().ToString('o')
    if ($response -and $response.reportId) { $meta.remoteReportId = $response.reportId }
    Write-JsonFile -Path $metaPath -Object $meta | Out-Null
    Move-Item -Path $entry.FullName -Destination (Join-Path $sentDir $entry.Name) -Force
    Write-Log "Entree $($entry.Name) synchronisee avec succes." 'INFO'
    $summary.sent++
  } catch {
    $attempts = 0
    try { $attempts = [int]$meta.attempts } catch { $attempts = 0 }
    $attempts++
    $meta.attempts = $attempts
    $meta.lastAttemptAt = (Get-Date).ToUniversalTime().ToString('o')
    $meta.lastError = $_.Exception.Message
    $meta.status = if ($attempts -ge $MaxAttempts) { 'failed' } else { 'pending' }
    Write-JsonFile -Path $metaPath -Object $meta | Out-Null

    if ($attempts -ge $MaxAttempts) {
      Write-Log "Entree $($entry.Name) deplacee en echec apres $attempts tentatives: $($_.Exception.Message)" 'ERROR'
      Move-Item -Path $entry.FullName -Destination (Join-Path $failedDir $entry.Name) -Force
      $summary.failed++
    } else {
      Write-Log "Echec sync $($entry.Name), conservee en attente (tentative $attempts): $($_.Exception.Message)" 'WARN'
      $summary.keptPending++
    }
  }
}

Write-Log ("Resume sync: pending_scanned={0} sent={1} kept={2} failed={3}" -f $summary.scanned, $summary.sent, $summary.keptPending, $summary.failed)
$summary | ConvertTo-Json -Depth 5
