[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$ReportPath,
  [string]$ApiUrl = $env:MDT_API_URL
)

$ErrorActionPreference = 'Stop'

$processPath = Join-Path $env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe'
$logPath = Join-Path (Join-Path $env:WINDIR 'Temp') 'mdt-report-bootstrap.log'

function Write-BootstrapLog {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[{0}][{1}] {2}" -f $timestamp, $Level, $Message
  try { Add-Content -Path $logPath -Value $line } catch { }
  try { Write-Host $line } catch { }
}

function Get-BootstrapLogExcerpt {
  if (-not (Test-Path -LiteralPath $logPath)) {
    return $null
  }
  try {
    $lines = @(Get-Content -LiteralPath $logPath -Tail 120 -ErrorAction Stop)
    $text = $lines -join "`n"
    if ($text.Length -gt 4000) {
      $text = $text.Substring($text.Length - 4000)
    }
    return $text
  } catch {
    return $null
  }
}

function Send-BootstrapFailureReport {
  param([string]$Message)

  $targetUrl = if ($ApiUrl) { $ApiUrl } else { 'http://10.1.10.27:3000/api/ingest' }
  $payload = [ordered]@{
    reportId = [guid]::NewGuid().ToString('D')
    hostname = $env:COMPUTERNAME
    technician = $env:MDT_TECHNICIAN
    tag = $env:MDT_REPORT_TAG
    scriptVersion = 'bootstrap-1.0.0'
    diag = [ordered]@{
      appVersion = 'bootstrap-1.0.0'
      completedAt = (Get-Date).ToUniversalTime().ToString('o')
      status = 'bootstrap_failed'
      unhandledException = $true
      message = $Message
    }
  }

  $excerpt = Get-BootstrapLogExcerpt
  if ($excerpt) {
    $payload.reportLogs = [ordered]@{
      capturedAt = (Get-Date).ToUniversalTime().ToString('o')
      files = @(
        [ordered]@{
          kind = 'bootstrap'
          label = 'Bootstrap'
          path = $logPath
          excerpt = $excerpt
        }
      )
    }
  }

  try {
    $json = $payload | ConvertTo-Json -Depth 8
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    Invoke-RestMethod -Uri $targetUrl -Method Post -ContentType 'application/json; charset=utf-8' -Body $bytes -TimeoutSec 20 | Out-Null
    Write-BootstrapLog "Bootstrap failure report sent to $targetUrl" 'WARN'
  } catch {
    Write-BootstrapLog "Bootstrap failure report failed: $($_.Exception.Message)" 'WARN'
  }
}

$exclusionAdded = $false

try {
  if (-not (Test-Path -LiteralPath $ReportPath)) {
    throw "Report script not found: $ReportPath"
  }

  Add-MpPreference -ExclusionProcess $processPath -ErrorAction Stop
  $exclusionAdded = $true
  Write-BootstrapLog "Defender process exclusion added for $processPath"
  Write-BootstrapLog "Launching MDT report script: $ReportPath"

  & $processPath -NoLogo -NoProfile -ExecutionPolicy Bypass -File $ReportPath
  $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
  Write-BootstrapLog "MDT report script finished with exit code $exitCode"
  exit $exitCode
} catch {
  Write-BootstrapLog "Bootstrap failed: $($_.Exception.Message)" 'ERROR'
  Send-BootstrapFailureReport -Message $_.Exception.Message
  throw
} finally {
  if ($exclusionAdded) {
    try {
      Remove-MpPreference -ExclusionProcess $processPath -ErrorAction Stop
      Write-BootstrapLog "Defender process exclusion removed for $processPath"
    } catch {
      Write-BootstrapLog "Unable to remove Defender process exclusion: $($_.Exception.Message)" 'WARN'
    }
  }
}
