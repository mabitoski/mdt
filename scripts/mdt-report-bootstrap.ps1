[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$ReportPath,
  [string]$ApiUrl = $env:MDT_API_URL
)

$ErrorActionPreference = 'Stop'

$processPath = Join-Path $env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe'
$logPath = Join-Path (Join-Path $env:WINDIR 'Temp') 'mdt-report-bootstrap.log'
$scriptVersion = 'bootstrap-1.0.1'

function Add-BootstrapLogLine {
  param([string]$Line)

  if (-not $logPath -or -not $Line) {
    return $false
  }

  try {
    $logDir = Split-Path -Path $logPath -Parent
    if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
      New-Item -Path $logDir -ItemType Directory -Force | Out-Null
    }
  } catch { }

  for ($attempt = 1; $attempt -le 6; $attempt++) {
    $stream = $null
    $writer = $null
    try {
      $stream = [System.IO.File]::Open($logPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::ReadWrite)
      [void]$stream.Seek(0, [System.IO.SeekOrigin]::End)
      $writer = New-Object System.IO.StreamWriter($stream, [System.Text.UTF8Encoding]::new($false))
      $writer.WriteLine($Line)
      $writer.Flush()
      return $true
    } catch {
      if ($attempt -ge 6) {
        return $false
      }
      Start-Sleep -Milliseconds (75 * $attempt)
    } finally {
      if ($writer) {
        $writer.Dispose()
      } elseif ($stream) {
        $stream.Dispose()
      }
    }
  }

  return $false
}

function Write-BootstrapLog {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[{0}][{1}] {2}" -f $timestamp, $Level, $Message
  try { Add-BootstrapLogLine -Line $line | Out-Null } catch { }
  try { Write-Host $line } catch { }
}

function Get-TextExcerpt {
  param(
    [string]$Path,
    [int]$TailLines = 120,
    [int]$MaxChars = 4000
  )

  if (-not $Path -or -not (Test-Path -LiteralPath $Path)) {
    return $null
  }
  try {
    $lines = @(Get-Content -LiteralPath $Path -Tail $TailLines -ErrorAction Stop)
    $text = $lines -join "`n"
    if ($text.Length -gt $MaxChars) {
      $text = $text.Substring($text.Length - $MaxChars)
    }
    return $text
  } catch {
    return $null
  }
}

function Write-BootstrapExcerpt {
  param(
    [string]$Label,
    [string]$Path,
    [int]$TailLines = 80,
    [int]$MaxChars = 2500
  )

  $excerpt = Get-TextExcerpt -Path $Path -TailLines $TailLines -MaxChars $MaxChars
  if (-not $excerpt) {
    return
  }

  Write-BootstrapLog "$Label excerpt begin" 'WARN'
  foreach ($line in @($excerpt -split "`r?`n")) {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    Write-BootstrapLog "$Label> $line" 'WARN'
  }
  Write-BootstrapLog "$Label excerpt end" 'WARN'
}

function Send-BootstrapFailureReport {
  param([string]$Message)

  $targetUrl = if ($ApiUrl) { $ApiUrl } else { 'http://10.1.10.27:3000/api/ingest' }
  $payload = [ordered]@{
    reportId = [guid]::NewGuid().ToString('D')
    hostname = $env:COMPUTERNAME
    technician = $env:MDT_TECHNICIAN
    tag = $env:MDT_REPORT_TAG
    scriptVersion = $scriptVersion
    diag = [ordered]@{
      appVersion = $scriptVersion
      completedAt = (Get-Date).ToUniversalTime().ToString('o')
      status = 'bootstrap_failed'
      unhandledException = $true
      message = $Message
    }
  }

  $excerpt = Get-TextExcerpt -Path $logPath -TailLines 120 -MaxChars 4000
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

  $workingDirectory = Split-Path -Path $ReportPath -Parent
  if (-not $workingDirectory) {
    $workingDirectory = $env:TEMP
  }
  $runToken = [guid]::NewGuid().ToString('N')
  $childStdoutPath = Join-Path $env:TEMP ("mdt-report-bootstrap.{0}.stdout.log" -f $runToken)
  $childStderrPath = Join-Path $env:TEMP ("mdt-report-bootstrap.{0}.stderr.log" -f $runToken)

  $childArgs = @('-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ReportPath)
  $childProcess = Start-Process `
    -FilePath $processPath `
    -ArgumentList $childArgs `
    -WorkingDirectory $workingDirectory `
    -WindowStyle Hidden `
    -PassThru `
    -Wait `
    -RedirectStandardOutput $childStdoutPath `
    -RedirectStandardError $childStderrPath

  Write-BootstrapExcerpt -Label 'Report stderr' -Path $childStderrPath
  $exitCode = if ($childProcess -and $null -ne $childProcess.ExitCode) { [int]$childProcess.ExitCode } elseif ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
  Write-BootstrapLog "MDT report script finished with exit code $exitCode"
  if ($exitCode -ne 0) {
    throw "Report script failed with exit code $exitCode"
  }
  exit $exitCode
} catch {
  Write-BootstrapLog "Bootstrap failed: $($_.Exception.Message)" 'ERROR'
  Send-BootstrapFailureReport -Message $_.Exception.Message
  exit 1
} finally {
  if ($exclusionAdded) {
    try {
      Remove-MpPreference -ExclusionProcess $processPath -ErrorAction Stop
      Write-BootstrapLog "Defender process exclusion removed for $processPath"
    } catch {
      Write-BootstrapLog "Unable to remove Defender process exclusion: $($_.Exception.Message)" 'WARN'
    }
  }
  foreach ($tempLog in @($childStdoutPath, $childStderrPath)) {
    try {
      if ($tempLog -and (Test-Path -LiteralPath $tempLog)) {
        Remove-Item -LiteralPath $tempLog -Force -ErrorAction SilentlyContinue
      }
    } catch { }
  }
}
