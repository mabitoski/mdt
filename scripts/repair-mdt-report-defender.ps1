[CmdletBinding()]
param(
  [string]$DeploymentShareRoot = 'W:\DeploymentShare',
  [string[]]$TaskSequenceIds,
  [switch]$IncludeHiddenTemplates
)

$ErrorActionPreference = 'Stop'

function Convert-ToPowerShellEncodedCommand {
  param([string]$Script)

  $bytes = [Text.Encoding]::Unicode.GetBytes($Script)
  return [Convert]::ToBase64String($bytes)
}

function Get-TimeStamp {
  return (Get-Date).ToString('yyyyMMdd-HHmmss')
}

function Get-TaskSequenceIdsFromControl {
  param([string]$ControlRoot)

  $ids = @()
  foreach ($directory in @(Get-ChildItem -Path $ControlRoot -Directory -ErrorAction Stop)) {
    $tsPath = Join-Path $directory.FullName 'TS.xml'
    if (-not (Test-Path $tsPath)) {
      continue
    }

    [xml]$sequenceXml = Get-Content $tsPath
    if (-not $sequenceXml.SelectSingleNode("//step[@name='execute report script']")) {
      continue
    }

    if (-not $IncludeHiddenTemplates) {
      $tsMetaPath = Join-Path $ControlRoot 'TaskSequences.xml'
      if (Test-Path $tsMetaPath) {
        [xml]$taskSequencesXml = Get-Content $tsMetaPath
        $tsMeta = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $directory.Name } | Select-Object -First 1
        if ($tsMeta -and [string]$tsMeta.Hide -eq 'True') {
          continue
        }
      }
    }

    $ids += $directory.Name
  }

  return @($ids | Sort-Object -Unique)
}

function Get-ReportScriptPathFromCopyStep {
  param([xml]$SequenceXml)

  $copyActionNode = $SequenceXml.SelectSingleNode("//step[@name='copy report']/action")
  if (-not $copyActionNode -or [string]::IsNullOrWhiteSpace($copyActionNode.InnerText)) {
    return $null
  }

  $match = [regex]::Match($copyActionNode.InnerText, 'xcopy\s+"[^"]*\\([^\\"]+\.ps1)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if (-not $match.Success) {
    return $null
  }

  return ('C:\Users\Administrateur\Desktop\{0}' -f $match.Groups[1].Value)
}

function Get-ReportScriptPathFromExecuteStep {
  param([xml]$SequenceXml)

  $executeActionNode = $SequenceXml.SelectSingleNode("//step[@name='execute report script']/action")
  if (-not $executeActionNode -or [string]::IsNullOrWhiteSpace($executeActionNode.InnerText)) {
    return $null
  }

  $match = [regex]::Match($executeActionNode.InnerText, '-File\s+"([^"]+)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if ($match.Success) {
    return $match.Groups[1].Value
  }

  $fallbackMatch = [regex]::Match($executeActionNode.InnerText, '-File\s+([^\s]+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if ($fallbackMatch.Success) {
    return $fallbackMatch.Groups[1].Value.Trim('"')
  }

  return $null
}

function Get-DefenderBootstrapAction {
  param([string]$ReportScriptPath)

  if ([string]::IsNullOrWhiteSpace($ReportScriptPath)) {
    throw 'Report script path is required to build the Defender bootstrap action.'
  }

  $escapedReportScriptPath = $ReportScriptPath.Replace("'", "''")
  $bootstrapScript = @"
`$ErrorActionPreference = 'Stop'
`$processPath = Join-Path `$env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe'
`$reportPath = '$escapedReportScriptPath'
`$logPath = Join-Path (Join-Path `$env:WINDIR 'Temp') 'mdt-report-bootstrap.log'

function Write-BootstrapLog {
  param(
    [string]`$Message,
    [string]`$Level = 'INFO'
  )

  `$timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  `$line = "[`$timestamp][`$Level] `$Message"
  try { Add-Content -Path `$logPath -Value `$line } catch { }
  try { Write-Host `$line } catch { }
}

`$exclusionAdded = `$false

try {
  if (-not (Test-Path -LiteralPath `$reportPath)) {
    throw "Report script not found: `$reportPath"
  }

  Add-MpPreference -ExclusionProcess `$processPath -ErrorAction Stop
  `$exclusionAdded = `$true
  Write-BootstrapLog "Defender process exclusion added for `$processPath"
  Write-BootstrapLog "Launching MDT report script: `$reportPath"

  & `$processPath -NoLogo -NoProfile -ExecutionPolicy Bypass -File `$reportPath
  `$exitCode = if (`$null -eq `$LASTEXITCODE) { 0 } else { [int]`$LASTEXITCODE }
  Write-BootstrapLog "MDT report script finished with exit code `$exitCode"
  exit `$exitCode
} catch {
  Write-BootstrapLog "Bootstrap failed: `$(`$_.Exception.Message)" 'ERROR'
  throw
} finally {
  if (`$exclusionAdded) {
    try {
      Remove-MpPreference -ExclusionProcess `$processPath -ErrorAction Stop
      Write-BootstrapLog "Defender process exclusion removed for `$processPath"
    } catch {
      Write-BootstrapLog "Unable to remove Defender process exclusion: `$(`$_.Exception.Message)" 'WARN'
    }
  }
}
"@

  $encodedCommand = Convert-ToPowerShellEncodedCommand -Script $bootstrapScript
  return ('%TOOLROOT%\bddrun.exe /runas powershell -NoProfile -ExecutionPolicy Bypass -EncodedCommand {0}' -f $encodedCommand)
}

function Set-ExecuteReportStepWithDefenderBootstrap {
  param([xml]$SequenceXml)

  $executeStep = $SequenceXml.SelectSingleNode("//step[@name='execute report script']")
  if (-not $executeStep) {
    throw 'Unable to locate the "execute report script" step in TS.xml.'
  }

  $actionNode = $executeStep.SelectSingleNode('action')
  if (-not $actionNode) {
    $actionNode = $SequenceXml.CreateElement('action')
    [void]$executeStep.AppendChild($actionNode)
  }

  $reportScriptPath = Get-ReportScriptPathFromCopyStep -SequenceXml $SequenceXml
  if (-not $reportScriptPath) {
    $reportScriptPath = Get-ReportScriptPathFromExecuteStep -SequenceXml $SequenceXml
  }
  if (-not $reportScriptPath) {
    throw 'Unable to resolve the local report script path for the execute report script step.'
  }

  $actionNode.InnerText = Get-DefenderBootstrapAction -ReportScriptPath $reportScriptPath
  return $reportScriptPath
}

$controlRoot = Join-Path $DeploymentShareRoot 'Control'
$backupRoot = Join-Path $controlRoot ('_backups\defender-bootstrap-' + (Get-TimeStamp))
New-Item -Path $backupRoot -ItemType Directory -Force | Out-Null

if (-not $TaskSequenceIds -or $TaskSequenceIds.Count -eq 0) {
  $TaskSequenceIds = Get-TaskSequenceIdsFromControl -ControlRoot $controlRoot
}

foreach ($taskSequenceId in $TaskSequenceIds) {
  $tsPath = Join-Path $controlRoot ("{0}\TS.xml" -f $taskSequenceId)
  if (-not (Test-Path $tsPath)) {
    Write-Warning "Task sequence not found: $tsPath"
    continue
  }

  $backupPath = Join-Path $backupRoot ("{0}.TS.xml" -f $taskSequenceId)
  Copy-Item -Path $tsPath -Destination $backupPath -Force

  [xml]$sequenceXml = Get-Content $tsPath
  $reportScriptPath = Set-ExecuteReportStepWithDefenderBootstrap -SequenceXml $sequenceXml
  $sequenceXml.Save($tsPath)

  $executeActionNode = $sequenceXml.SelectSingleNode("//step[@name='execute report script']/action")
  Write-Host ("Updated {0}" -f $taskSequenceId)
  Write-Host ("  ReportScript: {0}" -f $reportScriptPath)
  Write-Host ("  ExecuteAction: {0}" -f $executeActionNode.InnerText)
}

Write-Host ("BackupRoot: {0}" -f $backupRoot)
