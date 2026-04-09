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

function Get-ReportScriptSharePathFromCopyStep {
  param([xml]$SequenceXml)

  $copyActionNode = $SequenceXml.SelectSingleNode("//step[@name='copy report']/action")
  if (-not $copyActionNode -or [string]::IsNullOrWhiteSpace($copyActionNode.InnerText)) {
    return $null
  }

  $match = [regex]::Match($copyActionNode.InnerText, 'xcopy\s+"([^"]+\.ps1)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if (-not $match.Success) {
    return $null
  }

  return $match.Groups[1].Value
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

function Get-BootstrapSharePathFromCopyStep {
  param([xml]$SequenceXml)

  $reportSharePath = Get-ReportScriptSharePathFromCopyStep -SequenceXml $SequenceXml
  if (-not $reportSharePath) {
    return $null
  }

  return (Join-Path (Split-Path -Path $reportSharePath -Parent) 'mdt-report-bootstrap.ps1')
}

function Get-BootstrapLocalPath {
  return 'C:\Windows\Temp\mdt-report-bootstrap.ps1'
}

function Get-ExecuteReportStepContext {
  param([xml]$SequenceXml)

  foreach ($group in @($SequenceXml.SelectNodes('//group'))) {
    foreach ($step in @($group.step)) {
      if ($step -and [string]$step.name -eq 'execute report script') {
        return @{
          Group = $group
          Step = $step
        }
      }
    }
  }

  return $null
}

function New-RunCommandLineStep {
  param(
    [xml]$SequenceXml,
    [string]$StepName,
    [string]$Action,
    [bool]$RunAsUser = $false,
    [string]$RunAsUserName = '',
    [string]$RunAsPassword = ''
  )

  $stepNode = $SequenceXml.CreateElement('step')
  [void]$stepNode.SetAttribute('type', 'SMS_TaskSequence_RunCommandLineAction')
  [void]$stepNode.SetAttribute('name', $StepName)
  [void]$stepNode.SetAttribute('description', '')
  [void]$stepNode.SetAttribute('disable', 'false')
  [void]$stepNode.SetAttribute('continueOnError', 'false')
  [void]$stepNode.SetAttribute('startIn', '')
  [void]$stepNode.SetAttribute('successCodeList', '0 3010')
  [void]$stepNode.SetAttribute('runIn', 'WinPEandFullOS')

  $defaultVarList = $SequenceXml.CreateElement('defaultVarList')
  [void]$stepNode.AppendChild($defaultVarList)

  $variables = @(
    @{ name = 'PackageID'; property = 'PackageID'; value = $null },
    @{ name = 'RunAsUser'; property = 'RunAsUser'; value = $(if ($RunAsUser) { 'true' } else { 'false' }) },
    @{ name = 'SMSTSRunCommandLineUserName'; property = 'SMSTSRunCommandLineUserName'; value = $RunAsUserName },
    @{ name = 'SMSTSRunCommandLineUserPassword'; property = 'SMSTSRunCommandLineUserPassword'; value = $RunAsPassword },
    @{ name = 'LoadProfile'; property = 'LoadProfile'; value = 'false' }
  )

  foreach ($variable in $variables) {
    $variableNode = $SequenceXml.CreateElement('variable')
    [void]$variableNode.SetAttribute('name', [string]$variable.name)
    [void]$variableNode.SetAttribute('property', [string]$variable.property)
    if ($null -ne $variable.value) {
      $variableNode.InnerText = [string]$variable.value
    }
    [void]$defaultVarList.AppendChild($variableNode)
  }

  $actionNode = $SequenceXml.CreateElement('action')
  $actionNode.InnerText = $Action
  [void]$stepNode.AppendChild($actionNode)
  return $stepNode
}

function Get-DefenderBootstrapAction {
  param(
    [string]$ReportScriptPath,
    [string]$BootstrapLocalPath
  )

  if ([string]::IsNullOrWhiteSpace($ReportScriptPath)) {
    throw 'Report script path is required to build the Defender bootstrap action.'
  }
  if ([string]::IsNullOrWhiteSpace($BootstrapLocalPath)) {
    throw 'Bootstrap script path is required to build the Defender bootstrap action.'
  }

  $escapedReportScriptPath = $ReportScriptPath.Replace('"', '""')
  $escapedBootstrapLocalPath = $BootstrapLocalPath.Replace('"', '""')
  return ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File "{0}" -ReportPath "{1}"' -f $escapedBootstrapLocalPath, $escapedReportScriptPath)
}

function Set-OrCreateReportBootstrapCopyStep {
  param([xml]$SequenceXml)

  $bootstrapSharePath = Get-BootstrapSharePathFromCopyStep -SequenceXml $SequenceXml
  if (-not $bootstrapSharePath) {
    throw 'Unable to resolve the bootstrap share path from the copy report step.'
  }

  $bootstrapLocalPath = Get-BootstrapLocalPath
  $bootstrapLocalDirectory = Split-Path -Path $bootstrapLocalPath -Parent
  $copyAction = 'xcopy "{0}" "{1}\" /Y /I' -f $bootstrapSharePath, $bootstrapLocalDirectory

  $context = Get-ExecuteReportStepContext -SequenceXml $SequenceXml
  if (-not $context) {
    throw 'Unable to locate the "execute report script" step in TS.xml.'
  }

  $existingStep = $SequenceXml.SelectSingleNode("//step[@name='copy report bootstrap']")
  if ($existingStep) {
    $actionNode = $existingStep.SelectSingleNode('action')
    if (-not $actionNode) {
      $actionNode = $SequenceXml.CreateElement('action')
      [void]$existingStep.AppendChild($actionNode)
    }
    $actionNode.InnerText = $copyAction
    return @{
      SharePath = $bootstrapSharePath
      LocalPath = $bootstrapLocalPath
    }
  }

  $stepNode = New-RunCommandLineStep -SequenceXml $SequenceXml -StepName 'copy report bootstrap' -Action $copyAction
  [void]$context.Group.InsertBefore($stepNode, $context.Step)
  return @{
    SharePath = $bootstrapSharePath
    LocalPath = $bootstrapLocalPath
  }
}

function Set-ExecuteReportStepWithDefenderBootstrap {
  param([xml]$SequenceXml)

  $context = Get-ExecuteReportStepContext -SequenceXml $SequenceXml
  if (-not $context) {
    throw 'Unable to locate the "execute report script" step in TS.xml.'
  }
  $executeStep = $context.Step

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

  $bootstrapInfo = Set-OrCreateReportBootstrapCopyStep -SequenceXml $SequenceXml
  $actionNode.InnerText = Get-DefenderBootstrapAction -ReportScriptPath $reportScriptPath -BootstrapLocalPath $bootstrapInfo.LocalPath
  return @{
    ReportScriptPath = $reportScriptPath
    BootstrapSharePath = $bootstrapInfo.SharePath
    BootstrapLocalPath = $bootstrapInfo.LocalPath
  }
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
  $bootstrapInfo = Set-ExecuteReportStepWithDefenderBootstrap -SequenceXml $sequenceXml
  $sequenceXml.Save($tsPath)

  $executeActionNode = $sequenceXml.SelectSingleNode("//step[@name='execute report script']/action")
  Write-Host ("Updated {0}" -f $taskSequenceId)
  Write-Host ("  ReportScript: {0}" -f $bootstrapInfo.ReportScriptPath)
  Write-Host ("  BootstrapShare: {0}" -f $bootstrapInfo.BootstrapSharePath)
  Write-Host ("  BootstrapLocal: {0}" -f $bootstrapInfo.BootstrapLocalPath)
  Write-Host ("  ExecuteAction: {0}" -f $executeActionNode.InnerText)
}

Write-Host ("BackupRoot: {0}" -f $backupRoot)
