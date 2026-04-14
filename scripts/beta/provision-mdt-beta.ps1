[CmdletBinding()]
param(
  [string]$DeploymentShareRoot = 'W:\DeploymentShare',
  [string]$ShareServerName = 'CAPR-MDT-01',
  [string]$SourceTaskSequenceId = 'MDT-MELISSE',
  [string]$DestinationTaskSequenceId = 'MDT-AUTO-MELISSE',
  [string]$DestinationTaskSequenceName = 'MDT-AUTO-Melisse',
  [string]$TaskSequenceGroupName = 'MMA Beta',
  [string]$TechnicianDisplayName = 'Beta',
  [string]$BetaScriptsFolder = 'beta',
  [bool]$InstallRunnerMsi = $true,
  [string]$RunnerPackageRelativePath = 'Scripts\marl\packages\MmaMdtRunner-1.0.0.msi'
)

$ErrorActionPreference = 'Stop'

function Get-TimeStamp {
  return (Get-Date).ToString('yyyyMMdd_HHmmss')
}

function Backup-File {
  param(
    [string]$Path,
    [string]$BackupRoot
  )

  if (-not (Test-Path $Path)) { return }
  $target = Join-Path $BackupRoot ([IO.Path]::GetFileName($Path))
  Copy-Item -Path $Path -Destination $target -Force
}

function Replace-LiteralInFile {
  param(
    [string]$Path,
    [string]$Search,
    [string]$Replacement
  )

  $content = [IO.File]::ReadAllText($Path)
  $updated = $content.Replace($Search, $Replacement)
  [IO.File]::WriteAllText($Path, $updated)
}

function Replace-RegexInFile {
  param(
    [string]$Path,
    [string]$Pattern,
    [string]$Replacement
  )

  $content = [IO.File]::ReadAllText($Path)
  $updated = $content -replace $Pattern, $Replacement
  [IO.File]::WriteAllText($Path, $updated)
}

function Get-OrCreateGroupNode {
  param(
    [xml]$GroupsXml,
    [string]$GroupName
  )

  $group = $GroupsXml.groups.group | Where-Object { $_.Name -eq $GroupName } | Select-Object -First 1
  if ($group) {
    return $group
  }

  $group = $GroupsXml.CreateElement('group')
  [void]$group.SetAttribute('guid', ('{' + [guid]::NewGuid().ToString() + '}'))
  [void]$group.SetAttribute('enable', 'True')

  $name = $GroupsXml.CreateElement('Name')
  $name.InnerText = $GroupName
  [void]$group.AppendChild($name)

  $createdTime = $GroupsXml.CreateElement('CreatedTime')
  $createdTime.InnerText = (Get-Date).ToString('G')
  [void]$group.AppendChild($createdTime)

  $createdBy = $GroupsXml.CreateElement('CreatedBy')
  $createdBy.InnerText = "$env:COMPUTERNAME\$env:USERNAME"
  [void]$group.AppendChild($createdBy)

  $lastModifiedTime = $GroupsXml.CreateElement('LastModifiedTime')
  $lastModifiedTime.InnerText = (Get-Date).ToString('G')
  [void]$group.AppendChild($lastModifiedTime)

  $lastModifiedBy = $GroupsXml.CreateElement('LastModifiedBy')
  $lastModifiedBy.InnerText = "$env:COMPUTERNAME\$env:USERNAME"
  [void]$group.AppendChild($lastModifiedBy)

  [void]$GroupsXml.groups.AppendChild($group)
  return $group
}

function Set-OrCreateVariableNode {
  param(
    [xml]$SequenceXml,
    [string]$Name,
    [string]$Value
  )

  if (-not $SequenceXml.sequence.globalVarList) {
    $globalVarList = $SequenceXml.CreateElement('globalVarList')
    $firstChild = $SequenceXml.sequence.FirstChild
    if ($firstChild) {
      [void]$SequenceXml.sequence.InsertBefore($globalVarList, $firstChild)
    } else {
      [void]$SequenceXml.sequence.AppendChild($globalVarList)
    }
  }

  $existingNode = $null
  foreach ($variableNode in @($SequenceXml.sequence.globalVarList.variable)) {
    if ($variableNode -and $variableNode.name -eq $Name) {
      $existingNode = $variableNode
      break
    }
  }

  if (-not $existingNode) {
    $existingNode = $SequenceXml.CreateElement('variable')
    [void]$existingNode.SetAttribute('name', $Name)
    [void]$existingNode.SetAttribute('property', $Name)
    [void]$SequenceXml.sequence.globalVarList.AppendChild($existingNode)
  }

  $existingNode.InnerText = $Value
}

function New-RunCommandStepNode {
  param(
    [xml]$SequenceXml,
    [string]$StepName,
    [string]$Action,
    [bool]$ContinueOnError = $false,
    [bool]$RunAsUser = $false,
    [string]$RunAsUserName = '',
    [string]$RunAsPassword = ''
  )

  $stepNode = $SequenceXml.CreateElement('step')
  [void]$stepNode.SetAttribute('type', 'SMS_TaskSequence_RunCommandLineAction')
  [void]$stepNode.SetAttribute('name', $StepName)
  [void]$stepNode.SetAttribute('description', '')
  [void]$stepNode.SetAttribute('disable', 'false')
  [void]$stepNode.SetAttribute('continueOnError', $(if ($ContinueOnError) { 'true' } else { 'false' }))
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

function Set-OrCreateRunnerInstallStep {
  param(
    [xml]$SequenceXml,
    [string]$ShareServerName,
    [string]$RunnerPackageRelativePath
  )

  $runnerSharePath = "\\$ShareServerName\DeploymentShare$\" + $RunnerPackageRelativePath.TrimStart('\')
  $installAction = '%WINDIR%\System32\msiexec.exe /i "{0}" /qn /norestart' -f $runnerSharePath

  $allGroups = @($SequenceXml.SelectNodes('//group'))
  $targetGroup = $null
  $executeStep = $null
  foreach ($group in $allGroups) {
    foreach ($step in @($group.step)) {
      if ($step -and [string]$step.name -eq 'execute report script') {
        $targetGroup = $group
        $executeStep = $step
        break
      }
    }
    if ($targetGroup) { break }
  }

  if (-not $targetGroup -or -not $executeStep) {
    throw 'Unable to locate the "execute report script" step in TS.xml.'
  }

  $existingStep = $null
  foreach ($step in @($targetGroup.step)) {
    if ($step -and [string]$step.name -eq 'install MMA MDT Runner') {
      $existingStep = $step
      break
    }
  }

  if (-not $existingStep) {
    $existingStep = New-RunCommandStepNode `
      -SequenceXml $SequenceXml `
      -StepName 'install MMA MDT Runner' `
      -Action $installAction `
      -ContinueOnError $true
    [void]$targetGroup.InsertBefore($existingStep, $executeStep)
  } else {
    [void]$existingStep.SetAttribute('continueOnError', 'true')
    [void]$existingStep.SetAttribute('disable', 'false')
    [void]$existingStep.SetAttribute('successCodeList', '0 3010')
    $actionNode = $existingStep.SelectSingleNode('action')
    if (-not $actionNode) {
      $actionNode = $SequenceXml.CreateElement('action')
      [void]$existingStep.AppendChild($actionNode)
    }
    $actionNode.InnerText = $installAction
  }

  return $runnerSharePath
}

function Set-OrCreateRunnerDesktopCopyStep {
  param(
    [xml]$SequenceXml,
    [string]$ShareServerName,
    [string]$RunnerPackageRelativePath
  )

  $runnerSharePath = "\\$ShareServerName\DeploymentShare$\" + $RunnerPackageRelativePath.TrimStart('\')
  $desktopCopyAction = 'cmd.exe /c if exist "C:\Users\Administrateur\Desktop" copy /Y "{0}" "C:\Users\Administrateur\Desktop\MmaMdtRunner-1.0.0.msi" >nul & if exist "C:\Users\Public\Desktop" copy /Y "{0}" "C:\Users\Public\Desktop\MmaMdtRunner-1.0.0.msi" >nul' -f $runnerSharePath

  $allGroups = @($SequenceXml.SelectNodes('//group'))
  $targetGroup = $null
  $executeStep = $null
  foreach ($group in $allGroups) {
    foreach ($step in @($group.step)) {
      if ($step -and [string]$step.name -eq 'execute report script') {
        $targetGroup = $group
        $executeStep = $step
        break
      }
    }
    if ($targetGroup) { break }
  }

  if (-not $targetGroup -or -not $executeStep) {
    throw 'Unable to locate the "execute report script" step in TS.xml.'
  }

  $installStep = $null
  $copyStep = $null
  foreach ($step in @($targetGroup.step)) {
    if ($step -and [string]$step.name -eq 'install MMA MDT Runner') {
      $installStep = $step
    }
    if ($step -and [string]$step.name -eq 'copy MMA MDT Runner MSI to desktop') {
      $copyStep = $step
    }
  }

  if (-not $copyStep) {
    $copyStep = New-RunCommandStepNode `
      -SequenceXml $SequenceXml `
      -StepName 'copy MMA MDT Runner MSI to desktop' `
      -Action $desktopCopyAction `
      -ContinueOnError $true
    if ($installStep) {
      [void]$targetGroup.InsertBefore($copyStep, $installStep)
    } else {
      [void]$targetGroup.InsertBefore($copyStep, $executeStep)
    }
  } else {
    [void]$copyStep.SetAttribute('continueOnError', 'true')
    [void]$copyStep.SetAttribute('disable', 'false')
    [void]$copyStep.SetAttribute('successCodeList', '0 3010')
    $actionNode = $copyStep.SelectSingleNode('action')
    if (-not $actionNode) {
      $actionNode = $SequenceXml.CreateElement('action')
      [void]$copyStep.AppendChild($actionNode)
    }
    $actionNode.InnerText = $desktopCopyAction
  }

  return $runnerSharePath
}

$controlRoot = Join-Path $DeploymentShareRoot 'Control'
$sourceControlPath = Join-Path $controlRoot $SourceTaskSequenceId
$destinationControlPath = Join-Path $controlRoot $DestinationTaskSequenceId
$taskSequencesPath = Join-Path $controlRoot 'TaskSequences.xml'
$taskSequenceGroupsPath = Join-Path $controlRoot 'TaskSequenceGroups.xml'
$backupRoot = Join-Path $controlRoot ('_beta_backups\' + (Get-TimeStamp))

if (-not (Test-Path $sourceControlPath)) {
  throw "Source task sequence folder not found: $sourceControlPath"
}

New-Item -Path $backupRoot -ItemType Directory -Force | Out-Null
Backup-File -Path $taskSequencesPath -BackupRoot $backupRoot
Backup-File -Path $taskSequenceGroupsPath -BackupRoot $backupRoot

if (Test-Path $destinationControlPath) {
  Remove-Item -Path $destinationControlPath -Recurse -Force
}
Copy-Item -Path $sourceControlPath -Destination $destinationControlPath -Recurse -Force

$destinationTsXmlPath = Join-Path $destinationControlPath 'TS.xml'
$marlScriptBase = "\\$ShareServerName\DeploymentShare$\Scripts\marl\"
$betaScriptBase = "\\$ShareServerName\DeploymentShare$\Scripts\marl\$BetaScriptsFolder\"
# Only redirect the scripts that actually exist in the beta folder.
# All other steps (copy camera, etc.) keep their original prod path.
Replace-RegexInFile -Path $destinationTsXmlPath -Pattern ([regex]::Escape($marlScriptBase) + 'mdt-report-[^"\\]+\.ps1') -Replacement ($betaScriptBase + 'mdt-report-beta.ps1')
Replace-LiteralInFile -Path $destinationTsXmlPath -Search ($marlScriptBase + 'mdt-desktop.ps1') -Replacement ($betaScriptBase + 'mdt-desktop-beta.ps1')
Replace-LiteralInFile -Path $destinationTsXmlPath -Search ($marlScriptBase + 'mdt-laptop.ps1')  -Replacement ($betaScriptBase + 'mdt-laptop-beta.ps1')
Replace-LiteralInFile -Path $destinationTsXmlPath -Search ($marlScriptBase + 'mdt-stress.ps1')  -Replacement ($betaScriptBase + 'mdt-stress-beta.ps1')
# Replace any existing hardcoded desktop report script call in the execute step.
Replace-RegexInFile -Path $destinationTsXmlPath -Pattern 'mdt-report-[^"\\]+\.ps1' -Replacement 'mdt-report-beta.ps1'

[xml]$destinationTsXml = Get-Content $destinationTsXmlPath
Set-OrCreateVariableNode -SequenceXml $destinationTsXml -Name 'MMA_TECHNICIAN' -Value $TechnicianDisplayName
Set-OrCreateVariableNode -SequenceXml $destinationTsXml -Name 'MDT_TECHNICIAN' -Value $TechnicianDisplayName
$runnerSharePath = $null
if ($InstallRunnerMsi) {
  $runnerSharePath = Set-OrCreateRunnerDesktopCopyStep `
    -SequenceXml $destinationTsXml `
    -ShareServerName $ShareServerName `
    -RunnerPackageRelativePath $RunnerPackageRelativePath
  $runnerSharePath = Set-OrCreateRunnerInstallStep `
    -SequenceXml $destinationTsXml `
    -ShareServerName $ShareServerName `
    -RunnerPackageRelativePath $RunnerPackageRelativePath
}
$destinationTsXml.Save($destinationTsXmlPath)

[xml]$taskSequencesXml = Get-Content $taskSequencesPath
[xml]$taskSequenceGroupsXml = Get-Content $taskSequenceGroupsPath

$sourceTaskSequenceNode = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $SourceTaskSequenceId } | Select-Object -First 1
if (-not $sourceTaskSequenceNode) {
  throw "Source task sequence metadata not found for ID $SourceTaskSequenceId"
}

$destinationTaskSequenceNode = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $DestinationTaskSequenceId } | Select-Object -First 1
$newGuid = '{' + [guid]::NewGuid().ToString() + '}'

if (-not $destinationTaskSequenceNode) {
  $destinationTaskSequenceNode = $taskSequencesXml.CreateElement('ts')
  [void]$taskSequencesXml.tss.AppendChild($destinationTaskSequenceNode)
}

[void]$destinationTaskSequenceNode.SetAttribute('guid', $newGuid)
[void]$destinationTaskSequenceNode.SetAttribute('enable', 'True')
[void]$destinationTaskSequenceNode.SetAttribute('hide', 'False')

$childValues = @{
  Name = $DestinationTaskSequenceName
  CreatedTime = (Get-Date).ToString('G')
  CreatedBy = "$env:COMPUTERNAME\$env:USERNAME"
  LastModifiedTime = (Get-Date).ToString('G')
  LastModifiedBy = "$env:COMPUTERNAME\$env:USERNAME"
  ID = $DestinationTaskSequenceId
  Version = [string]$sourceTaskSequenceNode.Version
  TaskSequenceTemplate = [string]$sourceTaskSequenceNode.TaskSequenceTemplate
}

if ($sourceTaskSequenceNode.Comments) {
  $childValues.Comments = [string]$sourceTaskSequenceNode.Comments
}

foreach ($key in $childValues.Keys) {
  $existing = $destinationTaskSequenceNode.SelectSingleNode($key)
  if (-not $existing) {
    $existing = $taskSequencesXml.CreateElement($key)
    [void]$destinationTaskSequenceNode.AppendChild($existing)
  }
  $existing.InnerText = $childValues[$key]
}

$groupNode = Get-OrCreateGroupNode -GroupsXml $taskSequenceGroupsXml -GroupName $TaskSequenceGroupName
$groupNode.Member | Where-Object { $_.'#text' -eq [string]$sourceTaskSequenceNode.guid } | ForEach-Object { [void]$groupNode.RemoveChild($_) }
$groupNode.Member | Where-Object { $_.'#text' -eq [string]$newGuid } | ForEach-Object { [void]$groupNode.RemoveChild($_) }
$memberNode = $taskSequenceGroupsXml.CreateElement('Member')
$memberNode.InnerText = $newGuid
[void]$groupNode.AppendChild($memberNode)

$taskSequencesXml.Save($taskSequencesPath)
$taskSequenceGroupsXml.Save($taskSequenceGroupsPath)

Write-Host "Provisioned beta task sequence:"
Write-Host "  Source:      $SourceTaskSequenceId"
Write-Host "  Destination: $DestinationTaskSequenceId"
Write-Host "  Name:        $DestinationTaskSequenceName"
Write-Host "  Technician:  $TechnicianDisplayName"
Write-Host "  Group:       $TaskSequenceGroupName"
Write-Host "  Backup:      $backupRoot"
if ($runnerSharePath) {
  Write-Host "  Runner MSI:  $runnerSharePath"
}

[pscustomobject]@{
  SourceTaskSequenceId = $SourceTaskSequenceId
  DestinationTaskSequenceId = $DestinationTaskSequenceId
  DestinationTaskSequenceName = $DestinationTaskSequenceName
  TechnicianDisplayName = $TechnicianDisplayName
  TaskSequenceGroupName = $TaskSequenceGroupName
  DeploymentShareRoot = $DeploymentShareRoot
  ControlPath = $destinationControlPath
  TaskSequencesPath = $taskSequencesPath
  TaskSequenceGroupsPath = $taskSequenceGroupsPath
  BackupPath = $backupRoot
  ScriptsFolder = $BetaScriptsFolder
  RunnerPackageRelativePath = $RunnerPackageRelativePath
  RunnerSharePath = $runnerSharePath
}
