[CmdletBinding()]
param(
  [string]$DeploymentShareRoot = 'W:\DeploymentShare',
  [string]$ShareServerName = 'CAPR-MDT-01',
  [string[]]$TaskSequenceIds = @('MDT-AUTO-LUKA', 'MDT-AUTO-REMI'),
  [string]$RunnerPackageRelativePath = 'Scripts\marl\packages\MmaMdtRunner-1.0.0.msi'
)

$ErrorActionPreference = 'Stop'

function Convert-ToPowerShellEncodedCommand {
  param([string]$Script)

  $bytes = [Text.Encoding]::Unicode.GetBytes($Script)
  return [Convert]::ToBase64String($bytes)
}

function New-RunCommandStepNode {
  param(
    [xml]$SequenceXml,
    [string]$StepName,
    [string]$Action,
    [bool]$ContinueOnError = $false
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

  foreach ($variable in @(
    @{ name = 'PackageID'; property = 'PackageID'; value = $null },
    @{ name = 'RunAsUser'; property = 'RunAsUser'; value = 'false' },
    @{ name = 'SMSTSRunCommandLineUserName'; property = 'SMSTSRunCommandLineUserName'; value = '' },
    @{ name = 'SMSTSRunCommandLineUserPassword'; property = 'SMSTSRunCommandLineUserPassword'; value = '' },
    @{ name = 'LoadProfile'; property = 'LoadProfile'; value = 'false' }
  )) {
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

function Set-StepAction {
  param(
    [xml]$SequenceXml,
    [System.Xml.XmlElement]$Step,
    [string]$StepName,
    [string]$Action
  )

  [void]$Step.SetAttribute('name', $StepName)
  [void]$Step.SetAttribute('continueOnError', 'true')
  [void]$Step.SetAttribute('disable', 'false')
  [void]$Step.SetAttribute('successCodeList', '0 3010')

  $actionNode = $Step.SelectSingleNode('action')
  if (-not $actionNode) {
    $actionNode = $SequenceXml.CreateElement('action')
    [void]$Step.AppendChild($actionNode)
  }
  $actionNode.InnerText = $Action
}

$runnerSharePath = "\\$ShareServerName\DeploymentShare$\" + $RunnerPackageRelativePath.TrimStart('\')
$localInstallerDir = 'C:\ProgramData\MMA\MdtRunner\Installer'
$localInstallerPath = "$localInstallerDir\MmaMdtRunner-1.0.0.msi"
$stageAction = 'cmd.exe /c if not exist "{0}" mkdir "{0}" & copy /Y "{1}" "{2}" >nul' -f $localInstallerDir, $runnerSharePath, $localInstallerPath
$installAction = '%WINDIR%\System32\msiexec.exe /i "{0}" /qn /norestart' -f $localInstallerPath

$launcherScript = @'
$targets = @(
  'C:\Users\Public\Desktop\Relancer diagnostic MDT.lnk',
  'C:\Users\Administrateur\Desktop\Relancer diagnostic MDT.lnk'
)
$shortcutTarget = 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
$shortcutArguments = '-NoProfile -ExecutionPolicy Bypass -File "C:\Program Files\MMA Automation\MdtRunner\MmaMdtRunner.ps1"'
$workingDirectory = 'C:\Program Files\MMA Automation\MdtRunner'
$shortcutDescription = 'Relance locale des checks atelier MDT'

if (Test-Path $workingDirectory) {
  $shell = New-Object -ComObject WScript.Shell
  foreach ($shortcutPath in $targets) {
    $directory = Split-Path -Path $shortcutPath -Parent
    if (-not (Test-Path $directory)) { continue }
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $shortcutTarget
    $shortcut.Arguments = $shortcutArguments
    $shortcut.WorkingDirectory = $workingDirectory
    $shortcut.Description = $shortcutDescription
    $shortcut.Save()
  }
}

Remove-Item 'C:\Relancer diagnostic MDT.lnk' -Force -ErrorAction SilentlyContinue
Remove-Item 'C:\Users\Public\Desktop\MmaMdtRunner-1.0.0.msi' -Force -ErrorAction SilentlyContinue
Remove-Item 'C:\Users\Administrateur\Desktop\MmaMdtRunner-1.0.0.msi' -Force -ErrorAction SilentlyContinue
'@
$encodedLauncherScript = Convert-ToPowerShellEncodedCommand -Script $launcherScript
$launcherAction = '%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe -NoProfile -ExecutionPolicy Bypass -EncodedCommand {0}' -f $encodedLauncherScript

foreach ($taskSequenceId in $TaskSequenceIds) {
  $tsPath = Join-Path $DeploymentShareRoot ("Control\{0}\TS.xml" -f $taskSequenceId)
  if (-not (Test-Path $tsPath)) {
    Write-Warning "Task sequence not found: $tsPath"
    continue
  }

  [xml]$sequenceXml = Get-Content $tsPath

  $targetGroup = $null
  $executeStep = $null
  foreach ($group in @($sequenceXml.SelectNodes('//group'))) {
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
    Write-Warning "Unable to locate execute report script in $taskSequenceId"
    continue
  }

  $stageStep = $null
  $installStep = $null
  $launcherStep = $null
  foreach ($step in @($targetGroup.step)) {
    if ($step -and @('stage MMA MDT Runner MSI locally', 'copy MMA MDT Runner MSI to desktop') -contains [string]$step.name) {
      $stageStep = $step
    }
    if ($step -and [string]$step.name -eq 'install MMA MDT Runner') {
      $installStep = $step
    }
    if ($step -and [string]$step.name -eq 'publish MMA MDT Runner launcher') {
      $launcherStep = $step
    }
  }

  if (-not $stageStep) {
    $stageStep = New-RunCommandStepNode `
      -SequenceXml $sequenceXml `
      -StepName 'stage MMA MDT Runner MSI locally' `
      -Action $stageAction `
      -ContinueOnError $true
    if ($installStep) {
      [void]$targetGroup.InsertBefore($stageStep, $installStep)
    } else {
      [void]$targetGroup.InsertBefore($stageStep, $executeStep)
    }
  } else {
    Set-StepAction -SequenceXml $sequenceXml -Step $stageStep -StepName 'stage MMA MDT Runner MSI locally' -Action $stageAction
  }

  if (-not $installStep) {
    $installStep = New-RunCommandStepNode `
      -SequenceXml $sequenceXml `
      -StepName 'install MMA MDT Runner' `
      -Action $installAction `
      -ContinueOnError $true
    [void]$targetGroup.InsertBefore($installStep, $executeStep)
  } else {
    Set-StepAction -SequenceXml $sequenceXml -Step $installStep -StepName 'install MMA MDT Runner' -Action $installAction
  }

  if (-not $launcherStep) {
    $launcherStep = New-RunCommandStepNode `
      -SequenceXml $sequenceXml `
      -StepName 'publish MMA MDT Runner launcher' `
      -Action $launcherAction `
      -ContinueOnError $true
  } else {
    Set-StepAction -SequenceXml $sequenceXml -Step $launcherStep -StepName 'publish MMA MDT Runner launcher' -Action $launcherAction
    [void]$targetGroup.RemoveChild($launcherStep)
  }

  if ($installStep.NextSibling) {
    [void]$targetGroup.InsertBefore($launcherStep, $installStep.NextSibling)
  } else {
    [void]$targetGroup.AppendChild($launcherStep)
  }

  $sequenceXml.Save($tsPath)

  Write-Host "Updated $taskSequenceId"
  foreach ($step in @($targetGroup.step)) {
    if ($step -and @('stage MMA MDT Runner MSI locally', 'install MMA MDT Runner', 'publish MMA MDT Runner launcher', 'execute report script') -contains [string]$step.name) {
      $actionNode = $step.SelectSingleNode('action')
      $actionText = if ($actionNode) { $actionNode.InnerText } else { '' }
      Write-Host ("  {0}: {1}" -f $step.name, $actionText)
    }
  }
}
