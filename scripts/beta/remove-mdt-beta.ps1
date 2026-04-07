[CmdletBinding()]
param(
  [string]$DeploymentShareRoot = 'W:\DeploymentShare',
  [string]$DestinationTaskSequenceId = 'MDT-AUTO-MELISSE',
  [string]$DestinationTaskSequenceName = 'MDT-AUTO-Melisse',
  [string]$TaskSequenceGroupName = 'MMA Beta',
  [string]$TechnicianDisplayName = 'Beta'
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

  if (-not (Test-Path $Path)) {
    return
  }
  $target = Join-Path $BackupRoot ([IO.Path]::GetFileName($Path))
  Copy-Item -Path $Path -Destination $target -Force
}

$controlRoot = Join-Path $DeploymentShareRoot 'Control'
$destinationControlPath = Join-Path $controlRoot $DestinationTaskSequenceId
$taskSequencesPath = Join-Path $controlRoot 'TaskSequences.xml'
$taskSequenceGroupsPath = Join-Path $controlRoot 'TaskSequenceGroups.xml'
$backupRoot = Join-Path $controlRoot ('_beta_backups\' + (Get-TimeStamp))

New-Item -Path $backupRoot -ItemType Directory -Force | Out-Null
Backup-File -Path $taskSequencesPath -BackupRoot $backupRoot
Backup-File -Path $taskSequenceGroupsPath -BackupRoot $backupRoot

if (Test-Path $destinationControlPath) {
  Copy-Item -Path $destinationControlPath -Destination (Join-Path $backupRoot $DestinationTaskSequenceId) -Recurse -Force
}

[xml]$taskSequencesXml = Get-Content $taskSequencesPath
[xml]$taskSequenceGroupsXml = Get-Content $taskSequenceGroupsPath

$destinationTaskSequenceNode = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $DestinationTaskSequenceId } | Select-Object -First 1
$destinationGuid = $null
if ($destinationTaskSequenceNode) {
  $destinationGuid = [string]$destinationTaskSequenceNode.guid
  [void]$taskSequencesXml.tss.RemoveChild($destinationTaskSequenceNode)
}

foreach ($groupNode in @($taskSequenceGroupsXml.groups.group)) {
  foreach ($memberNode in @($groupNode.Member)) {
    if ($destinationGuid -and $memberNode.'#text' -eq $destinationGuid) {
      [void]$groupNode.RemoveChild($memberNode)
    }
  }
}

$taskSequencesXml.Save($taskSequencesPath)
$taskSequenceGroupsXml.Save($taskSequenceGroupsPath)

$removed = $false
if (Test-Path $destinationControlPath) {
  Remove-Item -Path $destinationControlPath -Recurse -Force
  $removed = $true
}

Write-Host "Removed beta task sequence:"
Write-Host "  Destination: $DestinationTaskSequenceId"
Write-Host "  Name:        $DestinationTaskSequenceName"
Write-Host "  Technician:  $TechnicianDisplayName"
Write-Host "  Group:       $TaskSequenceGroupName"
Write-Host "  Backup:      $backupRoot"

[pscustomobject]@{
  DestinationTaskSequenceId = $DestinationTaskSequenceId
  DestinationTaskSequenceName = $DestinationTaskSequenceName
  TechnicianDisplayName = $TechnicianDisplayName
  TaskSequenceGroupName = $TaskSequenceGroupName
  DeploymentShareRoot = $DeploymentShareRoot
  ControlPath = $destinationControlPath
  TaskSequencesPath = $taskSequencesPath
  TaskSequenceGroupsPath = $taskSequenceGroupsPath
  BackupPath = $backupRoot
  Removed = ($removed -or [bool]$destinationGuid)
}
