[CmdletBinding()]
param(
  [string]$DeploymentShareRoot = 'W:\DeploymentShare',
  [string]$SourceTaskSequenceId = 'MDT-MELISSE',
  [string]$DestinationTaskSequenceId = 'MDT-AUTO',
  [string]$DestinationTaskSequenceName = 'MDT-AUTO',
  [string]$DestinationComments = 'Template cache pour automatisation MMA',
  [string]$HiddenGroupName = 'hidden'
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

  foreach ($pair in @(
      @{ Name = 'Name'; Value = $GroupName },
      @{ Name = 'Comments'; Value = 'Folder created by MMA automation' },
      @{ Name = 'CreatedTime'; Value = (Get-Date).ToString('G') },
      @{ Name = 'CreatedBy'; Value = "$env:COMPUTERNAME\$env:USERNAME" },
      @{ Name = 'LastModifiedTime'; Value = (Get-Date).ToString('G') },
      @{ Name = 'LastModifiedBy'; Value = "$env:COMPUTERNAME\$env:USERNAME" }
    )) {
    $node = $GroupsXml.CreateElement($pair.Name)
    $node.InnerText = $pair.Value
    [void]$group.AppendChild($node)
  }

  [void]$GroupsXml.groups.AppendChild($group)
  return $group
}

$controlRoot = Join-Path $DeploymentShareRoot 'Control'
$sourceControlPath = Join-Path $controlRoot $SourceTaskSequenceId
$destinationControlPath = Join-Path $controlRoot $DestinationTaskSequenceId
$taskSequencesPath = Join-Path $controlRoot 'TaskSequences.xml'
$taskSequenceGroupsPath = Join-Path $controlRoot 'TaskSequenceGroups.xml'
$backupRoot = Join-Path $controlRoot ('_beta_backups\mdt_auto_' + (Get-TimeStamp))

if (-not (Test-Path $sourceControlPath)) {
  throw "Source task sequence folder not found: $sourceControlPath"
}

New-Item -Path $backupRoot -ItemType Directory -Force | Out-Null
Backup-File -Path $taskSequencesPath -BackupRoot $backupRoot
Backup-File -Path $taskSequenceGroupsPath -BackupRoot $backupRoot
if (Test-Path $destinationControlPath) {
  Copy-Item -Path $destinationControlPath -Destination (Join-Path $backupRoot $DestinationTaskSequenceId) -Recurse -Force
  Remove-Item -Path $destinationControlPath -Recurse -Force
}
Copy-Item -Path $sourceControlPath -Destination $destinationControlPath -Recurse -Force

[xml]$taskSequencesXml = Get-Content $taskSequencesPath
[xml]$taskSequenceGroupsXml = Get-Content $taskSequenceGroupsPath

$sourceTaskSequenceNode = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $SourceTaskSequenceId } | Select-Object -First 1
if (-not $sourceTaskSequenceNode) {
  throw "Source task sequence metadata not found for ID $SourceTaskSequenceId"
}

$destinationTaskSequenceNode = $taskSequencesXml.tss.ts | Where-Object { $_.ID -eq $DestinationTaskSequenceId } | Select-Object -First 1
$destinationGuid = $null
if ($destinationTaskSequenceNode -and $destinationTaskSequenceNode.guid) {
  $destinationGuid = [string]$destinationTaskSequenceNode.guid
} else {
  $destinationGuid = '{' + [guid]::NewGuid().ToString() + '}'
}

if (-not $destinationTaskSequenceNode) {
  $destinationTaskSequenceNode = $taskSequencesXml.CreateElement('ts')
  [void]$taskSequencesXml.tss.AppendChild($destinationTaskSequenceNode)
}

[void]$destinationTaskSequenceNode.SetAttribute('guid', $destinationGuid)
[void]$destinationTaskSequenceNode.SetAttribute('enable', 'True')
[void]$destinationTaskSequenceNode.SetAttribute('hide', 'True')

$childValues = @{
  Name = $DestinationTaskSequenceName
  Comments = $DestinationComments
  CreatedTime = (Get-Date).ToString('G')
  CreatedBy = "$env:COMPUTERNAME\$env:USERNAME"
  LastModifiedTime = (Get-Date).ToString('G')
  LastModifiedBy = "$env:COMPUTERNAME\$env:USERNAME"
  ID = $DestinationTaskSequenceId
  Version = [string]$sourceTaskSequenceNode.Version
  TaskSequenceTemplate = [string]$sourceTaskSequenceNode.TaskSequenceTemplate
}

foreach ($key in $childValues.Keys) {
  $existing = $destinationTaskSequenceNode.SelectSingleNode($key)
  if (-not $existing) {
    $existing = $taskSequencesXml.CreateElement($key)
    [void]$destinationTaskSequenceNode.AppendChild($existing)
  }
  $existing.InnerText = $childValues[$key]
}

foreach ($group in @($taskSequenceGroupsXml.groups.group)) {
  @($group.Member) | Where-Object { $_.'#text' -eq $destinationGuid } | ForEach-Object {
    [void]$group.RemoveChild($_)
  }
}

$hiddenGroup = Get-OrCreateGroupNode -GroupsXml $taskSequenceGroupsXml -GroupName $HiddenGroupName
$memberNode = $taskSequenceGroupsXml.CreateElement('Member')
$memberNode.InnerText = $destinationGuid
[void]$hiddenGroup.AppendChild($memberNode)

$taskSequencesXml.Save($taskSequencesPath)
$taskSequenceGroupsXml.Save($taskSequenceGroupsPath)

[pscustomobject]@{
  SourceTaskSequenceId = $SourceTaskSequenceId
  DestinationTaskSequenceId = $DestinationTaskSequenceId
  DestinationTaskSequenceName = $DestinationTaskSequenceName
  HiddenGroupName = $HiddenGroupName
  DeploymentShareRoot = $DeploymentShareRoot
  ControlPath = $destinationControlPath
  BackupPath = $backupRoot
}
