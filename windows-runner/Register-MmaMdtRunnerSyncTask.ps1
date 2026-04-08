[CmdletBinding()]
param(
  [string]$InstallRoot = $PSScriptRoot,
  [string]$TaskName = 'MMA MDT Runner Sync',
  [int]$IntervalMinutes = 15
)

$runnerScript = Join-Path $InstallRoot 'scripts\mdt-outbox-sync.ps1'
if (-not (Test-Path -Path $runnerScript)) {
  throw "Script introuvable: $runnerScript"
}

$action = New-ScheduledTaskAction `
  -Execute 'powershell.exe' `
  -Argument ('-NoProfile -ExecutionPolicy Bypass -File "{0}"' -f $runnerScript)

$triggerBoot = New-ScheduledTaskTrigger -AtLogOn
$triggerRepeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(1)
$triggerRepeat.Repetition = New-ScheduledTaskRepetitionSettingsSet -Interval (New-TimeSpan -Minutes $IntervalMinutes) -Duration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -MultipleInstances IgnoreNew `
  -StartWhenAvailable

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger @($triggerBoot, $triggerRepeat) `
  -Settings $settings `
  -Description 'Synchronise la file locale MMA MDT Runner vers l API MMA.' `
  -Force | Out-Null

Write-Output "Tache planifiee creee: $TaskName"
