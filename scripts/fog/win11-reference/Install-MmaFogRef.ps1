[CmdletBinding()]
param(
  [string]$ApiUrl = 'http://10.1.10.27:3000/api/ingest',
  [string]$Technician = '',
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode = 'quick',
  [switch]$SkipSysprep
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-RefLog {
  param(
    [string]$Path,
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  Add-Content -Path $Path -Value "[$timestamp][$Level] $Message"
}

function Find-SourceRoot {
  foreach ($letter in 'D'..'Z') {
    $candidate = '{0}:\' -f $letter
    if (Test-Path -Path (Join-Path $candidate 'MMA_FOG_REF.TAG')) {
      return $candidate
    }
  }
  throw 'Unable to locate MMA FOG reference media.'
}

function Ensure-Directory {
  param([string]$Path)
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

$logPath = 'C:\Windows\Temp\MMA-Fog-Win11-Reference.log'
$stagingRoot = 'C:\MMA_FOG_REF'
$payloadRoot = Join-Path $stagingRoot 'payload'
$stateRoot = 'C:\ProgramData\MMA\FogBootstrap\state'
$lockPath = Join-Path $stateRoot 'reference-prep.lock'
$donePath = Join-Path $stateRoot 'reference-prep.done'

Ensure-Directory -Path (Split-Path -Path $logPath -Parent)
Ensure-Directory -Path $stagingRoot
Ensure-Directory -Path $stateRoot

if (Test-Path -Path $donePath) {
  Write-RefLog -Path $logPath -Message "Reference preparation already completed, skipping."
  return
}

if (Test-Path -Path $lockPath) {
  Write-RefLog -Path $logPath -Message "Reference preparation already running, skipping duplicate invocation." -Level 'WARN'
  return
}

Set-Content -Path $lockPath -Value ((Get-Date).ToString('o'))

Write-RefLog -Path $logPath -Message 'Starting Windows 11 FOG reference preparation.'
try {
  if ((Test-Path -Path $payloadRoot) -and (Test-Path -Path (Join-Path $payloadRoot 'scripts\fog\install-fog-bootstrap.ps1'))) {
    Write-RefLog -Path $logPath -Message "Using pre-staged payload at $payloadRoot"
  } else {
    $sourceRoot = Find-SourceRoot
    Write-RefLog -Path $logPath -Message "Source media detected at $sourceRoot"
    if (Test-Path -Path $payloadRoot) {
      Remove-Item -Path $payloadRoot -Recurse -Force
    }
    Copy-Item -Path (Join-Path $sourceRoot 'payload') -Destination $payloadRoot -Recurse -Force
    Write-RefLog -Path $logPath -Message "Payload copied to $payloadRoot"
  }

  $bootstrapScriptPath = Join-Path $payloadRoot 'scripts\fog\install-fog-bootstrap.ps1'
  if (-not (Test-Path -Path $bootstrapScriptPath)) {
    throw "Bootstrap installer not found: $bootstrapScriptPath"
  }

  & $bootstrapScriptPath `
    -DestinationRoot 'C:\ProgramData\MMA\FogBootstrap' `
    -ApiUrl $ApiUrl `
    -Technician $Technician `
    -Category $Category `
    -TestMode $TestMode `
    -EnableSetupComplete

  Write-RefLog -Path $logPath -Message 'FOG bootstrap installed successfully.'

  $marker = 'C:\ProgramData\MMA\FogBootstrap\state\firstboot.done'
  if (Test-Path -Path $marker) {
    Remove-Item -Path $marker -Force
    Write-RefLog -Path $logPath -Message "Removed stale firstboot marker: $marker"
  }

  try {
    $bitLockerState = Get-BitLockerVolume -MountPoint 'C:' -ErrorAction Stop
    Write-RefLog -Path $logPath -Message ("BitLocker state before sysprep: ProtectionStatus={0}; VolumeStatus={1}; EncryptionPercentage={2}" -f $bitLockerState.ProtectionStatus, $bitLockerState.VolumeStatus, $bitLockerState.EncryptionPercentage)
    if ($bitLockerState.ProtectionStatus -ne 'Off' -or $bitLockerState.VolumeStatus -ne 'FullyDecrypted') {
      Disable-BitLocker -MountPoint 'C:' -ErrorAction Stop
      Write-RefLog -Path $logPath -Message 'Disable-BitLocker triggered on C:.'
    }
  } catch {
    Write-RefLog -Path $logPath -Message ("Unable to inspect or disable BitLocker before sysprep: {0}" -f $_.Exception.Message) -Level 'WARN'
    $manageBde = Join-Path $env:SystemRoot 'System32\manage-bde.exe'
    if (Test-Path -Path $manageBde) {
      try {
        $result = & $manageBde -off C: 2>&1 | Out-String
        Write-RefLog -Path $logPath -Message ("manage-bde -off C: result: {0}" -f ($result.Trim()))
      } catch {
        Write-RefLog -Path $logPath -Message ("manage-bde fallback failed: {0}" -f $_.Exception.Message) -Level 'WARN'
      }
    }
  }

  if ($SkipSysprep) {
    Write-RefLog -Path $logPath -Message 'SkipSysprep requested, leaving machine running.'
    Set-Content -Path $donePath -Value ((Get-Date).ToString('o'))
    return
  }

  $sysprepPath = Join-Path $env:WINDIR 'System32\Sysprep\Sysprep.exe'
  if (-not (Test-Path -Path $sysprepPath)) {
    throw "Sysprep not found: $sysprepPath"
  }

  Set-Content -Path $donePath -Value ((Get-Date).ToString('o'))
  Write-RefLog -Path $logPath -Message 'Launching sysprep /generalize /oobe /shutdown /mode:vm'
  Start-Process -FilePath $sysprepPath -ArgumentList '/generalize', '/oobe', '/shutdown', '/mode:vm' -Wait
} finally {
  if (Test-Path -Path $lockPath) {
    Remove-Item -Path $lockPath -Force -ErrorAction SilentlyContinue
  }
}
