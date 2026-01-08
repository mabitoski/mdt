[CmdletBinding()]
param(
  [string]$OutputDir,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [string]$Technician,
  [switch]$SkipWinSat,
  [int]$WinSatIdleTimeoutSec = 180,
  [switch]$SkipDxDiag,
  [switch]$SkipMsinfo,
  [switch]$SkipBatteryReport,
  [int]$WinSatTimeoutSec = 1800,
  [int]$DxDiagTimeoutSec = 300,
  [int]$MsinfoTimeoutSec = 300,
  [string]$CameraTestPath,
  [string[]]$CameraTestArguments,
  [int]$CameraTestTimeoutSec = 30
)

$scriptVersion = '1.0.0'
$startTime = Get-Date
$logPath = $null

function Ensure-Directory {
  param([string]$Path)
  if (-not $Path) { return }
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$timestamp][$Level] $Message"
  Write-Host $line
  if ($logPath) {
    try {
      Add-Content -Path $logPath -Value $line
    } catch { }
  }
}

function Write-JsonFile {
  param(
    [string]$Path,
    [object]$Object,
    [int]$Depth = 10
  )
  $json = $Object | ConvertTo-Json -Depth $Depth
  Set-Content -Path $Path -Value $json -Encoding UTF8
}

function Convert-DmtfDateSafe {
  param([string]$Value)
  if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
  try {
    return ([Management.ManagementDateTimeConverter]::ToDateTime($Value)).ToString('o')
  } catch {
    return $null
  }
}

function Get-PrimaryMac {
  param([object[]]$Adapters)
  if (-not $Adapters) { return $null }
  $preferred = $Adapters | Where-Object { $_.IPEnabled -and $_.MACAddress } | Select-Object -First 1
  if ($preferred) { return $preferred.MACAddress }
  $fallback = $Adapters | Where-Object { $_.MACAddress } | Select-Object -First 1
  return $fallback.MACAddress
}

function Get-CategoryFromChassis {
  param(
    [int[]]$ChassisTypes,
    [bool]$HasBattery
  )
  if ($Category -ne 'auto') { return $Category }
  $portable = @(8, 9, 10, 11, 12, 14, 30, 31, 32)
  $desktop = @(3, 4, 5, 6, 7, 15, 16)
  if ($HasBattery) { return 'laptop' }
  foreach ($item in ($ChassisTypes | Where-Object { $_ -ne $null })) {
    if ($portable -contains $item) { return 'laptop' }
    if ($desktop -contains $item) { return 'desktop' }
  }
  return 'unknown'
}

function Get-WinSatNote {
  param([double]$Score)
  if ($Score -lt 3.0) { return 'Horrible' }
  if ($Score -lt 4.5) { return 'Mauvais' }
  if ($Score -lt 6.0) { return 'Moyen' }
  if ($Score -lt 7.5) { return 'Bon' }
  return 'Excellent'
}

function Parse-WinSatXml {
  param([string]$Path)
  if (-not (Test-Path -Path $Path)) { return $null }
  try {
    $xml = [xml](Get-Content -Path $Path -Raw)
  } catch {
    return $null
  }
  $winSpr = $xml.WinSAT.WinSPR
  if (-not $winSpr) { return $null }
  $result = [ordered]@{
    source = (Split-Path -Leaf $Path)
    winSPR = [ordered]@{
      CpuScore = [double]$winSpr.CPUScore
      MemoryScore = [double]$winSpr.MemoryScore
      GraphicsScore = [double]$winSpr.GraphicsScore
      GamingScore = [double]$winSpr.GamingScore
      DiskScore = [double]$winSpr.DiskScore
    }
  }
  $result.cpuNote = if ($result.winSPR.CpuScore) { Get-WinSatNote -Score $result.winSPR.CpuScore } else { $null }
  $result.ramNote = if ($result.winSPR.MemoryScore) { Get-WinSatNote -Score $result.winSPR.MemoryScore } else { $null }
  $gpuScore = if ($result.winSPR.GamingScore) { $result.winSPR.GamingScore } else { $result.winSPR.GraphicsScore }
  $result.gpuNote = if ($gpuScore) { Get-WinSatNote -Score $gpuScore } else { $null }
  return $result
}

$timestamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
if (-not $OutputDir) {
  $root = if ($env:USERPROFILE) { Join-Path $env:USERPROFILE 'Documents' } else { $env:TEMP }
  $OutputDir = Join-Path $root "mdt-offline-$timestamp"
}
Ensure-Directory -Path $OutputDir
$logPath = Join-Path $OutputDir 'offline.log'
Write-Log "Offline capture started (version=$scriptVersion)."
Write-Log "OutputDir=$OutputDir"

$runId = [guid]::NewGuid().ToString()
$hostname = $env:COMPUTERNAME
Write-Log "RunId=$runId Host=$hostname"

Write-Log 'Collecting hardware inventory...'
$os = Get-CimInstance -ClassName Win32_OperatingSystem
$cs = Get-CimInstance -ClassName Win32_ComputerSystem
$bios = Get-CimInstance -ClassName Win32_BIOS
$baseboard = Get-CimInstance -ClassName Win32_BaseBoard
$enclosure = Get-CimInstance -ClassName Win32_SystemEnclosure
$processors = Get-CimInstance -ClassName Win32_Processor
$memoryModules = Get-CimInstance -ClassName Win32_PhysicalMemory
$memoryArrays = Get-CimInstance -ClassName Win32_PhysicalMemoryArray
$disks = Get-CimInstance -ClassName Win32_DiskDrive
$volumes = Get-CimInstance -ClassName Win32_LogicalDisk -Filter "DriveType=3"
$netAdapters = Get-CimInstance -ClassName Win32_NetworkAdapterConfiguration -Filter "MACAddress IS NOT NULL"
$gpus = Get-CimInstance -ClassName Win32_VideoController
$battery = Get-CimInstance -ClassName Win32_Battery -ErrorAction SilentlyContinue
Write-Log 'Inventory OK.'

$serialNumber = $bios.SerialNumber
if (-not $serialNumber) { $serialNumber = $enclosure.SerialNumber }

$macAddresses = @($netAdapters | ForEach-Object { $_.MACAddress } | Where-Object { $_ } | Sort-Object -Unique)
$primaryMac = Get-PrimaryMac -Adapters $netAdapters
$ramMb = if ($cs.TotalPhysicalMemory) { [math]::Round($cs.TotalPhysicalMemory / 1MB) } else { $null }
$ramSlotsTotal = if ($memoryArrays) { ($memoryArrays | Select-Object -First 1).MemoryDevices } else { $null }
$ramSlotsFree = if ($ramSlotsTotal -ne $null) { $ramSlotsTotal - @($memoryModules).Count } else { $null }
$hasBattery = $battery -ne $null
$categoryValue = Get-CategoryFromChassis -ChassisTypes $enclosure.ChassisTypes -HasBattery:$hasBattery

$installDate = Convert-DmtfDateSafe -Value $os.InstallDate

$summary = [ordered]@{
  reportId = $runId
  hostname = $hostname
  serialNumber = $serialNumber
  macAddress = $primaryMac
  macAddresses = $macAddresses
  category = $categoryValue
  technician = $Technician
  vendor = $cs.Manufacturer
  model = $cs.Model
  osVersion = $os.Version
  ramMb = $ramMb
  ramSlotsTotal = $ramSlotsTotal
  ramSlotsFree = $ramSlotsFree
  cpu = @{
    name = ($processors | Select-Object -First 1).Name
    cores = ($processors | Select-Object -First 1).NumberOfCores
    threads = ($processors | Select-Object -First 1).NumberOfLogicalProcessors
    maxClockMHz = ($processors | Select-Object -First 1).MaxClockSpeed
  }
  gpu = @{
    name = ($gpus | Select-Object -First 1).Name
    driverVersion = ($gpus | Select-Object -First 1).DriverVersion
    memoryMb = if (($gpus | Select-Object -First 1).AdapterRAM) { [math]::Round(($gpus | Select-Object -First 1).AdapterRAM / 1MB) } else { $null }
  }
  disks = @($disks | ForEach-Object {
    @{
      model = $_.Model
      sizeGb = if ($_.Size) { [math]::Round($_.Size / 1GB, 1) } else { $null }
      serial = $_.SerialNumber
      mediaType = $_.MediaType
    }
  })
  volumes = @($volumes | ForEach-Object {
    @{
      device = $_.DeviceID
      label = $_.VolumeName
      fileSystem = $_.FileSystem
      sizeGb = if ($_.Size) { [math]::Round($_.Size / 1GB, 1) } else { $null }
      freeGb = if ($_.FreeSpace) { [math]::Round($_.FreeSpace / 1GB, 1) } else { $null }
    }
  })
  bios = @{
    vendor = $bios.Manufacturer
    version = $bios.SMBIOSBIOSVersion
    releaseDate = Convert-DmtfDateSafe -Value $bios.ReleaseDate
  }
  baseboard = @{
    product = $baseboard.Product
  }
  windows = @{
    edition = $os.Caption
    version = $os.Version
    build = $os.BuildNumber
    installedOn = $installDate
  }
  battery = if ($battery) {
    @{
      estimatedChargeRemaining = $battery.EstimatedChargeRemaining
      status = $battery.BatteryStatus
    }
  } else {
    $null
  }
}

$manifest = [ordered]@{
  reportId = $runId
  generatedAt = (Get-Date).ToUniversalTime().ToString('o')
  scriptVersion = $scriptVersion
  hostname = $hostname
}

Write-JsonFile -Path (Join-Path $OutputDir 'manifest.json') -Object $manifest
Write-JsonFile -Path (Join-Path $OutputDir 'summary.json') -Object $summary

$rawDir = Join-Path $OutputDir 'raw'
Ensure-Directory -Path $rawDir
Write-Log 'Writing raw inventory...'
Write-JsonFile -Path (Join-Path $rawDir 'inventory.json') -Object @{
  os = $os
  computerSystem = $cs
  bios = $bios
  baseboard = $baseboard
  enclosure = $enclosure
  cpu = $processors
  memoryModules = $memoryModules
  memoryArrays = $memoryArrays
  disks = $disks
  volumes = $volumes
  network = $netAdapters
  gpus = $gpus
  battery = $battery
} -Depth 20
Write-Log 'Raw inventory saved.'

if (-not $SkipMsinfo) {
  $msinfoDir = Join-Path $OutputDir 'msinfo'
  Ensure-Directory -Path $msinfoDir
  $msinfoPath = Join-Path $msinfoDir 'msinfo.txt'
  Write-Log 'Running msinfo32...'
  try {
    $proc = Start-Process -FilePath 'msinfo32.exe' -ArgumentList "/report `"$msinfoPath`"" -PassThru
    if ($MsinfoTimeoutSec -gt 0) {
      if (-not $proc.WaitForExit($MsinfoTimeoutSec * 1000)) {
        $proc.Kill() | Out-Null
        Write-Log "msinfo32 timeout after ${MsinfoTimeoutSec}s" 'WARN'
      }
    } else {
      $proc.WaitForExit() | Out-Null
    }
    Write-Log 'msinfo32 done.'
  } catch {
    Write-Warning "msinfo32 failed: $($_.Exception.Message)"
    Write-Log "msinfo32 failed: $($_.Exception.Message)" 'WARN'
  }
}

if (-not $SkipDxDiag) {
  $dxDiagDir = Join-Path $OutputDir 'dxdiag'
  Ensure-Directory -Path $dxDiagDir
  $dxDiagPath = Join-Path $dxDiagDir 'dxdiag.txt'
  Write-Log 'Running dxdiag...'
  try {
    $proc = Start-Process -FilePath 'dxdiag.exe' -ArgumentList "/t `"$dxDiagPath`"" -PassThru
    if ($DxDiagTimeoutSec -gt 0) {
      if (-not $proc.WaitForExit($DxDiagTimeoutSec * 1000)) {
        $proc.Kill() | Out-Null
        Write-Log "dxdiag timeout after ${DxDiagTimeoutSec}s" 'WARN'
      }
    } else {
      $proc.WaitForExit() | Out-Null
    }
    Write-Log 'dxdiag done.'
  } catch {
    Write-Warning "dxdiag failed: $($_.Exception.Message)"
    Write-Log "dxdiag failed: $($_.Exception.Message)" 'WARN'
  }
}

if (-not $SkipBatteryReport) {
  $batteryDir = Join-Path $OutputDir 'battery'
  Ensure-Directory -Path $batteryDir
  $batteryPath = Join-Path $batteryDir 'battery-report.html'
  Write-Log 'Running battery report...'
  try {
    & powercfg /batteryreport /output $batteryPath | Out-Null
    Write-Log 'Battery report done.'
  } catch {
    Write-Warning "battery report failed: $($_.Exception.Message)"
    Write-Log "battery report failed: $($_.Exception.Message)" 'WARN'
  }
}

if (-not $SkipWinSat) {
  $winSatDir = Join-Path $OutputDir 'winsat'
  Ensure-Directory -Path $winSatDir
  $winSatXml = Join-Path $winSatDir 'winsat.xml'
  $winSatLog = Join-Path $winSatDir 'winsat.log'
  $winSatSystemLog = $null
  if ($env:SystemRoot) {
    $candidate = Join-Path $env:SystemRoot 'Performance\WinSAT\winsat.log'
    if (Test-Path -Path $candidate) {
      $winSatSystemLog = $candidate
    } else {
      $candidate = Join-Path $env:SystemRoot 'Performance\WinSAT\WinSAT.log'
      if (Test-Path -Path $candidate) { $winSatSystemLog = $candidate }
    }
  }
  Write-Log 'Running winsat formal...'
  try {
    $winSatExe = if ($env:SystemRoot) { Join-Path $env:SystemRoot 'System32\winsat.exe' } else { $null }
    if ($winSatExe -and -not (Test-Path -Path $winSatExe)) {
      $winSatExe = $null
    }
    $exeToRun = if ($winSatExe) { $winSatExe } else { 'winsat' }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $exeToRun
    $psi.Arguments = "formal -xml `"$winSatXml`""
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    $proc.EnableRaisingEvents = $true

    $script:lastWinSatOutput = Get-Date
    $proc.add_OutputDataReceived({
      param($sender, $e)
      if ($e.Data) {
        $script:lastWinSatOutput = Get-Date
        try { Add-Content -Path $winSatLog -Value $e.Data } catch { }
        Write-Log ("winsat> {0}" -f $e.Data)
      }
    })
    $proc.add_ErrorDataReceived({
      param($sender, $e)
      if ($e.Data) {
        $script:lastWinSatOutput = Get-Date
        try { Add-Content -Path $winSatLog -Value $e.Data } catch { }
        Write-Log ("winsat! {0}" -f $e.Data) 'WARN'
      }
    })

    if ($proc.Start()) {
      Set-Content -Path $winSatLog -Value "winsat started (pid=$($proc.Id))" -Encoding UTF8
      Write-Log "winsat pid=$($proc.Id)"
      $proc.BeginOutputReadLine()
      $proc.BeginErrorReadLine()
      $startAt = Get-Date
      $nextUpdate = $startAt.AddSeconds(30)
      $lastCpu = $proc.TotalProcessorTime.TotalSeconds
      $lastLogWrite = (Get-Item $winSatLog).LastWriteTimeUtc
      $lastActivity = $startAt
      $timeoutSec = if ($WinSatTimeoutSec -gt 0) { $WinSatTimeoutSec } else { 0 }
      $idleTimeoutSec = if ($WinSatIdleTimeoutSec -gt 0) { $WinSatIdleTimeoutSec } else { 0 }

      while (-not $proc.HasExited) {
        Start-Sleep -Seconds 2
        $elapsed = [int]((Get-Date) - $startAt).TotalSeconds
        if ($timeoutSec -gt 0 -and $elapsed -ge $timeoutSec) {
          $proc.Kill()
          Add-Content -Path $winSatLog -Value 'winsat timeout'
          Write-Log "winsat timeout after ${WinSatTimeoutSec}s" 'WARN'
          break
        }
        if ((Get-Date) -ge $nextUpdate) {
          $xmlPresent = Test-Path -Path $winSatXml
          $proc.Refresh()
          $cpuNow = $proc.TotalProcessorTime.TotalSeconds
          $cpuDelta = [math]::Round(($cpuNow - $lastCpu), 2)
          $lastCpu = $cpuNow
          $wsMb = [math]::Round(($proc.WorkingSet64 / 1MB), 1)
          $logTouched = $false
          $currentLogWrite = (Get-Item $winSatLog).LastWriteTimeUtc
          if ($currentLogWrite -gt $lastLogWrite) { $logTouched = $true }
          $lastLogWrite = $currentLogWrite
          if ($cpuDelta -gt 0 -or $logTouched -or $script:lastWinSatOutput -gt $lastActivity) {
            $lastActivity = Get-Date
          }
          $idleSec = [int]((Get-Date) - $lastActivity).TotalSeconds
          Write-Log ("winsat still running ({0}s, xml_present={1}, cpu_delta={2}s, ws={3}MB, log_touched={4}, idle={5}s)" -f $elapsed, $xmlPresent, $cpuDelta, $wsMb, $logTouched, $idleSec)
          if ($idleTimeoutSec -gt 0 -and $idleSec -ge $idleTimeoutSec) {
            $proc.Kill()
            Add-Content -Path $winSatLog -Value "winsat idle timeout (${idleSec}s)"
            Write-Log "winsat idle timeout after ${idleSec}s" 'WARN'
            break
          }
          $nextUpdate = (Get-Date).AddSeconds(30)
        }
      }
      $proc.WaitForExit()
      try { $proc.CancelOutputReadLine() } catch { }
      try { $proc.CancelErrorReadLine() } catch { }
      if ($proc.HasExited) {
        Write-Log ("winsat done (exit_code={0})." -f $proc.ExitCode)
      }
    }
  } catch {
    Write-Warning "winsat failed: $($_.Exception.Message)"
    Write-Log "winsat failed: $($_.Exception.Message)" 'WARN'
  }
  $winsatSummary = Parse-WinSatXml -Path $winSatXml
  if ($winsatSummary) {
    Write-JsonFile -Path (Join-Path $winSatDir 'winsat.json') -Object $winsatSummary
    Write-Log 'winsat summary saved.'
  }
}

if ($CameraTestPath -and (Test-Path -Path $CameraTestPath)) {
  $cameraDir = Join-Path $OutputDir 'camera'
  Ensure-Directory -Path $cameraDir
  $cameraLog = Join-Path $cameraDir 'camera.log'
  Write-Log 'Running camera test...'
  try {
    $proc = Start-Process -FilePath $CameraTestPath -ArgumentList $CameraTestArguments -PassThru
    if (-not $proc.WaitForExit($CameraTestTimeoutSec * 1000)) {
      $proc.Kill() | Out-Null
      Set-Content -Path $cameraLog -Value 'camera timeout'
      Write-Log "camera timeout after ${CameraTestTimeoutSec}s" 'WARN'
    } else {
      Set-Content -Path $cameraLog -Value ("exit_code=" + $proc.ExitCode)
      Write-Log "camera done (exit_code=$($proc.ExitCode))."
    }
  } catch {
    Write-Warning "camera test failed: $($_.Exception.Message)"
    Write-Log "camera test failed: $($_.Exception.Message)" 'WARN'
  }
}

$duration = [int]((Get-Date) - $startTime).TotalSeconds
Write-Log "Offline capture completed in ${duration}s."
Write-Output "Offline bundle created: $OutputDir"
