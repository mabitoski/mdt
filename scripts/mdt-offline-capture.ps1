[CmdletBinding()]
param(
  [string]$OutputDir,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [string]$Technician,
  [switch]$SkipWinSat,
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

function Ensure-Directory {
  param([string]$Path)
  if (-not $Path) { return }
  if (-not (Test-Path -Path $Path)) {
    New-Item -Path $Path -ItemType Directory -Force | Out-Null
  }
}

function Write-JsonFile {
  param(
    [string]$Path,
    [object]$Object
  )
  $json = $Object | ConvertTo-Json -Depth 10
  Set-Content -Path $Path -Value $json -Encoding UTF8
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

$runId = [guid]::NewGuid().ToString()
$hostname = $env:COMPUTERNAME

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

$serialNumber = $bios.SerialNumber
if (-not $serialNumber) { $serialNumber = $enclosure.SerialNumber }

$macAddresses = @($netAdapters | ForEach-Object { $_.MACAddress } | Where-Object { $_ } | Sort-Object -Unique)
$primaryMac = Get-PrimaryMac -Adapters $netAdapters
$ramMb = if ($cs.TotalPhysicalMemory) { [math]::Round($cs.TotalPhysicalMemory / 1MB) } else { $null }
$ramSlotsTotal = if ($memoryArrays) { ($memoryArrays | Select-Object -First 1).MemoryDevices } else { $null }
$ramSlotsFree = if ($ramSlotsTotal -ne $null) { $ramSlotsTotal - @($memoryModules).Count } else { $null }
$hasBattery = $battery -ne $null
$categoryValue = Get-CategoryFromChassis -ChassisTypes $enclosure.ChassisTypes -HasBattery:$hasBattery

$installDate = $null
if ($os.InstallDate) {
  try { $installDate = ([Management.ManagementDateTimeConverter]::ToDateTime($os.InstallDate)).ToString('o') } catch { }
}

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
    releaseDate = if ($bios.ReleaseDate) { ([Management.ManagementDateTimeConverter]::ToDateTime($bios.ReleaseDate)).ToString('o') } else { $null }
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
}

if (-not $SkipMsinfo) {
  $msinfoDir = Join-Path $OutputDir 'msinfo'
  Ensure-Directory -Path $msinfoDir
  $msinfoPath = Join-Path $msinfoDir 'msinfo.txt'
  try {
    Start-Process -FilePath 'msinfo32.exe' -ArgumentList "/report `"$msinfoPath`"" -Wait -NoNewWindow | Out-Null
  } catch {
    Write-Warning "msinfo32 failed: $($_.Exception.Message)"
  }
}

if (-not $SkipDxDiag) {
  $dxDiagDir = Join-Path $OutputDir 'dxdiag'
  Ensure-Directory -Path $dxDiagDir
  $dxDiagPath = Join-Path $dxDiagDir 'dxdiag.txt'
  try {
    Start-Process -FilePath 'dxdiag.exe' -ArgumentList "/t `"$dxDiagPath`"" -Wait -NoNewWindow | Out-Null
  } catch {
    Write-Warning "dxdiag failed: $($_.Exception.Message)"
  }
}

if (-not $SkipBatteryReport) {
  $batteryDir = Join-Path $OutputDir 'battery'
  Ensure-Directory -Path $batteryDir
  $batteryPath = Join-Path $batteryDir 'battery-report.html'
  try {
    & powercfg /batteryreport /output $batteryPath | Out-Null
  } catch {
    Write-Warning "battery report failed: $($_.Exception.Message)"
  }
}

if (-not $SkipWinSat) {
  $winSatDir = Join-Path $OutputDir 'winsat'
  Ensure-Directory -Path $winSatDir
  $winSatXml = Join-Path $winSatDir 'winsat.xml'
  $winSatLog = Join-Path $winSatDir 'winsat.log'
  try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'winsat'
    $psi.Arguments = "formal -xml `"$winSatXml`""
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    if ($proc) {
      if (-not $proc.WaitForExit($WinSatTimeoutSec * 1000)) {
        $proc.Kill()
        Set-Content -Path $winSatLog -Value 'winsat timeout' -Encoding UTF8
      } else {
        $output = $proc.StandardOutput.ReadToEnd() + "`n" + $proc.StandardError.ReadToEnd()
        Set-Content -Path $winSatLog -Value $output -Encoding UTF8
      }
    }
  } catch {
    Write-Warning "winsat failed: $($_.Exception.Message)"
  }
  $winsatSummary = Parse-WinSatXml -Path $winSatXml
  if ($winsatSummary) {
    Write-JsonFile -Path (Join-Path $winSatDir 'winsat.json') -Object $winsatSummary
  }
}

if ($CameraTestPath -and (Test-Path -Path $CameraTestPath)) {
  $cameraDir = Join-Path $OutputDir 'camera'
  Ensure-Directory -Path $cameraDir
  $cameraLog = Join-Path $cameraDir 'camera.log'
  try {
    $proc = Start-Process -FilePath $CameraTestPath -ArgumentList $CameraTestArguments -PassThru
    if (-not $proc.WaitForExit($CameraTestTimeoutSec * 1000)) {
      $proc.Kill() | Out-Null
      Set-Content -Path $cameraLog -Value 'camera timeout'
    } else {
      Set-Content -Path $cameraLog -Value ("exit_code=" + $proc.ExitCode)
    }
  } catch {
    Write-Warning "camera test failed: $($_.Exception.Message)"
  }
}

Write-Output "Offline bundle created: $OutputDir"
