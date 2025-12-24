[CmdletBinding()]
param(
  [string]$ApiUrl = $env:MDT_API_URL,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode = 'quick',
  [int]$TimeoutSec = 15,
  [int]$DiskTestTimeoutSec = 180,
  [int]$MemTestTimeoutSec = 180,
  [int]$StressLoops = 2,
  [string]$CameraTestPath = $env:MDT_CAMERA_TEST_PATH,
  [int]$CameraTestTimeoutSec = 20,
  [int]$MsinfoTimeoutSec = 0,
  [ValidateSet('auto', 'ethernet', 'wifi', 'any')][string]$MacPreference = 'auto',
  [string]$LogPath,
  [switch]$SkipTlsValidation
)

$scriptVersion = '1.2.9'
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

if (-not $ApiUrl) {
  $ApiUrl = 'http://10.1.142.28:3000/api/ingest'
}

if (-not $LogPath) {
  $LogPath = Join-Path $PSScriptRoot 'mdt-report.log'
}

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$timestamp][$Level] $Message"
  try {
    Add-Content -Path $LogPath -Value $line
  } catch {
    Write-Output $line
  }
}

function Test-IsAdmin {
  try {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]$current
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  } catch {
    return $false
  }
}

Write-Log "Start script version $scriptVersion"
Write-Log "ApiUrl=$ApiUrl Category=$Category TestMode=$TestMode"

$script:IsAdmin = Test-IsAdmin
Write-Log "IsAdmin=$script:IsAdmin"

if ($SkipTlsValidation) {
  [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
  Write-Log 'TLS validation disabled' 'WARN'
}

$laptopChassis = @(8, 9, 10, 11, 12, 14, 18, 21, 31)
$desktopChassis = @(3, 4, 5, 6, 7, 15, 16)

function Get-CimInstanceSafe {
  param(
    [string]$ClassName,
    [string]$Filter,
    [string]$Namespace
  )

  try {
    $params = @{ ClassName = $ClassName; ErrorAction = 'Stop' }
    if ($Filter) { $params.Filter = $Filter }
    if ($Namespace) { $params.Namespace = $Namespace }
    return Get-CimInstance @params
  } catch {
    try {
      $params = @{ Class = $ClassName; ErrorAction = 'Stop' }
      if ($Filter) { $params.Filter = $Filter }
      if ($Namespace) { $params.Namespace = $Namespace }
      return Get-WmiObject @params
    } catch {
      Write-Log "WMI query failed: $ClassName" 'WARN'
      return @()
    }
  }
}

function Get-RegistryValue {
  param(
    [string]$Path,
    [string]$Name
  )

  try {
    return (Get-ItemProperty -Path $Path -Name $Name -ErrorAction Stop).$Name
  } catch {
    return $null
  }
}

function Decode-WmiString {
  param([array]$Chars)

  if (-not $Chars) { return $null }
  $clean = $Chars | Where-Object { $_ -ne 0 }
  if (-not $clean) { return $null }
  return -join ($clean | ForEach-Object { [char]$_ })
}

function Convert-WmiDate {
  param([string]$Value)

  if (-not $Value) { return $null }
  try {
    return [System.Management.ManagementDateTimeConverter]::ToDateTime($Value)
  } catch {
    return $null
  }
}

function Get-StatusFromDevices {
  param([array]$Devices)

  if (-not $Devices -or $Devices.Count -eq 0) { return 'absent' }
  foreach ($device in $Devices) {
    if ($device.ConfigManagerErrorCode -eq 0) { return 'ok' }
    if ($device.Status -eq 'OK') { return 'ok' }
  }
  return 'nok'
}

function Get-ChassisCategory {
  $enclosure = Get-CimInstanceSafe -ClassName 'Win32_SystemEnclosure'
  $codes = @()
  foreach ($item in $enclosure) {
    if ($item.ChassisTypes) { $codes += $item.ChassisTypes }
  }

  foreach ($code in $codes) {
    if ($laptopChassis -contains $code) { return 'laptop' }
    if ($desktopChassis -contains $code) { return 'desktop' }
  }

  return 'unknown'
}

function Get-MacFromMsinfo {
  param([int]$TimeoutSec = 30)

  if ($TimeoutSec -le 0) { return $null }

  $cmd = Get-Command msinfo32 -ErrorAction SilentlyContinue
  if (-not $cmd) { return $null }

  $reportPath = Join-Path ([System.IO.Path]::GetTempPath()) ("mdt-msinfo-{0}.txt" -f [guid]::NewGuid().ToString('N'))
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $cmd.Source
  $psi.Arguments = "/report `"$reportPath`""
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "msinfo32 start failed: $($_.Exception.Message)" 'WARN'
    return $null
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      Write-Log "msinfo32 timed out after ${TimeoutSec}s" 'WARN'
      return $null
    }
  } catch {
    Write-Log "msinfo32 wait failed: $($_.Exception.Message)" 'WARN'
    return $null
  }

  if (-not (Test-Path $reportPath)) {
    Write-Log 'msinfo32 report not found' 'WARN'
    return $null
  }

  $content = Get-Content -Path $reportPath -ErrorAction SilentlyContinue
  Remove-Item -Path $reportPath -ErrorAction SilentlyContinue
  if (-not $content) { return $null }

  $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
  $skipPattern = 'virtual|vpn|loopback|bluetooth|wi-fi direct|tap|hyper-v|pseudo|wan miniport|ras'

  foreach ($line in $content) {
    if ($line -match '(adresse physique|physical address|adresse mac|mac address)') {
      if ($line -match $macRegex) {
        if ($line -match $skipPattern) { continue }
        return $Matches[0]
      }
    }
  }

  $matches = [regex]::Matches(($content -join "`n"), $macRegex)
  $values = @()
  foreach ($match in $matches) { $values += $match.Value }
  $unique = $values | Sort-Object -Unique
  foreach ($value in $unique) {
    if ($value -notmatch '^00([-:]?00){5}$') { return $value }
  }

  return $null
}

function Get-MacFromGetmac {
  param([string]$SkipPattern)

  $cmd = Get-Command getmac -ErrorAction SilentlyContinue
  if (-not $cmd) { return $null }

  try {
    $output = & $cmd.Source /V /FO CSV /NH 2>$null
    if (-not $output) { return $null }
    $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
    foreach ($line in $output) {
      $row = $line | ConvertFrom-Csv -Header 'Connection','Adapter','Mac','Transport'
      if (-not $row) { continue }
      $mac = $row.Mac
      if (-not $mac -or $mac -notmatch $macRegex) { continue }
      if ($row.Adapter -and $row.Adapter -match $SkipPattern) { continue }
      if ($row.Connection -and $row.Connection -match $SkipPattern) { continue }
      return $mac
    }
  } catch {
    Write-Log "getmac failed: $($_.Exception.Message)" 'WARN'
    return $null
  }

  return $null
}

function Get-MacFromIpconfig {
  param([string]$SkipPattern)

  $cmd = Get-Command ipconfig -ErrorAction SilentlyContinue
  if (-not $cmd) { return $null }

  try {
    $output = & $cmd.Source /all 2>$null
    if (-not $output) { return $null }
    $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
    foreach ($line in $output) {
      if ($line -match '(physical address|adresse physique|adresse mac|mac address)') {
        if ($line -match $macRegex) {
          if ($line -match $SkipPattern) { continue }
          return $Matches[0]
        }
      }
    }
  } catch {
    Write-Log "ipconfig failed: $($_.Exception.Message)" 'WARN'
    return $null
  }

  return $null
}

function New-MacCandidate {
  param(
    [string]$Mac,
    [string]$Name,
    [string]$Description,
    [string]$MediaType,
    [string]$PhysicalMediaType,
    [bool]$IsUp,
    [string]$EthernetPattern,
    [string]$WifiPattern
  )

  $label = (($Name, $Description, $MediaType, $PhysicalMediaType) -join ' ').Trim()
  return [pscustomobject]@{
    Mac = $Mac
    Name = $Name
    Description = $Description
    IsUp = $IsUp
    IsEthernet = ($label -match $EthernetPattern)
    IsWifi = ($label -match $WifiPattern)
  }
}

function Select-MacCandidate {
  param(
    [array]$Candidates,
    [string]$Preference,
    [string]$Source
  )

  if (-not $Candidates -or $Candidates.Count -eq 0) { return $null }
  $selected = $null
  $priorities = @()
  switch ($Preference) {
    'ethernet' { $priorities = @('ethernet', 'wifi', 'any') }
    'wifi' { $priorities = @('wifi', 'ethernet', 'any') }
    'any' { $priorities = @('any') }
    default { $priorities = @('ethernet', 'wifi', 'any') }
  }

  foreach ($priority in $priorities) {
    $subset = $Candidates
    if ($priority -eq 'ethernet') { $subset = $Candidates | Where-Object { $_.IsEthernet } }
    if ($priority -eq 'wifi') { $subset = $Candidates | Where-Object { $_.IsWifi } }
    if (-not $subset -or $subset.Count -eq 0) { continue }
    $up = $subset | Where-Object { $_.IsUp }
    $selected = if ($up -and $up.Count -gt 0) { $up | Select-Object -First 1 } else { $subset | Select-Object -First 1 }
    if ($selected) { break }
  }

  if (-not $selected) { $selected = $Candidates | Select-Object -First 1 }
  if ($selected -and $selected.Mac) {
    Write-Log "MAC selected from $Source ($Preference): $($selected.Mac) [$($selected.Name)]"
    return $selected.Mac
  }

  return $null
}

function Log-MacCandidates {
  param(
    [string]$Source,
    [array]$Candidates
  )

  if (-not $Candidates -or $Candidates.Count -eq 0) { return }
  $lines = $Candidates | ForEach-Object {
    $type = if ($_.IsEthernet) { 'eth' } elseif ($_.IsWifi) { 'wifi' } else { 'other' }
    $state = if ($_.IsUp) { 'up' } else { 'down' }
    "$($_.Mac) [$type,$state,$($_.Name)]"
  }
  Write-Log "MAC candidates from $Source: $($lines -join ' | ')"
}

function Get-PrimaryMac {
  $skipPattern = 'Virtual|VPN|Loopback|Bluetooth|Wi-Fi Direct|TAP|Hyper-V|Pseudo|WAN Miniport|RAS'
  $ethernetPattern = '(?i)ethernet|lan|gigabit|gbe|realtek|intel|broadcom|qualcomm|pci'
  $wifiPattern = '(?i)wi-?fi|wireless|802\.11|wlan'

  if (Get-Command Get-NetAdapter -ErrorAction SilentlyContinue) {
    try {
      $netAdapters = Get-NetAdapter -ErrorAction Stop | Where-Object {
        $_.MacAddress -and ($_.HardwareInterface -eq $true -or $_.Virtual -eq $false)
      }
      $netCandidates = @()
      foreach ($adapter in $netAdapters) {
        $netCandidates += New-MacCandidate `
          -Mac $adapter.MacAddress `
          -Name $adapter.Name `
          -Description $adapter.InterfaceDescription `
          -MediaType $adapter.MediaType `
          -PhysicalMediaType $adapter.PhysicalMediaType `
          -IsUp ($adapter.Status -eq 'Up') `
          -EthernetPattern $ethernetPattern `
          -WifiPattern $wifiPattern
      }
      Log-MacCandidates -Source 'Get-NetAdapter' -Candidates $netCandidates
      $selected = Select-MacCandidate -Candidates $netCandidates -Preference $MacPreference -Source 'Get-NetAdapter'
      if ($selected) { return $selected }
    } catch {
      Write-Log "Get-NetAdapter failed: $($_.Exception.Message)" 'WARN'
    }
  }

  $configs = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapterConfiguration'
  $filteredConfigs = $configs | Where-Object {
    $_.MACAddress -and $_.Description -notmatch $skipPattern
  }
  $cfgCandidates = @()
  foreach ($cfg in $filteredConfigs) {
    $cfgCandidates += New-MacCandidate `
      -Mac $cfg.MACAddress `
      -Name $cfg.Caption `
      -Description $cfg.Description `
      -MediaType $null `
      -PhysicalMediaType $null `
      -IsUp ($cfg.IPEnabled -eq $true) `
      -EthernetPattern $ethernetPattern `
      -WifiPattern $wifiPattern
  }
  Log-MacCandidates -Source 'Win32_NetworkAdapterConfiguration' -Candidates $cfgCandidates
  $selected = Select-MacCandidate -Candidates $cfgCandidates -Preference $MacPreference -Source 'Win32_NetworkAdapterConfiguration'
  if ($selected) { return $selected }

  $adapters = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapter'
  $filteredAdapters = $adapters | Where-Object {
    $_.MACAddress -and ($_.PhysicalAdapter -eq $true -or ($_.PNPDeviceID -and $_.PNPDeviceID -notmatch '^ROOT\\')) -and $_.Description -notmatch $skipPattern
  }
  $adapterCandidates = @()
  foreach ($adapter in $filteredAdapters) {
    $adapterCandidates += New-MacCandidate `
      -Mac $adapter.MACAddress `
      -Name $adapter.NetConnectionID `
      -Description $adapter.Description `
      -MediaType $null `
      -PhysicalMediaType $null `
      -IsUp ($adapter.NetConnectionStatus -eq 2) `
      -EthernetPattern $ethernetPattern `
      -WifiPattern $wifiPattern
  }
  Log-MacCandidates -Source 'Win32_NetworkAdapter' -Candidates $adapterCandidates
  $selected = Select-MacCandidate -Candidates $adapterCandidates -Preference $MacPreference -Source 'Win32_NetworkAdapter'
  if ($selected) { return $selected }

  $getmacMac = Get-MacFromGetmac -SkipPattern $skipPattern
  if ($getmacMac) {
    Write-Log "MAC from getmac: $getmacMac"
    return $getmacMac
  }

  $ipconfigMac = Get-MacFromIpconfig -SkipPattern $skipPattern
  if ($ipconfigMac) {
    Write-Log "MAC from ipconfig: $ipconfigMac"
    return $ipconfigMac
  }

  $msinfoMac = Get-MacFromMsinfo -TimeoutSec $MsinfoTimeoutSec
  if ($msinfoMac) {
    Write-Log "MAC from msinfo32: $msinfoMac"
    return $msinfoMac
  }

  return $null
}

function Get-SerialNumber {
  $bios = Get-CimInstanceSafe -ClassName 'Win32_BIOS'
  $serial = ($bios | Select-Object -First 1).SerialNumber
  if ($serial -and $serial -notmatch 'To Be Filled' -and $serial -notmatch 'Default string') {
    return $serial.Trim()
  }

  $enclosure = Get-CimInstanceSafe -ClassName 'Win32_SystemEnclosure'
  $serial = ($enclosure | Select-Object -First 1).SerialNumber
  if ($serial) { return $serial.Trim() }

  return $null
}

function Get-OsVersion {
  $os = Get-CimInstanceSafe -ClassName 'Win32_OperatingSystem'
  $caption = ($os | Select-Object -First 1).Caption
  if ($caption) { return $caption.Trim() }
  $version = ($os | Select-Object -First 1).Version
  if ($version) { return $version.Trim() }
  return $null
}

function Get-RamMb {
  $modules = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMemory'
  $totalBytes = ($modules | Measure-Object -Property Capacity -Sum).Sum
  if ($totalBytes) { return [int][math]::Round($totalBytes / 1MB) }
  return $null
}

function Get-RamSlots {
  $arrays = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMemoryArray'
  $slotsTotal = ($arrays | Measure-Object -Property MemoryDevices -Sum).Sum
  if ($slotsTotal -le 0) { $slotsTotal = $null }

  $modules = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMemory'
  $slotsUsed = ($modules | Measure-Object).Count

  $slotsFree = $null
  if ($slotsTotal -ne $null) {
    $slotsFree = [math]::Max($slotsTotal - $slotsUsed, 0)
  }

  return @{ Total = $slotsTotal; Free = $slotsFree }
}

function Get-BatteryHealth {
  $static = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'BatteryStaticData'
  $full = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'BatteryFullChargedCapacity'

  if (-not $static -or -not $full) { return $null }

  $values = @()
  foreach ($item in $static) {
    $design = $item.DesignedCapacity
    if (-not $design) { $design = $item.DesignCapacity }
    $match = $full | Where-Object { $_.InstanceName -eq $item.InstanceName }
    $fullCap = $match.FullChargedCapacity

    if ($design -and $fullCap -and $design -gt 0 -and $fullCap -gt 0) {
      $health = [math]::Round(($fullCap / $design) * 100)
      if ($health -gt 100) { $health = 100 }
      if ($health -lt 0) { $health = 0 }
      $values += $health
    }
  }

  if ($values.Count -gt 0) {
    return [int][math]::Round(($values | Measure-Object -Average).Average)
  }

  return $null
}

function Get-BatteryInfo {
  $static = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'BatteryStaticData'
  $full = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'BatteryFullChargedCapacity'
  $status = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'BatteryStatus'
  $battery = Get-CimInstanceSafe -ClassName 'Win32_Battery'

  $designWh = $null
  $fullWh = $null
  $remainingWh = $null
  $chargePercent = $null
  $powerSource = $null

  foreach ($item in $static) {
    $design = $item.DesignedCapacity
    if (-not $design) { $design = $item.DesignCapacity }
    if ($design -and $designWh -eq $null) { $designWh = [math]::Round($design / 1000, 1) }
  }

  foreach ($item in $full) {
    if ($item.FullChargedCapacity -and $fullWh -eq $null) {
      $fullWh = [math]::Round($item.FullChargedCapacity / 1000, 1)
    }
  }

  foreach ($item in $status) {
    if ($item.RemainingCapacity -and $remainingWh -eq $null) {
      $remainingWh = [math]::Round($item.RemainingCapacity / 1000, 1)
    }
  }

  $estimatedRuntimeMin = $null
  if ($battery) {
    $batteryItem = $battery | Select-Object -First 1
    $estimatedRuntimeMin = $batteryItem.EstimatedRunTime
    $chargePercent = $batteryItem.EstimatedChargeRemaining
    $statusValue = $batteryItem.BatteryStatus
    if ($statusValue -in 2, 3, 6, 7, 8, 9, 11) { $powerSource = 'ac' }
    elseif ($statusValue -eq 1) { $powerSource = 'battery' }
  }

  return [ordered]@{
    designCapacityWh = $designWh
    fullChargeCapacityWh = $fullWh
    remainingCapacityWh = $remainingWh
    estimatedRuntimeMin = $estimatedRuntimeMin
    chargePercent = $chargePercent
    powerSource = $powerSource
  }
}

function Get-DiskSmartStatus {
  $smart = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'MSStorageDriver_FailurePredictStatus'
  if (-not $smart -or $smart.Count -eq 0) { return 'absent' }
  foreach ($drive in $smart) {
    if ($drive.PredictFailure -eq $true) { return 'nok' }
  }
  return 'ok'
}

function Get-DiskInventory {
  $disks = Get-CimInstanceSafe -ClassName 'Win32_DiskDrive'
  $physicalDisks = @()
  $reliabilityCounters = @{}

  if (Get-Command Get-PhysicalDisk -ErrorAction SilentlyContinue) {
    try {
      $physicalDisks = Get-PhysicalDisk -ErrorAction Stop
    } catch {
      Write-Log "Get-PhysicalDisk failed: $($_.Exception.Message)" 'WARN'
      $physicalDisks = @()
    }
  }

  if (-not $script:IsAdmin) {
    if ($physicalDisks.Count -gt 0) {
      Write-Log 'Storage reliability counters skipped (admin required)' 'WARN'
    }
  } elseif (Get-Command Get-StorageReliabilityCounter -ErrorAction SilentlyContinue) {
    foreach ($pd in $physicalDisks) {
      try {
        $reliabilityCounters[$pd.UniqueId] = Get-StorageReliabilityCounter -PhysicalDisk $pd -ErrorAction Stop
      } catch {
        Write-Log "Get-StorageReliabilityCounter failed for $($pd.FriendlyName): $($_.Exception.Message)" 'WARN'
        continue
      }
    }
  }

  $list = @()
  foreach ($disk in $disks) {
    $sizeGb = $null
    if ($disk.Size) { $sizeGb = [math]::Round($disk.Size / 1GB, 1) }

    $matched = $null
    foreach ($pd in $physicalDisks) {
      if ($pd.SerialNumber -and $disk.SerialNumber -and $pd.SerialNumber.Trim() -eq $disk.SerialNumber.Trim()) {
        $matched = $pd
        break
      }
      if ($pd.FriendlyName -and $disk.Model -and $pd.FriendlyName -eq $disk.Model) {
        $matched = $pd
      }
    }

    $reliability = $null
    if ($matched -and $reliabilityCounters.ContainsKey($matched.UniqueId)) {
      $reliability = $reliabilityCounters[$matched.UniqueId]
    }

    $list += [ordered]@{
      model = $disk.Model
      serialNumber = $disk.SerialNumber
      interface = $disk.InterfaceType
      mediaType = $disk.MediaType
      sizeGb = $sizeGb
      firmware = $disk.FirmwareRevision
      healthStatus = if ($matched) { $matched.HealthStatus } else { $null }
      mediaTypeDetail = if ($matched) { $matched.MediaType } else { $null }
      powerOnHours = if ($reliability) { $reliability.PowerOnHours } else { $null }
      powerCycleCount = if ($reliability) { $reliability.PowerCycleCount } else { $null }
      temperatureC = if ($reliability) { $reliability.Temperature } else { $null }
      wearPercent = if ($reliability) { $reliability.Wear } else { $null }
    }
  }
  return $list
}

function Get-VolumeInventory {
  if (Get-Command Get-Volume -ErrorAction SilentlyContinue) {
    try {
      $volumes = Get-Volume | Where-Object { $_.DriveLetter }
      return $volumes | ForEach-Object {
        [ordered]@{
          drive = $_.DriveLetter
          fileSystem = $_.FileSystem
          sizeGb = if ($_.Size) { [math]::Round($_.Size / 1GB, 1) } else { $null }
          freeGb = if ($_.SizeRemaining) { [math]::Round($_.SizeRemaining / 1GB, 1) } else { $null }
        }
      }
    } catch {
      return @()
    }
  }

  $logical = Get-CimInstanceSafe -ClassName 'Win32_LogicalDisk' -Filter 'DriveType=3'
  return $logical | ForEach-Object {
    [ordered]@{
      drive = $_.DeviceID
      fileSystem = $_.FileSystem
      sizeGb = if ($_.Size) { [math]::Round($_.Size / 1GB, 1) } else { $null }
      freeGb = if ($_.FreeSpace) { [math]::Round($_.FreeSpace / 1GB, 1) } else { $null }
    }
  }
}

function Invoke-WinsatCapture {
  param(
    [string[]]$Arguments,
    [int]$TimeoutSec
  )

  $cmd = Get-Command winsat -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent'; mbps = $null } }
  if (-not $script:IsAdmin) {
    Write-Log 'WinSAT skipped (admin required)' 'WARN'
    return @{ status = 'denied'; mbps = $null }
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $cmd.Source
  $psi.Arguments = ($Arguments -join ' ')
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "WinSAT start failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'denied'; mbps = $null }
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      return @{ status = 'timeout'; mbps = $null }
    }
  } catch {
    Write-Log "WinSAT wait failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok'; mbps = $null }
  }

  $output = ($proc.StandardOutput.ReadToEnd() + $proc.StandardError.ReadToEnd())
  $status = if ($proc.ExitCode -eq 0) { 'ok' } else { 'nok' }

  $matches = [regex]::Matches($output, '([0-9]+(?:[\.,][0-9]+)?)\s*MB/s')
  $mbps = $null
  if ($matches.Count -gt 0) {
    $values = @()
    foreach ($match in $matches) {
      $raw = $match.Groups[1].Value.Replace(',', '.')
      $parsed = [double]::Parse($raw, [System.Globalization.CultureInfo]::InvariantCulture)
      $values += $parsed
    }
    if ($values.Count -gt 0) {
      $mbps = [math]::Round(($values | Measure-Object -Maximum).Maximum, 1)
    }
  }

  return @{ status = $status; mbps = $mbps }
}

function Invoke-ExternalTest {
  param(
    [string]$Path,
    [string[]]$Arguments = @(),
    [int]$TimeoutSec = 20,
    [string]$Name = 'External'
  )

  if (-not $Path -or -not (Test-Path $Path)) { return $null }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $Path
  if ($Arguments -and $Arguments.Count -gt 0) {
    $psi.Arguments = ($Arguments -join ' ')
  }
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "$Name test start failed: $($_.Exception.Message)" 'WARN'
    return 'nok'
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      Write-Log "$Name test timeout after ${TimeoutSec}s" 'WARN'
      return 'nok'
    }
  } catch {
    Write-Log "$Name test wait failed: $($_.Exception.Message)" 'WARN'
    return 'nok'
  }

  if ($proc.ExitCode -eq 0) { return 'ok' }
  Write-Log "$Name test failed with exit code $($proc.ExitCode)" 'WARN'
  return 'nok'
}

function Run-WinsatLoop {
  param(
    [string[]]$Arguments,
    [int]$Loops,
    [int]$TimeoutSec
  )

  if ($Loops -le 0) { return $null }
  $resultStatus = 'ok'
  $bestMbps = $null

  for ($i = 1; $i -le $Loops; $i++) {
    $result = Invoke-WinsatCapture -Arguments $Arguments -TimeoutSec $TimeoutSec
    if ($result.status -ne 'ok') {
      $resultStatus = $result.status
      break
    }
    if ($result.mbps -ne $null) {
      if ($bestMbps -eq $null -or $result.mbps -gt $bestMbps) {
        $bestMbps = $result.mbps
      }
    }
  }

  return @{ status = $resultStatus; mbps = $bestMbps }
}

function Get-PowerPlanName {
  $powercfg = Get-Command powercfg -ErrorAction SilentlyContinue
  if (-not $powercfg) { return $null }

  try {
    $output = & $powercfg.Source /getactivescheme 2>$null
    if (-not $output) { return $null }
    if ($output -match '\((.+)\)') { return $Matches[1].Trim() }
  } catch {
    return $null
  }
  return $null
}

function Get-BootMode {
  $value = Get-RegistryValue -Path 'HKLM:\SYSTEM\CurrentControlSet\Control' -Name 'PEFirmwareType'
  if ($value -eq 1) { return 'Legacy' }
  if ($value -eq 2) { return 'UEFI' }
  return 'unknown'
}

function Get-SecureBootStatus {
  try {
    $secure = Confirm-SecureBootUEFI
    if ($secure -eq $true) { return 'enabled' }
    if ($secure -eq $false) { return 'disabled' }
  } catch {
    return $null
  }
  return $null
}

function Get-FastStartupStatus {
  $value = Get-RegistryValue -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Power' -Name 'HiberbootEnabled'
  if ($value -eq $null) { return $null }
  if ($value -eq 1) { return 'enabled' }
  return 'disabled'
}

function Get-BootDurationMs {
  try {
    $event = Get-WinEvent -FilterHashtable @{ LogName = 'Microsoft-Windows-Diagnostics-Performance/Operational'; Id = 100 } -MaxEvents 1 -ErrorAction SilentlyContinue
    if (-not $event) { return $null }
    $xml = [xml]$event.ToXml()
    foreach ($data in $xml.Event.EventData.Data) {
      if ($data.Name -eq 'BootDuration') {
        return [int]$data.'#text'
      }
    }
  } catch {
    return $null
  }
  return $null
}

function Get-CpuUsagePercent {
  $perf = Get-CimInstanceSafe -ClassName 'Win32_PerfFormattedData_PerfOS_Processor' -Filter "Name='_Total'"
  if ($perf) { return $perf.PercentProcessorTime }
  return $null
}

function Get-DeviceGuardStatus {
  $dg = Get-CimInstanceSafe -ClassName 'Win32_DeviceGuard'
  if (-not $dg) { return $null }
  if ($dg.SecurityServicesRunning -contains 1) { return 'enabled' }
  if ($dg.SecurityServicesConfigured -contains 1) { return 'configured' }
  return 'disabled'
}

function Get-AntivirusNames {
  $av = Get-CimInstanceSafe -Namespace 'root\SecurityCenter2' -ClassName 'AntiVirusProduct'
  if (-not $av) { return @() }
  return ($av | Select-Object -ExpandProperty displayName)
}

function Get-MemoryTypeLabel {
  param([int]$Type)

  switch ($Type) {
    20 { return 'DDR' }
    21 { return 'DDR2' }
    22 { return 'DDR2 FB-DIMM' }
    24 { return 'DDR3' }
    26 { return 'DDR4' }
    27 { return 'DDR4' }
    30 { return 'DDR4' }
    34 { return 'DDR5' }
    35 { return 'DDR5' }
    default { return $null }
  }
}

function Get-VideoOutputLabel {
  param([long]$Code)

  if ($Code -eq $null) { return $null }
  if ($Code -lt 0 -or $Code -gt 2147483647) { return $null }

  switch ([int]$Code) {
    0 { return 'HD15' }
    1 { return 'S-Video' }
    2 { return 'Composite' }
    3 { return 'Component' }
    4 { return 'DVI' }
    5 { return 'HDMI' }
    6 { return 'LVDS' }
    8 { return 'DisplayPort' }
    9 { return 'eDP' }
    10 { return 'MIPI' }
    11 { return 'HDMI' }
    12 { return 'USB-C' }
    default { return $null }
  }
}

function Get-DisplayInfo {
  $info = [ordered]@{}

  $monId = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'WmiMonitorID'
  $basic = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'WmiMonitorBasicDisplayParams'
  $conn = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'WmiMonitorConnectionParams'
  $modes = Get-CimInstanceSafe -Namespace 'root\wmi' -ClassName 'WmiMonitorListedSupportedSourceModes'

  if ($monId) {
    $first = $monId | Select-Object -First 1
    $info.model = Decode-WmiString $first.UserFriendlyName
    $info.manufacturer = Decode-WmiString $first.ManufacturerName
    $info.serialNumber = Decode-WmiString $first.SerialNumberID
    if ($first.YearOfManufacture) { $info.manufactureYear = $first.YearOfManufacture }
  }

  if ($basic) {
    $firstBasic = $basic | Select-Object -First 1
    if ($firstBasic.MaxHorizontalImageSize -and $firstBasic.MaxVerticalImageSize) {
      $diagCm = [math]::Sqrt([math]::Pow($firstBasic.MaxHorizontalImageSize, 2) + [math]::Pow($firstBasic.MaxVerticalImageSize, 2))
      $info.sizeInches = [math]::Round($diagCm / 2.54, 1)
    }
  }

  if ($conn) {
    $firstConn = $conn | Select-Object -First 1
    if ($firstConn.VideoOutputTechnology -ne $null) {
      $connectionLabel = Get-VideoOutputLabel -Code $firstConn.VideoOutputTechnology
      if (-not $connectionLabel) {
        Write-Log "Unknown video output code: $($firstConn.VideoOutputTechnology)" 'WARN'
      } else {
        $info.connection = $connectionLabel
      }
    }
  }

  if ($modes) {
    $maxWidth = 0
    $maxHeight = 0
    foreach ($mode in $modes) {
      foreach ($entry in $mode.MonitorSourceModes) {
        if ($entry.HorizontalActivePixels -gt $maxWidth) { $maxWidth = $entry.HorizontalActivePixels }
        if ($entry.VerticalActivePixels -gt $maxHeight) { $maxHeight = $entry.VerticalActivePixels }
      }
    }
    if ($maxWidth -gt 0 -and $maxHeight -gt 0) {
      $info.maxResolution = "${maxWidth}x${maxHeight}"
    }
  }

  $video = Get-CimInstanceSafe -ClassName 'Win32_VideoController'
  $primary = $video | Where-Object { $_.CurrentHorizontalResolution } | Select-Object -First 1
  if (-not $primary) { $primary = $video | Select-Object -First 1 }
  if ($primary) {
    if ($primary.CurrentHorizontalResolution -and $primary.CurrentVerticalResolution) {
      $info.currentResolution = "${primary.CurrentHorizontalResolution}x${primary.CurrentVerticalResolution}"
    }
    if ($primary.CurrentRefreshRate) { $info.currentRefreshRate = $primary.CurrentRefreshRate }
    $info.connectedTo = $primary.Name
  }

  return $info
}

$testLoops = 0
if ($TestMode -eq 'quick') { $testLoops = 1 }
if ($TestMode -eq 'stress') { $testLoops = [math]::Max($StressLoops, 2) }

$categoryValue = if ($Category -eq 'auto') { Get-ChassisCategory } else { $Category }

$system = Get-CimInstanceSafe -ClassName 'Win32_ComputerSystem'
$vendor = ($system | Select-Object -First 1).Manufacturer
$model = ($system | Select-Object -First 1).Model

$baseboard = Get-CimInstanceSafe -ClassName 'Win32_BaseBoard' | Select-Object -First 1
$biosInfo = Get-CimInstanceSafe -ClassName 'Win32_BIOS' | Select-Object -First 1

$hostname = $env:COMPUTERNAME
$macAddress = Get-PrimaryMac
$serialNumber = Get-SerialNumber
$osVersion = Get-OsVersion
$ramMb = Get-RamMb
$slotsInfo = Get-RamSlots
$batteryHealth = Get-BatteryHealth
$batteryInfo = Get-BatteryInfo

$cameraDevices = @()
$cameraDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='Camera'"
$cameraDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='Image'"
$cameraPresence = Get-StatusFromDevices $cameraDevices
$cameraTestStatus = $null
$cameraStatus = $null
if ($cameraPresence -eq 'absent' -or $cameraPresence -eq 'nok') {
  $cameraStatus = $cameraPresence
} else {
  if ($CameraTestPath -and -not (Test-Path $CameraTestPath)) {
    Write-Log "Camera test binary not found: $CameraTestPath" 'WARN'
  }
  $cameraTestStatus = Invoke-ExternalTest -Path $CameraTestPath -TimeoutSec $CameraTestTimeoutSec -Name 'Camera'
  if ($cameraTestStatus) {
    $cameraStatus = $cameraTestStatus
  } else {
    $cameraStatus = 'not_tested'
  }
}
Write-Log "Camera presence=$cameraPresence TestPath=$CameraTestPath TestStatus=$cameraTestStatus Final=$cameraStatus"

$usbDevices = Get-CimInstanceSafe -ClassName 'Win32_USBController'
$usbStatus = Get-StatusFromDevices $usbDevices

$keyboardDevices = Get-CimInstanceSafe -ClassName 'Win32_Keyboard'
$keyboardStatus = Get-StatusFromDevices $keyboardDevices

$pointingDevices = Get-CimInstanceSafe -ClassName 'Win32_PointingDevice'
$padDevices = $pointingDevices | Where-Object {
  $_.Description -match 'touchpad|trackpad|precision|clickpad|synaptics|elan|alps' -or
  $_.Name -match 'touchpad|trackpad|precision|clickpad|synaptics|elan|alps'
}
if (-not $padDevices -or $padDevices.Count -eq 0) {
  $padDevices = Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='Mouse'" | Where-Object {
    $_.Name -match 'touchpad|trackpad|precision|clickpad|synaptics|elan|alps'
  }
}
$padStatus = Get-StatusFromDevices $padDevices

$badgeDevices = @()
$badgeDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='SmartCardReader'"
$badgeDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='SecurityDevices'"
if (-not $badgeDevices -or $badgeDevices.Count -eq 0) {
  $badgeDevices = Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' | Where-Object {
    $_.Name -match 'smart card|badge|rfid|smartcard'
  }
}
$badgeStatus = Get-StatusFromDevices $badgeDevices

$diskSmart = Get-DiskSmartStatus
$diskInventory = Get-DiskInventory
$volumeInventory = Get-VolumeInventory

$systemDrive = $env:SystemDrive
if (-not $systemDrive) { $systemDrive = 'C:' }
$driveLetter = $systemDrive.TrimEnd('\')

$diskReadTest = Run-WinsatLoop -Arguments @('disk', '-seq', '-read', '-drive', $driveLetter) -Loops $testLoops -TimeoutSec $DiskTestTimeoutSec
$diskWriteTest = Run-WinsatLoop -Arguments @('disk', '-seq', '-write', '-drive', $driveLetter) -Loops $testLoops -TimeoutSec $DiskTestTimeoutSec
$memTest = Run-WinsatLoop -Arguments @('mem') -Loops $testLoops -TimeoutSec $MemTestTimeoutSec

$powerPlan = Get-PowerPlanName
$bootMode = Get-BootMode
$secureBoot = Get-SecureBootStatus
$fastStartup = Get-FastStartupStatus
$bootDurationMs = Get-BootDurationMs
$cpuUsage = Get-CpuUsagePercent
$vbsStatus = Get-DeviceGuardStatus
$avNames = Get-AntivirusNames

$os = Get-CimInstanceSafe -ClassName 'Win32_OperatingSystem' | Select-Object -First 1
$uptime = $null
if ($os) {
  $lastBoot = Convert-WmiDate $os.LastBootUpTime
  if ($lastBoot) { $uptime = [int]([DateTime]::Now - $lastBoot).TotalMinutes }
}

$osRegPath = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion'
$osEdition = Get-RegistryValue -Path $osRegPath -Name 'ProductName'
$osDisplayVersion = Get-RegistryValue -Path $osRegPath -Name 'DisplayVersion'
$osReleaseId = Get-RegistryValue -Path $osRegPath -Name 'ReleaseId'
$osBuild = Get-RegistryValue -Path $osRegPath -Name 'CurrentBuildNumber'
$osUbr = Get-RegistryValue -Path $osRegPath -Name 'UBR'
$osInstallDateRaw = Get-RegistryValue -Path $osRegPath -Name 'InstallDate'
$osInstallDate = $null
if ($osInstallDateRaw) {
  $osInstallDate = [DateTimeOffset]::FromUnixTimeSeconds($osInstallDateRaw).DateTime.ToString('o')
}

$cpu = Get-CimInstanceSafe -ClassName 'Win32_Processor' | Select-Object -First 1
$cpuInfo = [ordered]@{
  name = $cpu.Name
  manufacturer = $cpu.Manufacturer
  cores = $cpu.NumberOfCores
  threads = $cpu.NumberOfLogicalProcessors
  baseClockMHz = $cpu.MaxClockSpeed
  currentClockMHz = $cpu.CurrentClockSpeed
  usagePercent = $cpuUsage
  powerPlan = $powerPlan
}

$video = Get-CimInstanceSafe -ClassName 'Win32_VideoController'
$primaryVideo = $video | Where-Object { $_.CurrentHorizontalResolution } | Select-Object -First 1
if (-not $primaryVideo) { $primaryVideo = $video | Select-Object -First 1 }
$gpuInfo = [ordered]@{
  name = $primaryVideo.Name
  vendor = $primaryVideo.AdapterCompatibility
  vramMb = if ($primaryVideo.AdapterRAM) { [math]::Round($primaryVideo.AdapterRAM / 1MB) } else { $null }
  driverVersion = $primaryVideo.DriverVersion
  driverDate = $primaryVideo.DriverDate
}

$memoryModules = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMemory'
$memType = $null
$memSpeed = $null
if ($memoryModules) {
  $memType = Get-MemoryTypeLabel -Type ($memoryModules | Select-Object -First 1).SMBIOSMemoryType
  $memSpeed = ($memoryModules | Measure-Object -Property ConfiguredClockSpeed -Maximum).Maximum
}

$memoryUsage = $null
if ($os) {
  $totalMb = [math]::Round($os.TotalVisibleMemorySize / 1024)
  $freeMb = [math]::Round($os.FreePhysicalMemory / 1024)
  $usedMb = $totalMb - $freeMb
  $percent = $null
  if ($totalMb -gt 0) { $percent = [math]::Round(($usedMb / $totalMb) * 100) }
  $memoryUsage = [ordered]@{ usedMb = $usedMb; totalMb = $totalMb; percent = $percent }
}

$pageFile = Get-CimInstanceSafe -ClassName 'Win32_PageFileUsage' | Select-Object -First 1
$pageFileInfo = $null
if ($pageFile) {
  $pageFileInfo = [ordered]@{
    allocatedMb = $pageFile.AllocatedBaseSize
    currentMb = $pageFile.CurrentUsage
    peakMb = $pageFile.PeakUsage
  }
}

$memorySlots = @()
foreach ($module in $memoryModules) {
  $memorySlots += [ordered]@{
    slot = $module.DeviceLocator
    bank = $module.BankLabel
    manufacturer = $module.Manufacturer
    partNumber = $module.PartNumber
    serialNumber = $module.SerialNumber
    sizeGb = if ($module.Capacity) { [math]::Round($module.Capacity / 1GB, 1) } else { $null }
    speedMHz = $module.ConfiguredClockSpeed
  }
}

$displayInfo = Get-DisplayInfo

$biosReleaseDate = $null
if ($biosInfo -and $biosInfo.ReleaseDate) {
  $converted = Convert-WmiDate $biosInfo.ReleaseDate
  if ($converted) { $biosReleaseDate = $converted.ToString('o') }
}

$diag = [ordered]@{
  type = $TestMode
  diagnosticsPerformed = 0
  appVersion = $scriptVersion
}

$tests = [ordered]@{}
if ($diskReadTest) {
  $tests.diskRead = $diskReadTest.status
  if ($diskReadTest.mbps -ne $null) { $tests.diskReadMBps = $diskReadTest.mbps }
}
if ($diskWriteTest) {
  $tests.diskWrite = $diskWriteTest.status
  if ($diskWriteTest.mbps -ne $null) { $tests.diskWriteMBps = $diskWriteTest.mbps }
}
if ($memTest) {
  $tests.ramTest = $memTest.status
  if ($memTest.mbps -ne $null) { $tests.ramMBps = $memTest.mbps }
}

$diag.diagnosticsPerformed = $tests.Keys.Count

$stopwatch.Stop()
$diag.completedAt = (Get-Date).ToString('o')
$diag.durationSec = [int]$stopwatch.Elapsed.TotalSeconds

$payload = [ordered]@{}
if ($hostname) { $payload.hostname = $hostname }
if ($macAddress) { $payload.macAddress = $macAddress }
if ($serialNumber) { $payload.serialNumber = $serialNumber }
if ($categoryValue) { $payload.category = $categoryValue }
if ($vendor) { $payload.vendor = $vendor }
if ($model) { $payload.model = $model }
if ($osVersion) { $payload.osVersion = $osVersion }
if ($ramMb) { $payload.ramMb = $ramMb }
if ($slotsInfo.Total -ne $null) { $payload.ramSlotsTotal = $slotsInfo.Total }
if ($slotsInfo.Free -ne $null) { $payload.ramSlotsFree = $slotsInfo.Free }
if ($batteryHealth -ne $null) { $payload.batteryHealth = $batteryHealth }
if ($cameraStatus) { $payload.cameraStatus = $cameraStatus }
if ($usbStatus) { $payload.usbStatus = $usbStatus }
if ($keyboardStatus) { $payload.keyboardStatus = $keyboardStatus }
if ($padStatus) { $payload.padStatus = $padStatus }
if ($badgeStatus) { $payload.badgeReaderStatus = $badgeStatus }
if ($diskInventory.Count -gt 0) { $payload.disks = $diskInventory }
if ($volumeInventory.Count -gt 0) { $payload.volumes = $volumeInventory }

$payload.diag = $diag
$payload.device = [ordered]@{
  manufacturer = $vendor
  model = $model
  motherboard = $baseboard.Product
  powerSource = if ($batteryInfo.powerSource) { $batteryInfo.powerSource } else { $null }
  batteryCapacity = $batteryInfo
}

$payload.bios = [ordered]@{
  vendor = $biosInfo.Manufacturer
  version = $biosInfo.SMBIOSBIOSVersion
  releaseDate = $biosReleaseDate
  bootMode = $bootMode
  secureBoot = $secureBoot
  fastStartup = $fastStartup
  bootDurationMs = $bootDurationMs
}

$payload.windows = [ordered]@{
  edition = $osEdition
  version = if ($osDisplayVersion) { $osDisplayVersion } else { $osReleaseId }
  build = if ($osBuild) { if ($osUbr -ne $null) { "$osBuild.$osUbr" } else { $osBuild } } else { $null }
  installedOn = $osInstallDate
  fastStartup = $fastStartup
  uptimeMinutes = $uptime
  vbs = $vbsStatus
  antivirus = $avNames
}

$payload.cpu = $cpuInfo
$payload.gpu = $gpuInfo

$payload.memory = [ordered]@{
  totalGb = if ($ramMb) { [math]::Round($ramMb / 1024, 1) } else { $null }
  type = $memType
  speedMHz = $memSpeed
  usage = $memoryUsage
  pagefile = $pageFileInfo
}

if ($memorySlots.Count -gt 0) { $payload.memorySlots = $memorySlots }
if ($displayInfo.Count -gt 0) { $payload.display = $displayInfo }
if ($tests.Count -gt 0) { $payload.tests = $tests }

$components = @{}
if ($cameraStatus) { $components.camera = $cameraStatus }
if ($usbStatus) { $components.usb = $usbStatus }
if ($keyboardStatus) { $components.keyboard = $keyboardStatus }
if ($padStatus) { $components.pad = $padStatus }
if ($badgeStatus) { $components.badgeReader = $badgeStatus }
if ($diskSmart) { $components.diskSmart = $diskSmart }
if ($tests.diskRead) { $components.diskReadTest = $tests.diskRead }
if ($tests.diskWrite) { $components.diskWriteTest = $tests.diskWrite }
if ($tests.ramTest) { $components.ramTest = $tests.ramTest }

if ($components.Count -gt 0) {
  $payload.components = $components
}

$json = $payload | ConvertTo-Json -Depth 10

Write-Log "Payload size=$($json.Length)"

try {
  $response = Invoke-RestMethod -Uri $ApiUrl -Method Post -ContentType 'application/json' -Body $json -TimeoutSec $TimeoutSec
  Write-Log 'Ingest OK'
  Write-Output $response
} catch {
  Write-Log $_.Exception.Message 'ERROR'
  if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
    Write-Log $_.ErrorDetails.Message 'ERROR'
  }
  Write-Error $_
  exit 1
}
