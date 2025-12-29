[CmdletBinding()]
param(
  [string]$ApiUrl = $env:MDT_API_URL,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [ValidateSet('none', 'quick', 'stress')][string]$TestMode = 'quick',
  [int]$TimeoutSec = 15,
  [int]$DiskTestTimeoutSec = 180,
  [int]$MemTestTimeoutSec = 180,
  [int]$CpuTestTimeoutSec = 180,
  [int]$GpuTestTimeoutSec = 240,
  [int]$FsCheckTimeoutSec = 120,
  [ValidateSet('auto', 'none', 'scan')][string]$FsCheckMode = 'auto',
  [ValidateSet('none', 'schedule')][string]$MemDiagMode = 'none',
  [int]$StressLoops = 2,
  [string]$CpuTestPath = $env:MDT_CPU_TEST_PATH,
  [string[]]$CpuTestArguments = $env:MDT_CPU_TEST_ARGS,
  [string]$GpuTestPath = $env:MDT_GPU_TEST_PATH,
  [string[]]$GpuTestArguments = $env:MDT_GPU_TEST_ARGS,
  [string]$NetworkTestPath = $env:MDT_IPERF_PATH,
  [string]$NetworkTestServer = $env:MDT_IPERF_SERVER,
  [int]$NetworkTestPort = 5201,
  [int]$NetworkTestSeconds = 10,
  [int]$NetworkTestTimeoutSec = 40,
  [ValidateSet('download', 'upload', 'both')][string]$NetworkTestDirection = 'download',
  [string[]]$NetworkTestExtraArgs = $env:MDT_IPERF_ARGS,
  [string]$NetworkPingTarget = $env:MDT_PING_TARGET,
  [int]$NetworkPingCount = 2,
  [string]$CameraTestPath = $env:MDT_CAMERA_TEST_PATH,
  [string[]]$CameraTestArguments = $env:MDT_CAMERA_TEST_ARGS,
  [int]$CameraTestTimeoutSec = 20,
  [int]$MsinfoTimeoutSec = 0,
  [ValidateSet('auto', 'ethernet', 'wifi', 'any')][string]$MacPreference = 'auto',
  [string]$LogPath,
  [string]$Technician = $env:MDT_TECHNICIAN,
  [string]$KeyboardCapturePath,
  [string]$KeyboardCaptureLogPath,
  [string]$KeyboardCaptureConfigDir,
  [string]$KeyboardCaptureLayout,
  [string]$KeyboardCaptureLayoutConfig,
  [switch]$KeyboardCaptureBlockInput,
  [switch]$SkipKeyboardCapture,
  [switch]$SkipStressScript,
  [switch]$SkipTlsValidation
)

$scriptVersion = '1.3.3'
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

if (-not $ApiUrl) {
  $ApiUrl = 'http://192.168.1.36:3000/api/ingest'
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

function Normalize-ArgumentList {
  param([string[]]$Values)

  if (-not $Values -or $Values.Count -eq 0) { return @() }
  if ($Values.Count -eq 1) {
    $single = $Values[0]
    if (-not $single) { return @() }
    return ($single -split '\s+') | Where-Object { $_ -ne '' }
  }
  return $Values
}

function Resolve-KeyboardCapturePath {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return Join-Path $PSScriptRoot 'keyboard_capture.ps1'
  }
  if ([System.IO.Path]::IsPathRooted($Value)) {
    return $Value
  }
  return Join-Path (Get-Location) $Value
}

function Start-KeyboardCapture {
  param(
    [string]$ScriptPath,
    [string]$LogPath,
    [string]$ConfigDir,
    [string]$Layout,
    [string]$LayoutConfig,
    [switch]$BlockInput
  )

  if (-not $ScriptPath) { return $false }
  if (-not (Test-Path $ScriptPath)) {
    Write-Log "Keyboard capture script not found: $ScriptPath" 'WARN'
    return $false
  }

  $psCmd = Get-Command powershell.exe -ErrorAction SilentlyContinue
  if (-not $psCmd) {
    Write-Log 'powershell.exe not found for keyboard capture' 'WARN'
    return $false
  }

  $args = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath)
  if ($LogPath) { $args += @('-LogPath', $LogPath) }
  if ($ConfigDir) { $args += @('-ConfigDir', $ConfigDir) }
  if ($Layout) { $args += @('-Layout', $Layout) }
  if ($LayoutConfig) { $args += @('-LayoutConfig', $LayoutConfig) }
  if ($BlockInput) { $args += '-BlockInput' }

  try {
    Start-Process -FilePath $psCmd.Source -ArgumentList $args -WindowStyle Normal | Out-Null
    Write-Log "Keyboard capture launched: $ScriptPath"
    return $true
  } catch {
    Write-Log "Keyboard capture launch failed: $($_.Exception.Message)" 'WARN'
    return $false
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

if ($TestMode -eq 'stress' -and -not $SkipStressScript) {
  $stressScriptPath = Join-Path $PSScriptRoot 'mdt-stress.ps1'
  if (Test-Path $stressScriptPath) {
    Write-Log "Delegating stress run to mdt-stress.ps1"
    $delegateParams = @{
      ApiUrl = $ApiUrl
      Category = $Category
      TimeoutSec = $TimeoutSec
      DiskTestTimeoutSec = $DiskTestTimeoutSec
      MemTestTimeoutSec = $MemTestTimeoutSec
      CpuTestTimeoutSec = $CpuTestTimeoutSec
      GpuTestTimeoutSec = $GpuTestTimeoutSec
      FsCheckTimeoutSec = $FsCheckTimeoutSec
      FsCheckMode = $FsCheckMode
      MemDiagMode = $MemDiagMode
      StressLoops = $StressLoops
      CpuTestPath = $CpuTestPath
      CpuTestArguments = $CpuTestArguments
      GpuTestPath = $GpuTestPath
      GpuTestArguments = $GpuTestArguments
      NetworkTestPath = $NetworkTestPath
      NetworkTestServer = $NetworkTestServer
      NetworkTestPort = $NetworkTestPort
      NetworkTestSeconds = $NetworkTestSeconds
      NetworkTestTimeoutSec = $NetworkTestTimeoutSec
      NetworkTestDirection = $NetworkTestDirection
      NetworkTestExtraArgs = $NetworkTestExtraArgs
      NetworkPingTarget = $NetworkPingTarget
      NetworkPingCount = $NetworkPingCount
      CameraTestPath = $CameraTestPath
      CameraTestArguments = $CameraTestArguments
      CameraTestTimeoutSec = $CameraTestTimeoutSec
      MsinfoTimeoutSec = $MsinfoTimeoutSec
      MacPreference = $MacPreference
      LogPath = $LogPath
      Technician = $Technician
    }
    if ($SkipTlsValidation) { $delegateParams.SkipTlsValidation = $true }
    & $stressScriptPath @delegateParams
    exit $LASTEXITCODE
  } else {
    Write-Log "Stress script not found: $stressScriptPath" 'WARN'
  }
}

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

  $macs = Get-MacsFromMsinfo -TimeoutSec $TimeoutSec
  if ($macs -and $macs.Count -gt 0) { return $macs[0] }
  return $null
}

function Normalize-MacAddress {
  param([string]$Value)

  if (-not $Value) { return $null }
  $clean = ($Value -replace '[^0-9A-Fa-f]', '').ToUpper()
  if ($clean.Length -ne 12) { return $null }
  return (($clean -split '(.{2})' | Where-Object { $_ }) -join ':')
}

function Get-MacsFromGetmac {
  param([string]$SkipPattern)

  $cmd = Get-Command getmac -ErrorAction SilentlyContinue
  if (-not $cmd) { return @() }

  $list = @()
  try {
    $output = & $cmd.Source /V /FO CSV /NH 2>$null
    if (-not $output) { return @() }
    $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
    foreach ($line in $output) {
      $row = $line | ConvertFrom-Csv -Header 'Connection','Adapter','Mac','Transport'
      if (-not $row) { continue }
      $mac = $row.Mac
      if (-not $mac -or $mac -notmatch $macRegex) { continue }
      if ($row.Adapter -and $row.Adapter -match $SkipPattern) { continue }
      if ($row.Connection -and $row.Connection -match $SkipPattern) { continue }
      $normalized = Normalize-MacAddress $mac
      if ($normalized) { $list += $normalized }
    }
  } catch {
    Write-Log "getmac failed: $($_.Exception.Message)" 'WARN'
    return @()
  }

  return $list | Sort-Object -Unique
}

function Get-MacsFromIpconfig {
  param([string]$SkipPattern)

  $cmd = Get-Command ipconfig -ErrorAction SilentlyContinue
  if (-not $cmd) { return @() }

  $list = @()
  try {
    $output = & $cmd.Source /all 2>$null
    if (-not $output) { return @() }
    $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
    foreach ($line in $output) {
      if ($line -match '(physical address|adresse physique|adresse mac|mac address)') {
        if ($line -match $macRegex) {
          if ($line -match $SkipPattern) { continue }
          $normalized = Normalize-MacAddress $Matches[0]
          if ($normalized) { $list += $normalized }
        }
      }
    }
  } catch {
    Write-Log "ipconfig failed: $($_.Exception.Message)" 'WARN'
    return @()
  }

  return $list | Sort-Object -Unique
}

function Get-MacsFromMsinfo {
  param([int]$TimeoutSec = 30)

  if ($TimeoutSec -le 0) { return @() }

  $cmd = Get-Command msinfo32 -ErrorAction SilentlyContinue
  if (-not $cmd) { return @() }

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
    return @()
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      Write-Log "msinfo32 timed out after ${TimeoutSec}s" 'WARN'
      return @()
    }
  } catch {
    Write-Log "msinfo32 wait failed: $($_.Exception.Message)" 'WARN'
    return @()
  }

  if (-not (Test-Path $reportPath)) {
    Write-Log 'msinfo32 report not found' 'WARN'
    return @()
  }

  $content = Get-Content -Path $reportPath -ErrorAction SilentlyContinue
  Remove-Item -Path $reportPath -ErrorAction SilentlyContinue
  if (-not $content) { return @() }

  $macRegex = '([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2}'
  $matches = [regex]::Matches(($content -join "`n"), $macRegex)
  $list = @()
  foreach ($match in $matches) {
    $normalized = Normalize-MacAddress $match.Value
    if ($normalized -and $normalized -notmatch '^00(:00){5}$') { $list += $normalized }
  }

  return $list | Sort-Object -Unique
}

function Get-MacFromGetmac {
  param([string]$SkipPattern)

  $macs = Get-MacsFromGetmac -SkipPattern $SkipPattern
  if ($macs -and $macs.Count -gt 0) { return $macs[0] }
  return $null
}

function Get-MacFromIpconfig {
  param([string]$SkipPattern)

  $macs = Get-MacsFromIpconfig -SkipPattern $SkipPattern
  if ($macs -and $macs.Count -gt 0) { return $macs[0] }
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
  Write-Log ("MAC candidates from {0}: {1}" -f $Source, ($lines -join ' | '))
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

function Get-AllMacs {
  $skipPattern = 'Virtual|VPN|Loopback|Bluetooth|Wi-Fi Direct|TAP|Hyper-V|Pseudo|WAN Miniport|RAS'
  $macs = New-Object System.Collections.Generic.List[string]

  function Add-MacValue {
    param([string]$Value)
    $normalized = Normalize-MacAddress $Value
    if ($normalized -and -not $macs.Contains($normalized)) { [void]$macs.Add($normalized) }
  }

  if (Get-Command Get-NetAdapter -ErrorAction SilentlyContinue) {
    try {
      $netAdapters = Get-NetAdapter -ErrorAction Stop | Where-Object {
        $_.MacAddress -and ($_.HardwareInterface -eq $true -or $_.Virtual -eq $false)
      }
      foreach ($adapter in $netAdapters) {
        $label = (($adapter.Name, $adapter.InterfaceDescription, $adapter.MediaType, $adapter.PhysicalMediaType) -join ' ').Trim()
        if ($label -match $skipPattern) { continue }
        Add-MacValue $adapter.MacAddress
      }
    } catch {
      Write-Log "Get-NetAdapter failed: $($_.Exception.Message)" 'WARN'
    }
  }

  $configs = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapterConfiguration'
  foreach ($cfg in $configs) {
    if (-not $cfg.MACAddress) { continue }
    if ($cfg.Description -and $cfg.Description -match $skipPattern) { continue }
    Add-MacValue $cfg.MACAddress
  }

  $adapters = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapter'
  foreach ($adapter in $adapters) {
    if (-not $adapter.MACAddress) { continue }
    if ($adapter.Description -and $adapter.Description -match $skipPattern) { continue }
    if ($adapter.PhysicalAdapter -ne $true -and ($adapter.PNPDeviceID -and $adapter.PNPDeviceID -match '^ROOT\\')) { continue }
    Add-MacValue $adapter.MACAddress
  }

  foreach ($value in (Get-MacsFromGetmac -SkipPattern $skipPattern)) { Add-MacValue $value }
  foreach ($value in (Get-MacsFromIpconfig -SkipPattern $skipPattern)) { Add-MacValue $value }
  foreach ($value in (Get-MacsFromMsinfo -TimeoutSec $MsinfoTimeoutSec)) { Add-MacValue $value }

  return $macs.ToArray()
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

  if (-not $Path) { return $null }
  $resolvedPath = $Path
  if (-not (Test-Path $resolvedPath)) {
    $cmd = Get-Command $Path -ErrorAction SilentlyContinue
    if ($cmd) {
      $resolvedPath = $cmd.Source
    } else {
      return $null
    }
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $resolvedPath
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

function Invoke-ExternalCommand {
  param(
    [string]$Path,
    [string[]]$Arguments = @(),
    [int]$TimeoutSec = 20,
    [string]$Name = 'External'
  )

  if (-not $Path) {
    return @{ status = 'absent'; output = $null; exitCode = $null }
  }

  $resolvedPath = $Path
  if (-not (Test-Path $resolvedPath)) {
    $cmd = Get-Command $Path -ErrorAction SilentlyContinue
    if ($cmd) {
      $resolvedPath = $cmd.Source
    } else {
      return @{ status = 'absent'; output = $null; exitCode = $null }
    }
  }

  if (-not (Test-Path $resolvedPath)) {
    return @{ status = 'absent'; output = $null; exitCode = $null }
  }

  $args = Normalize-ArgumentList $Arguments

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $resolvedPath
  if ($args -and $args.Count -gt 0) {
    $psi.Arguments = ($args -join ' ')
  }
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "$Name command start failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok'; output = $null; exitCode = $null }
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      Write-Log "$Name command timeout after ${TimeoutSec}s" 'WARN'
      return @{ status = 'timeout'; output = $null; exitCode = $null }
    }
  } catch {
    Write-Log "$Name command wait failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok'; output = $null; exitCode = $null }
  }

  $output = ($proc.StandardOutput.ReadToEnd() + $proc.StandardError.ReadToEnd())
  $status = if ($proc.ExitCode -eq 0) { 'ok' } else { 'nok' }
  if ($status -ne 'ok') {
    Write-Log "$Name command failed with exit code $($proc.ExitCode)" 'WARN'
  }
  return @{ status = $status; output = $output; exitCode = $proc.ExitCode }
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

function Invoke-WinsatScore {
  param(
    [string[]]$Arguments,
    [int]$TimeoutSec
  )

  $cmd = Get-Command winsat -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent'; score = $null } }
  if (-not $script:IsAdmin) {
    Write-Log 'WinSAT skipped (admin required)' 'WARN'
    return @{ status = 'denied'; score = $null }
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
    return @{ status = 'denied'; score = $null }
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      return @{ status = 'timeout'; score = $null }
    }
  } catch {
    Write-Log "WinSAT wait failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok'; score = $null }
  }

  $output = ($proc.StandardOutput.ReadToEnd() + $proc.StandardError.ReadToEnd())
  $status = if ($proc.ExitCode -eq 0) { 'ok' } else { 'nok' }

  $score = $null
  $match = [regex]::Match(
    $output,
    '(?i)(?:D3D|Graphics|GPU|Direct3D)\s*(?:Score|Rating)?\s*[:=]\s*([0-9]+(?:[\\.,][0-9]+)?)'
  )
  if ($match.Success) {
    $raw = $match.Groups[1].Value.Replace(',', '.')
    $score = [double]::Parse($raw, [System.Globalization.CultureInfo]::InvariantCulture)
  }

  return @{ status = $status; score = $score }
}

function Get-DefaultGateway {
  $configs = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapterConfiguration'
  foreach ($cfg in $configs) {
    if (-not $cfg.DefaultIPGateway) { continue }
    foreach ($gw in $cfg.DefaultIPGateway) {
      if ($gw -and $gw -ne '0.0.0.0') {
        return $gw
      }
    }
  }
  return $null
}

function Test-NetworkPing {
  param(
    [string]$Target,
    [int]$Count = 2
  )

  if (-not $Target) { return @{ status = 'not_tested'; target = $null } }

  if (-not (Get-Command Test-Connection -ErrorAction SilentlyContinue)) {
    return @{ status = 'absent'; target = $Target }
  }

  try {
    $ok = Test-Connection -ComputerName $Target -Count $Count -Quiet -ErrorAction Stop
    return @{ status = if ($ok) { 'ok' } else { 'nok' }; target = $Target }
  } catch {
    Write-Log "Ping failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok'; target = $Target }
  }
}

function Get-IperfMbps {
  param(
    [object]$Json,
    [string]$Direction
  )

  if (-not $Json -or -not $Json.end) { return $null }
  $sum = if ($Direction -eq 'download') { $Json.end.sum_received } else { $Json.end.sum_sent }
  if (-not $sum) { $sum = $Json.end.sum }
  if (-not $sum) { return $null }
  if ($sum.bits_per_second -eq $null) { return $null }
  return [math]::Round(($sum.bits_per_second / 1e6), 1)
}

function Invoke-IperfTest {
  param(
    [string]$Path,
    [string]$Server,
    [int]$Port,
    [int]$Seconds,
    [string]$Direction,
    [int]$TimeoutSec,
    [string[]]$ExtraArgs
  )

  if (-not $Path -or -not $Server) {
    return @{ status = 'not_tested'; downMbps = $null; upMbps = $null }
  }

  $baseArgs = @('-J', '-c', $Server, '-p', $Port, '-t', $Seconds)
  $extra = Normalize-ArgumentList $ExtraArgs

  $results = @{ status = 'ok'; downMbps = $null; upMbps = $null }

  function Run-IperfOnce {
    param([string]$Mode)
    $args = @($baseArgs)
    if ($Mode -eq 'download') { $args += '-R' }
    if ($extra -and $extra.Count -gt 0) { $args += $extra }
    $name = "iPerf $Mode"
    $res = Invoke-ExternalCommand -Path $Path -Arguments $args -TimeoutSec $TimeoutSec -Name $name
    if ($res.status -ne 'ok') {
      $results.status = if ($res.status -eq 'absent') { 'not_tested' } else { $res.status }
      return $null
    }
    try {
      return $res.output | ConvertFrom-Json -ErrorAction Stop
    } catch {
      Write-Log "$name JSON parse failed: $($_.Exception.Message)" 'WARN'
      $results.status = 'nok'
      return $null
    }
  }

  if ($Direction -eq 'both') {
    $upJson = Run-IperfOnce -Mode 'upload'
    if ($upJson) { $results.upMbps = Get-IperfMbps -Json $upJson -Direction 'upload' }
    $downJson = Run-IperfOnce -Mode 'download'
    if ($downJson) { $results.downMbps = Get-IperfMbps -Json $downJson -Direction 'download' }
  } elseif ($Direction -eq 'download') {
    $downJson = Run-IperfOnce -Mode 'download'
    if ($downJson) { $results.downMbps = Get-IperfMbps -Json $downJson -Direction 'download' }
  } else {
    $upJson = Run-IperfOnce -Mode 'upload'
    if ($upJson) { $results.upMbps = Get-IperfMbps -Json $upJson -Direction 'upload' }
  }

  if ($results.downMbps -eq $null -and $results.upMbps -eq $null -and $results.status -eq 'ok') {
    $results.status = 'nok'
  }

  return $results
}

function Invoke-FsCheck {
  param(
    [string]$DriveLetter,
    [int]$TimeoutSec
  )

  $cmd = Get-Command chkdsk -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent' } }
  if (-not $script:IsAdmin) {
    Write-Log 'chkdsk skipped (admin required)' 'WARN'
    return @{ status = 'denied' }
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $cmd.Source
  $psi.Arguments = "$DriveLetter /scan"
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "chkdsk start failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok' }
  }

  try {
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      return @{ status = 'timeout' }
    }
  } catch {
    Write-Log "chkdsk wait failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'nok' }
  }

  $output = ($proc.StandardOutput.ReadToEnd() + $proc.StandardError.ReadToEnd())
  if ($proc.ExitCode -eq 0) {
    return @{ status = 'ok'; output = $output }
  }
  return @{ status = 'nok'; output = $output }
}

function Get-ThermalInfo {
  $zones = Get-CimInstanceSafe -Namespace 'root\\wmi' -ClassName 'MSAcpi_ThermalZoneTemperature'
  if (-not $zones -or $zones.Count -eq 0) {
    return @{ status = 'absent'; maxC = $null; zones = @() }
  }

  $items = @()
  foreach ($zone in $zones) {
    if ($zone.CurrentTemperature -eq $null) { continue }
    $tempC = [math]::Round(($zone.CurrentTemperature / 10) - 273.15, 1)
    $items += [ordered]@{
      name = $zone.InstanceName
      temperatureC = $tempC
    }
  }

  if ($items.Count -eq 0) {
    return @{ status = 'absent'; maxC = $null; zones = @() }
  }

  $maxC = ($items | Measure-Object -Property temperatureC -Maximum).Maximum
  return @{ status = 'ok'; maxC = $maxC; zones = $items }
}

function Invoke-MemoryDiagnostic {
  param([string]$Mode)

  if ($Mode -eq 'none') { return 'not_tested' }
  $cmd = Get-Command mdsched -ErrorAction SilentlyContinue
  if (-not $cmd) { return 'absent' }
  if (-not $script:IsAdmin) {
    Write-Log 'Memory diagnostic scheduling skipped (admin required)' 'WARN'
    return 'denied'
  }

  try {
    & $cmd.Source /f | Out-Null
    return 'scheduled'
  } catch {
    Write-Log "Memory diagnostic schedule failed: $($_.Exception.Message)" 'WARN'
    return 'nok'
  }
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
$macAddresses = Get-AllMacs
$macAddressesLog = if ($macAddresses) { $macAddresses -join ', ' } else { $null }
if ($macAddressesLog) { Write-Log ("MAC list: {0}" -f $macAddressesLog) }
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
$cameraTestPathValue = $CameraTestPath
if (-not $cameraTestPathValue) {
  $defaultCameraExe = Join-Path $PSScriptRoot 'camera_capture.exe'
  if (Test-Path $defaultCameraExe) {
    $cameraTestPathValue = $defaultCameraExe
  }
}
if ($cameraPresence -eq 'absent' -or $cameraPresence -eq 'nok') {
  $cameraStatus = $cameraPresence
} else {
  if ($cameraTestPathValue -and -not (Test-Path $cameraTestPathValue)) {
    Write-Log "Camera test binary not found: $cameraTestPathValue" 'WARN'
  }
  $cameraTestStatus = Invoke-ExternalTest `
    -Path $cameraTestPathValue `
    -Arguments (Normalize-ArgumentList $CameraTestArguments) `
    -TimeoutSec $CameraTestTimeoutSec `
    -Name 'Camera'
  if ($cameraTestStatus) {
    $cameraStatus = $cameraTestStatus
  } else {
    $cameraStatus = 'not_tested'
  }
}
Write-Log "Camera presence=$cameraPresence TestPath=$cameraTestPathValue TestStatus=$cameraTestStatus Final=$cameraStatus"

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

$cpuTest = $null
$gpuTest = $null
if ($testLoops -gt 0) {
  $cpuTest = Run-WinsatLoop -Arguments @('cpu') -Loops $testLoops -TimeoutSec $CpuTestTimeoutSec
  $gpuTest = Invoke-WinsatScore -Arguments @('d3d') -TimeoutSec $GpuTestTimeoutSec
}

$cpuExternalStatus = $null
if ($CpuTestPath) {
  $cpuExternalStatus = Invoke-ExternalTest -Path $CpuTestPath -Arguments (Normalize-ArgumentList $CpuTestArguments) -TimeoutSec $CpuTestTimeoutSec -Name 'CPU'
}

$gpuExternalStatus = $null
if ($GpuTestPath) {
  $gpuExternalStatus = Invoke-ExternalTest -Path $GpuTestPath -Arguments (Normalize-ArgumentList $GpuTestArguments) -TimeoutSec $GpuTestTimeoutSec -Name 'GPU'
}

$memDiagStatus = Invoke-MemoryDiagnostic -Mode $MemDiagMode

$networkPingTargetValue = if ($NetworkPingTarget) { $NetworkPingTarget } else { Get-DefaultGateway }
$networkPingResult = Test-NetworkPing -Target $networkPingTargetValue -Count $NetworkPingCount
$iperfResult = Invoke-IperfTest `
  -Path $NetworkTestPath `
  -Server $NetworkTestServer `
  -Port $NetworkTestPort `
  -Seconds $NetworkTestSeconds `
  -Direction $NetworkTestDirection `
  -TimeoutSec $NetworkTestTimeoutSec `
  -ExtraArgs $NetworkTestExtraArgs

$runFsCheck = $FsCheckMode -eq 'scan' -or ($FsCheckMode -eq 'auto' -and $TestMode -eq 'stress')
$fsCheckResult = if ($runFsCheck) { Invoke-FsCheck -DriveLetter $driveLetter -TimeoutSec $FsCheckTimeoutSec } else { @{ status = 'not_tested' } }

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
$gpuStatus = Get-StatusFromDevices $video
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
$thermalInfo = Get-ThermalInfo

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
if ($cpuTest) {
  $tests.cpuTest = $cpuTest.status
  if ($cpuTest.mbps -ne $null) { $tests.cpuMBps = $cpuTest.mbps }
}
if ($gpuTest) {
  $tests.gpuTest = $gpuTest.status
  if ($gpuTest.score -ne $null) { $tests.gpuScore = $gpuTest.score }
}
if ($cpuExternalStatus) { $tests.cpuStress = $cpuExternalStatus }
if ($gpuExternalStatus) { $tests.gpuStress = $gpuExternalStatus }
if ($networkPingResult) {
  $tests.networkPing = $networkPingResult.status
  if ($networkPingResult.target) { $tests.networkPingTarget = $networkPingResult.target }
}
if ($iperfResult) {
  $tests.network = $iperfResult.status
  if ($iperfResult.downMbps -ne $null) { $tests.networkDownMbps = $iperfResult.downMbps }
  if ($iperfResult.upMbps -ne $null) { $tests.networkUpMbps = $iperfResult.upMbps }
}
if ($fsCheckResult) { $tests.fsCheck = $fsCheckResult.status }
if ($memDiagStatus) { $tests.memDiag = $memDiagStatus }

$diag.diagnosticsPerformed = $tests.Keys.Count

$stopwatch.Stop()
$diag.completedAt = (Get-Date).ToString('o')
$diag.durationSec = [int]$stopwatch.Elapsed.TotalSeconds

$payload = [ordered]@{}
$technicianValue = if ($Technician) { $Technician.Trim() } else { $null }
if ($hostname) { $payload.hostname = $hostname }
if ($macAddress) { $payload.macAddress = $macAddress }
if ($macAddresses -and $macAddresses.Count -gt 0) { $payload.macAddresses = $macAddresses }
if ($serialNumber) { $payload.serialNumber = $serialNumber }
if ($categoryValue) { $payload.category = $categoryValue }
if ($technicianValue) { $payload.technician = $technicianValue }
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
if ($thermalInfo) { $payload.thermal = $thermalInfo }
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
if ($tests.cpuTest) { $components.cpuTest = $tests.cpuTest }
if ($tests.gpuTest) { $components.gpuTest = $tests.gpuTest }
if ($tests.cpuStress) { $components.cpuStress = $tests.cpuStress }
if ($tests.gpuStress) { $components.gpuStress = $tests.gpuStress }
if ($tests.network) { $components.networkTest = $tests.network }
if ($tests.networkPing) { $components.networkPing = $tests.networkPing }
if ($tests.fsCheck) { $components.fsCheck = $tests.fsCheck }
if ($tests.memDiag) { $components.memDiag = $tests.memDiag }
if ($gpuStatus) { $components.gpu = $gpuStatus }
if ($thermalInfo -and $thermalInfo.status) { $components.thermal = $thermalInfo.status }

if ($components.Count -gt 0) {
  $payload.components = $components
}

$json = $payload | ConvertTo-Json -Depth 10

Write-Log "Payload size=$($json.Length)"

$ingestOk = $false
try {
  $response = Invoke-RestMethod -Uri $ApiUrl -Method Post -ContentType 'application/json' -Body $json -TimeoutSec $TimeoutSec
  Write-Log 'Ingest OK'
  $ingestOk = $true
  Write-Output $response
} catch {
  Write-Log $_.Exception.Message 'ERROR'
  if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
    Write-Log $_.ErrorDetails.Message 'ERROR'
  }
  Write-Error $_
  exit 1
}

if ($ingestOk -and -not $SkipKeyboardCapture -and $categoryValue -eq 'laptop') {
  $keyboardScript = Resolve-KeyboardCapturePath -Value $KeyboardCapturePath
  $layoutValue = if ($KeyboardCaptureLayout) { $KeyboardCaptureLayout } else { $null }
  $layoutConfigValue = if ($KeyboardCaptureLayoutConfig) { $KeyboardCaptureLayoutConfig } else { $null }
  $configDirValue = if ($KeyboardCaptureConfigDir) { $KeyboardCaptureConfigDir } else { $null }
  $logPathValue = if ($KeyboardCaptureLogPath) { $KeyboardCaptureLogPath } else { $null }
  Write-Log 'Launching keyboard capture for laptop'
  Start-KeyboardCapture `
    -ScriptPath $keyboardScript `
    -LogPath $logPathValue `
    -ConfigDir $configDirValue `
    -Layout $layoutValue `
    -LayoutConfig $layoutConfigValue `
    -BlockInput:$KeyboardCaptureBlockInput
}
