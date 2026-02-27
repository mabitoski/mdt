[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)][string]$BundlePath,
  [string]$ApiUrl = 'http://hydra-dev.local/api/ingest',
  [string]$Technician,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
  [switch]$DryRun,
  [string]$OutputPayloadPath,
  [switch]$SkipTlsValidation
)

$scriptVersion = '1.0.0'

function Write-JsonFile {
  param(
    [string]$Path,
    [object]$Object
  )
  $json = $Object | ConvertTo-Json -Depth 10
  Set-Content -Path $Path -Value $json -Encoding UTF8
}

function Get-WinSatNote {
  param([double]$Score)
  if ($Score -lt 3.0) { return 'Horrible' }
  if ($Score -lt 4.5) { return 'Mauvais' }
  if ($Score -lt 6.0) { return 'Moyen' }
  if ($Score -lt 7.5) { return 'Bon' }
  return 'Excellent'
}

function Get-WinSatStatus {
  param([double]$Score)
  if ($Score -lt 4.5) { return 'nok' }
  return 'ok'
}

function Invoke-JsonPost {
  param(
    [string]$Url,
    [string]$Json
  )
  return Invoke-RestMethod -Uri $Url -Method Post -ContentType 'application/json; charset=utf-8' -Body $Json
}

if ($SkipTlsValidation) {
  try {
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
  } catch { }
}

if (-not (Test-Path -Path $BundlePath)) {
  throw "Bundle path not found: $BundlePath"
}

$bundleRoot = (Resolve-Path -Path $BundlePath).Path.TrimEnd('\', '/')
$manifestPath = Join-Path $bundleRoot 'manifest.json'
$summaryPath = Join-Path $bundleRoot 'summary.json'
if (-not (Test-Path -Path $summaryPath)) {
  throw "summary.json not found in bundle: $BundlePath"
}

$manifest = if (Test-Path -Path $manifestPath) { Get-Content -Path $manifestPath -Raw | ConvertFrom-Json } else { $null }
$summary = Get-Content -Path $summaryPath -Raw | ConvertFrom-Json

$reportId = if ($manifest -and $manifest.reportId) { $manifest.reportId } else { $summary.reportId }
if (-not $reportId) { $reportId = [guid]::NewGuid().ToString() }

if ($Technician) { $summary.technician = $Technician }
if ($Category -ne 'auto') { $summary.category = $Category }

$winsatPath = Join-Path $bundleRoot 'winsat\winsat.json'
$winsat = if (Test-Path -Path $winsatPath) { Get-Content -Path $winsatPath -Raw | ConvertFrom-Json } else { $null }

$tests = [ordered]@{}
if ($winsat -and $winsat.winSPR) {
  $cpuScore = $winsat.winSPR.CpuScore
  $memScore = $winsat.winSPR.MemoryScore
  $gpuScore = if ($winsat.winSPR.GamingScore) { $winsat.winSPR.GamingScore } else { $winsat.winSPR.GraphicsScore }
  $diskScore = $winsat.winSPR.DiskScore

  if ($cpuScore) {
    $tests.cpuTest = Get-WinSatStatus -Score $cpuScore
    $tests.cpuNote = Get-WinSatNote -Score $cpuScore
    $tests.cpuScore = $cpuScore
  }
  if ($memScore) {
    $tests.ramTest = Get-WinSatStatus -Score $memScore
    $tests.ramNote = Get-WinSatNote -Score $memScore
    $tests.ramScore = $memScore
  }
  if ($gpuScore) {
    $tests.gpuTest = Get-WinSatStatus -Score $gpuScore
    $tests.gpuNote = Get-WinSatNote -Score $gpuScore
    $tests.gpuScore = $gpuScore
  }
  if ($diskScore) {
    $tests.diskRead = Get-WinSatStatus -Score $diskScore
    $tests.diskWrite = Get-WinSatStatus -Score $diskScore
    $tests.diskScore = $diskScore
  }
}

$components = @{}
if ($summary.cameraStatus) { $components.camera = $summary.cameraStatus }
if ($summary.usbStatus) { $components.usb = $summary.usbStatus }
if ($summary.keyboardStatus) { $components.keyboard = $summary.keyboardStatus }
if ($summary.padStatus) { $components.pad = $summary.padStatus }
if ($summary.badgeReaderStatus) { $components.badgeReader = $summary.badgeReaderStatus }
if ($tests.cpuTest) { $components.cpuTest = $tests.cpuTest }
if ($tests.ramTest) { $components.ramTest = $tests.ramTest }
if ($tests.gpuTest) { $components.gpuTest = $tests.gpuTest }
if ($tests.diskRead) { $components.diskReadTest = $tests.diskRead }
if ($tests.diskWrite) { $components.diskWriteTest = $tests.diskWrite }

$rawFiles = @()
Push-Location -LiteralPath $bundleRoot
try {
  Get-ChildItem -Path $bundleRoot -File -Recurse | ForEach-Object {
    $relative = $null
    try {
      $relative = (Resolve-Path -Relative -Path $_.FullName)
    } catch {
      $relative = $_.Name
    }
    if ($relative) {
      $relative = $relative.TrimStart('.', '\', '/')
      $rawFiles += $relative
    }
  }
} finally {
  Pop-Location
}

$payload = [ordered]@{
  reportId = $reportId
  hostname = $summary.hostname
  macAddress = $summary.macAddress
  macAddresses = $summary.macAddresses
  serialNumber = $summary.serialNumber
  category = $summary.category
  technician = $summary.technician
  vendor = $summary.vendor
  model = $summary.model
  osVersion = $summary.osVersion
  ramMb = $summary.ramMb
  ramSlotsTotal = $summary.ramSlotsTotal
  ramSlotsFree = $summary.ramSlotsFree
  batteryHealth = $summary.batteryHealth
  disks = $summary.disks
  volumes = $summary.volumes
  diag = [ordered]@{
    reportId = $reportId
    scriptVersion = $scriptVersion
    source = 'offline-bundle'
    completedAt = (Get-Date).ToUniversalTime().ToString('o')
  }
  device = [ordered]@{
    manufacturer = $summary.vendor
    model = $summary.model
    motherboard = if ($summary.baseboard) { $summary.baseboard.product } else { $null }
    batteryCapacity = $summary.battery
  }
  bios = $summary.bios
  windows = $summary.windows
  cpu = $summary.cpu
  gpu = $summary.gpu
  memory = [ordered]@{
    totalGb = if ($summary.ramMb) { [math]::Round($summary.ramMb / 1024, 1) } else { $null }
  }
  rawArtifacts = [ordered]@{
    reportId = $reportId
    bundle = (Split-Path -Leaf $bundleRoot)
    files = $rawFiles
  }
}

if ($tests.Count -gt 0) { $payload.tests = $tests }
if ($winsat) { $payload.winsat = $winsat }
if ($components.Count -gt 0) { $payload.components = $components }

$json = $payload | ConvertTo-Json -Depth 10

if ($OutputPayloadPath) {
  Write-JsonFile -Path $OutputPayloadPath -Object $payload
}

if ($DryRun) {
  Write-Output "Dry run: payload prepared for $ApiUrl"
  return
}

if (-not $ApiUrl) {
  throw 'ApiUrl is required to ingest.'
}

$response = Invoke-JsonPost -Url $ApiUrl -Json $json
Write-Output $response
