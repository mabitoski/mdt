[CmdletBinding()]
param(
  [string]$ApiUrl = $env:MDT_API_URL,
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

$scriptPath = Join-Path $PSScriptRoot 'mdt-report.ps1'
$params = @{
  ApiUrl = $ApiUrl
  Category = 'desktop'
  TestMode = $TestMode
  TimeoutSec = $TimeoutSec
  DiskTestTimeoutSec = $DiskTestTimeoutSec
  MemTestTimeoutSec = $MemTestTimeoutSec
  StressLoops = $StressLoops
  CameraTestPath = $CameraTestPath
  CameraTestTimeoutSec = $CameraTestTimeoutSec
  MsinfoTimeoutSec = $MsinfoTimeoutSec
  MacPreference = $MacPreference
  LogPath = $LogPath
}
if ($SkipTlsValidation) { $params.SkipTlsValidation = $true }

& $scriptPath @params
