[CmdletBinding()]
param(
  [string]$ApiUrl = $env:MDT_API_URL,
  [ValidateSet('auto', 'laptop', 'desktop', 'unknown')][string]$Category = 'auto',
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
  [string]$WinSatDataStorePath = $env:MDT_WINSAT_DATASTORE,
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
  [switch]$SkipWinSatDataStore,
  [switch]$SkipElevation,
  [switch]$SkipTlsValidation
)

$scriptPath = Join-Path $PSScriptRoot 'mdt-report.ps1'
$params = @{
  ApiUrl = $ApiUrl
  Category = $Category
  TestMode = 'stress'
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
  WinSatDataStorePath = $WinSatDataStorePath
  MacPreference = $MacPreference
  LogPath = $LogPath
  Technician = $Technician
  KeyboardCapturePath = $KeyboardCapturePath
  KeyboardCaptureLogPath = $KeyboardCaptureLogPath
  KeyboardCaptureConfigDir = $KeyboardCaptureConfigDir
  KeyboardCaptureLayout = $KeyboardCaptureLayout
  KeyboardCaptureLayoutConfig = $KeyboardCaptureLayoutConfig
  KeyboardCaptureBlockInput = $KeyboardCaptureBlockInput
  SkipKeyboardCapture = $SkipKeyboardCapture
  SkipWinSatDataStore = $SkipWinSatDataStore
  SkipElevation = $SkipElevation
  SkipStressScript = $true
}
if ($SkipTlsValidation) { $params.SkipTlsValidation = $true }

& $scriptPath @params
