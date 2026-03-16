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
  [int]$DxDiagTimeoutSec = 300,
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
  [string]$CameraTestOutputDir = $env:MDT_CAMERA_OUTDIR,
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
  [int]$KeyboardCaptureTimeoutSec = 600,
  [switch]$SkipKeyboardCapture,
  [switch]$SkipWinSatDataStore,
  [switch]$SkipGpuAssessment,
  [switch]$SkipStressScript,
  [switch]$SkipElevation,
  [switch]$SkipTlsValidation,
  [switch]$FactoryReset,
  [string]$FactoryResetConfirm,
  [switch]$SkipFactoryResetPrompt,
  [string]$ArtifactRoot = $env:MDT_ARTIFACT_ROOT,
  [string]$ObjectStorageEndpoint = $env:MDT_OBJECT_STORAGE_ENDPOINT,
  [string]$ObjectStorageBucket = $env:MDT_OBJECT_STORAGE_BUCKET,
  [string]$ObjectStorageAccessKey = $env:MDT_OBJECT_STORAGE_ACCESS_KEY,
  [string]$ObjectStorageSecretKey = $env:MDT_OBJECT_STORAGE_SECRET_KEY,
  [string]$ObjectStoragePrefix = $env:MDT_OBJECT_STORAGE_PREFIX,
  [string]$ReportTag = $env:MDT_REPORT_TAG,
  [string]$ReportTagId = $env:MDT_REPORT_TAG_ID,
  [string]$ObjectStorageMcPath = $env:MDT_OBJECT_STORAGE_MC_PATH,
  [switch]$SkipRawUpload,
  [switch]$FailTaskSequenceOnError
)

$scriptVersion = '1.7.8'
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

if (-not $ApiUrl) {
  $ApiUrl = 'http://10.1.10.27:3000/api/ingest'
}

if (-not $NetworkPingTarget) {
  $NetworkPingTarget = '1.1.1.1'
}

if (-not $LogPath) {
  $baseLogDir = $null
  if ($env:_SMSTSLogPath -and (Test-Path -Path $env:_SMSTSLogPath)) {
    $baseLogDir = $env:_SMSTSLogPath
  } elseif ($env:TEMP) {
    $baseLogDir = $env:TEMP
  } elseif ($env:WINDIR) {
    $baseLogDir = Join-Path $env:WINDIR 'Temp'
  } elseif ($PSScriptRoot) {
    $baseLogDir = $PSScriptRoot
  } else {
    $baseLogDir = (Get-Location).Path
  }
  $LogPath = Join-Path $baseLogDir 'mdt-report.log'
}

try {
  $logDir = Split-Path $LogPath -Parent
  if ($logDir -and -not (Test-Path -Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
  }
  New-Item -Path $LogPath -ItemType File -Force | Out-Null
} catch {
}

if (-not $Technician) { $Technician = 'Antoine' }
if (-not $PSBoundParameters.ContainsKey('SkipRawUpload') -and $env:MDT_SKIP_RAW_UPLOAD -eq '1') {
  $SkipRawUpload = $true
}
if (-not $PSBoundParameters.ContainsKey('FailTaskSequenceOnError') -and $env:MDT_FAIL_TS_ON_REPORT_ERROR -eq '1') {
  $FailTaskSequenceOnError = $true
}
if (-not $PSBoundParameters.ContainsKey('SkipGpuAssessment') -and $env:MDT_SKIP_GPU_ASSESSMENT -eq '1') {
  $SkipGpuAssessment = $true
}

if (-not $ObjectStorageEndpoint) { $ObjectStorageEndpoint = 'http://10.1.10.28:9000' }
if (-not $ObjectStorageBucket) { $ObjectStorageBucket = 'alcyone-archive' }
if (-not $ObjectStorageAccessKey) { $ObjectStorageAccessKey = 'codexminio' }
if (-not $ObjectStorageSecretKey) { $ObjectStorageSecretKey = 'semngIYo36sZq27tixYVXeFF' }
if (-not $ObjectStoragePrefix) { $ObjectStoragePrefix = 'run' }
if (-not $ReportTag) { $ReportTag = 'En cours' }

if (-not $WinSatDataStorePath) {
  try {
    if ($env:SystemRoot) {
      $WinSatDataStorePath = Join-Path $env:SystemRoot 'Performance\\WinSAT\\DataStore'
    }
  } catch { }
}

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$timestamp][$Level] $Message"
  try { Write-Host $line } catch { }
  try {
    Add-Content -Path $LogPath -Value $line
  } catch {
  }
}

$script:ProgressEnabled = $true
try { $script:ProgressEnabled = [Environment]::UserInteractive } catch { }
$script:ProgressTotal = 8
$script:ProgressStep = 0

function Step-Progress {
  param([string]$Status)

  if (-not $script:ProgressEnabled) { return }
  $script:ProgressStep++
  $percent = 0
  if ($script:ProgressTotal -gt 0) {
    $percent = [math]::Min(99, [math]::Round(($script:ProgressStep / $script:ProgressTotal) * 100))
  }
  Write-Progress -Activity 'MDT report' -Status $Status -PercentComplete $percent
}

function Complete-Progress {
  if (-not $script:ProgressEnabled) { return }
  Write-Progress -Activity 'MDT report' -Completed
}

function Initialize-WindowApi {
  try {
    Add-Type -Namespace MDT -Name WindowApi -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("user32.dll")]
public static extern bool SetForegroundWindow(System.IntPtr hWnd);
[System.Runtime.InteropServices.DllImport("user32.dll")]
public static extern bool ShowWindowAsync(System.IntPtr hWnd, int nCmdShow);
[System.Runtime.InteropServices.DllImport("user32.dll")]
public static extern System.IntPtr GetForegroundWindow();
"@ -ErrorAction Stop | Out-Null
  } catch { }
}

function MaximizeForegroundWindow {
  try {
    $handle = [MDT.WindowApi]::GetForegroundWindow()
    if ($handle -ne [IntPtr]::Zero) {
      [MDT.WindowApi]::ShowWindowAsync($handle, 3) | Out-Null
    }
  } catch { }
}

function Try-ActivateWindow {
  param(
    [int]$ProcessId,
    [string[]]$Titles,
    [int]$RetryCount = 6,
    [int]$DelayMs = 400
  )

  $shell = $null
  try { $shell = New-Object -ComObject WScript.Shell } catch { }
  for ($i = 0; $i -lt $RetryCount; $i++) {
    if ($ProcessId) {
      try {
        $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        if ($proc -and $proc.MainWindowHandle -and $proc.MainWindowHandle -ne [IntPtr]::Zero) {
          try { [MDT.WindowApi]::ShowWindowAsync($proc.MainWindowHandle, 5) | Out-Null } catch { }
          try { [MDT.WindowApi]::SetForegroundWindow($proc.MainWindowHandle) | Out-Null } catch { }
          return $true
        }
      } catch { }
    }
    if ($shell -and $Titles) {
      foreach ($title in $Titles) {
        if (-not $title) { continue }
        try {
          if ($shell.AppActivate($title)) {
            return $true
          }
        } catch { }
      }
    }
    Start-Sleep -Milliseconds $DelayMs
  }
  return $false
}

function Send-KeysSafe {
  param(
    [object]$Shell,
    [string]$Keys,
    [string]$Label
  )

  if (-not $Shell -or [string]::IsNullOrWhiteSpace($Keys)) {
    return $false
  }
  try {
    $Shell.SendKeys($Keys)
    return $true
  } catch {
    $name = if ($Label) { $Label } else { $Keys }
    Write-Log "SendKeys failed ($name): $($_.Exception.Message)" 'WARN'
    return $false
  }
}

function Send-ResetUiKeys {
  param(
    [int]$ProcessId,
    [string]$WindowTitle,
    [int]$DelaySec = 20
  )

  $isInteractive = $true
  try { $isInteractive = [Environment]::UserInteractive } catch { }
  if (-not $isInteractive) {
    Write-Log 'Factory reset keystrokes skipped (non-interactive session).' 'WARN'
    return
  }

  try {
    $shell = New-Object -ComObject WScript.Shell
    if ($DelaySec -gt 0) {
      Write-Log "Waiting ${DelaySec}s before sending reset keys." 'WARN'
      Start-Sleep -Seconds $DelaySec
    }
    try {
      Add-Type -Namespace MDT -Name ConsoleNative -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("kernel32.dll")]
public static extern System.IntPtr GetConsoleWindow();
[System.Runtime.InteropServices.DllImport("user32.dll")]
public static extern bool ShowWindow(System.IntPtr hWnd, int nCmdShow);
"@ -ErrorAction Stop | Out-Null
      $consoleHandle = [MDT.ConsoleNative]::GetConsoleWindow()
      if ($consoleHandle -ne [IntPtr]::Zero) {
        [MDT.ConsoleNative]::ShowWindow($consoleHandle, 6) | Out-Null
      }
    } catch { }

    $activated = $false
    $targets = @()
    if ($ProcessId) { $targets += $ProcessId }
    if ($WindowTitle) { $targets += $WindowTitle }
    $targets += @('Paramètres', 'Settings', 'Réinitialiser ce PC', 'Reset this PC')
    foreach ($target in $targets) {
      try {
        if ($shell.AppActivate($target)) {
          $activated = $true
          break
        }
      } catch { }
    }
    if (-not $activated) {
      Write-Log 'Factory reset window not activated; sending keys anyway.' 'WARN'
    }

    Start-Sleep -Milliseconds 800
    for ($i = 0; $i -lt 5; $i++) {
      $shell.SendKeys('{TAB}')
      Start-Sleep -Milliseconds 150
    }
    $shell.SendKeys('{ENTER}')
    Write-Log 'Factory reset initial keystrokes sent (manual continuation expected).' 'WARN'
  } catch {
    Write-Log "Factory reset keystrokes failed: $($_.Exception.Message)" 'WARN'
  }
}

function Set-ConsoleFullscreen {
  if (-not $script:IsInteractive) { return }
  try {
    Add-Type -Namespace MDT -Name ConsoleNative -MemberDefinition @"
[System.Runtime.InteropServices.DllImport("kernel32.dll")]
public static extern System.IntPtr GetConsoleWindow();
[System.Runtime.InteropServices.DllImport("user32.dll")]
public static extern bool ShowWindow(System.IntPtr hWnd, int nCmdShow);
"@ -ErrorAction Stop | Out-Null
  } catch { }
  try {
    $handle = [MDT.ConsoleNative]::GetConsoleWindow()
    if ($handle -ne [IntPtr]::Zero) {
      [MDT.ConsoleNative]::ShowWindow($handle, 3) | Out-Null
    }
  } catch { }
  try {
    $raw = $Host.UI.RawUI
    if ($raw -and $raw.MaxWindowSize.Width -gt 0 -and $raw.MaxWindowSize.Height -gt 0) {
      $raw.WindowSize = $raw.MaxWindowSize
    }
  } catch { }
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

function Get-TestLabel {
  param([string]$Key)

  switch ($Key) {
    'diskRead' { return 'Lecture disque' }
    'diskWrite' { return 'Ecriture disque' }
    'ramTest' { return 'RAM (WinSAT)' }
    'cpuTest' { return 'CPU (WinSAT)' }
    'gpuTest' { return 'GPU (WinSAT)' }
    'cpuStress' { return 'CPU (stress)' }
    'gpuStress' { return 'GPU (stress)' }
    'network' { return 'Reseau (iperf)' }
    'networkPing' { return 'Ping' }
    'fsCheck' { return 'Check disque' }
    'memDiag' { return 'Diag memoire' }
    'cameraStatus' { return 'Camera' }
    'usbStatus' { return 'USB' }
    'keyboardStatus' { return 'Clavier' }
    'padStatus' { return 'Pave tactile' }
    'badgeReaderStatus' { return 'Badge' }
    'gpuStatus' { return 'GPU (etat)' }
    'thermalStatus' { return 'Thermique' }
    default { return $Key }
  }
}

function Get-NonOkTests {
  param(
    [hashtable]$Tests,
    [hashtable]$Extras
  )

  if (-not $Tests -and -not $Extras) { return @() }
  $combined = @{}
  if ($Tests) {
    foreach ($entry in $Tests.GetEnumerator()) { $combined[$entry.Key] = $entry.Value }
  }
  if ($Extras) {
    foreach ($entry in $Extras.GetEnumerator()) { $combined[$entry.Key] = $entry.Value }
  }

  $statusKeys = @(
    'diskRead',
    'diskWrite',
    'ramTest',
    'cpuTest',
    'gpuTest',
    'cpuStress',
    'gpuStress',
    'network',
    'networkPing',
    'fsCheck',
    'memDiag',
    'cameraStatus',
    'usbStatus',
    'keyboardStatus',
    'padStatus',
    'badgeReaderStatus',
    'gpuStatus',
    'thermalStatus'
  )
  $results = @()
  foreach ($key in $statusKeys) {
    if (-not $combined.ContainsKey($key)) { continue }
    $value = $combined[$key]
    if (-not ($value -is [string])) { continue }
    if ([string]::IsNullOrWhiteSpace($value)) { continue }
    $status = $value.Trim().ToLowerInvariant()
    if ($status -eq 'ok' -or $status -eq 'not_tested') { continue }
    $results += [ordered]@{
      key = $key
      label = Get-TestLabel -Key $key
      status = $status
    }
  }
  return $results
}

function Start-TestCommentCapture {
  param(
    [object[]]$Tests,
    [string]$RunDir,
    [string]$ApiUrl,
    [string]$ReportId,
    [string]$CommentUser,
    [string]$CommentPassword,
    [string]$LogPath,
    [int]$TimeoutSec = 120
  )

  if (-not $Tests -or $Tests.Count -eq 0) { return $false }
  if (-not $RunDir) { return $false }
  if (-not $ApiUrl) { return $false }
  if (-not $ReportId) { return $false }

  $testsPath = Join-Path $RunDir 'tests-nok.json'
  $scriptPath = Join-Path $RunDir 'mdt-comment-capture.ps1'
  try {
    $Tests | ConvertTo-Json -Depth 4 | Set-Content -Path $testsPath -Encoding UTF8
  } catch {
    Write-Log "Failed to write tests list for comment capture: $($_.Exception.Message)" 'WARN'
    return $false
  }

  $commentScript = @'
param(
  [string]$ApiUrl,
  [string]$ReportId,
  [string]$TestsPath,
  [string]$CommentUser,
  [string]$CommentPassword,
  [string]$LogPath,
  [int]$TimeoutSec = 120
)

function Write-CommentLog {
  param([string]$Message)
  if (-not $TestsPath) { return }
  $dir = Split-Path $TestsPath -Parent
  if (-not $dir) { return }
  $logPath = Join-Path $dir 'mdt-comment.log'
  $line = "[{0}] {1}" -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'), $Message
  try { Add-Content -Path $logPath -Value $line } catch { }
  if ($LogPath) {
    try { Add-Content -Path $LogPath -Value $line } catch { }
  }
}

function Get-ApiBaseUrl {
  param([string]$Url)
  if (-not $Url) { return $null }
  try {
    $uri = [Uri]$Url
    $port = if ($uri.IsDefaultPort) { '' } else { ':' + $uri.Port }
    return "$($uri.Scheme)://$($uri.Host)$port"
  } catch {
    return ($Url -replace '/api/.*$', '')
  }
}

if (-not (Test-Path $TestsPath)) { return }
$tests = $null
try { $tests = Get-Content -Path $TestsPath -Raw | ConvertFrom-Json } catch { $tests = $null }
if (-not $tests) { return }
if (-not $ApiUrl -or -not $ReportId) { Write-CommentLog 'Missing ApiUrl or ReportId'; return }
if (-not $CommentUser -or -not $CommentPassword) { Write-CommentLog 'Missing comment credentials'; return }

$hasUi = $true
try {
  Add-Type -AssemblyName System.Windows.Forms
  Add-Type -AssemblyName System.Drawing
} catch {
  $hasUi = $false
}
if (-not $hasUi) { return }

$form = New-Object System.Windows.Forms.Form
$form.Text = 'MDT report - Tests NOK'
$form.StartPosition = 'CenterScreen'
$form.TopMost = $true
$form.Size = New-Object System.Drawing.Size(720, 520)

$panel = New-Object System.Windows.Forms.TableLayoutPanel
$panel.Dock = 'Fill'
$panel.AutoScroll = $true
$panel.ColumnCount = 2
$panel.RowCount = 4
$panel.ColumnStyles.Add((New-Object System.Windows.Forms.ColumnStyle([System.Windows.Forms.SizeType]::Percent, 35)))
$panel.ColumnStyles.Add((New-Object System.Windows.Forms.ColumnStyle([System.Windows.Forms.SizeType]::Percent, 65)))

$timerLabel = New-Object System.Windows.Forms.Label
$timerLabel.Text = "Temps restant: ${TimeoutSec}s"
$timerLabel.AutoSize = $true
$timerLabel.Font = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$panel.Controls.Add($timerLabel, 0, 0)
$panel.SetColumnSpan($timerLabel, 2)

$summaryLabel = New-Object System.Windows.Forms.Label
$summaryText = ($tests | ForEach-Object { "$($_.label) [$($_.status)]" }) -join ', '
$summaryLabel.Text = "Tests NOK: $summaryText"
$summaryLabel.AutoSize = $true
$summaryLabel.Margin = New-Object System.Windows.Forms.Padding(6, 8, 6, 8)
$panel.Controls.Add($summaryLabel, 0, 1)
$panel.SetColumnSpan($summaryLabel, 2)

$commentLabel = New-Object System.Windows.Forms.Label
$commentLabel.Text = 'Commentaire'
$commentLabel.AutoSize = $true
$commentLabel.Margin = New-Object System.Windows.Forms.Padding(6, 8, 6, 8)
$panel.Controls.Add($commentLabel, 0, 2)

$commentBox = New-Object System.Windows.Forms.TextBox
$commentBox.Multiline = $true
$commentBox.Height = 80
$commentBox.Margin = New-Object System.Windows.Forms.Padding(6, 6, 6, 6)
$panel.Controls.Add($commentBox, 1, 2)

$btn = New-Object System.Windows.Forms.Button
$btn.Text = 'Envoyer'
$btn.Height = 32
$btn.Width = 120
$btn.Margin = New-Object System.Windows.Forms.Padding(6, 10, 6, 10)
$btn.Add_Click({ $form.Close() })
$panel.Controls.Add($btn, 0, 3)
$panel.SetColumnSpan($btn, 2)

$form.Controls.Add($panel)

$remaining = [int]$TimeoutSec
$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 1000
$timer.Add_Tick({
  $remaining--
  if ($remaining -lt 0) { $remaining = 0 }
  $timerLabel.Text = "Temps restant: ${remaining}s"
  if ($remaining -le 0) {
    $timer.Stop()
    $form.Close()
  }
})
$timer.Start()

try {
  $null = $form.ShowDialog()
} finally {
  $timer.Stop()
}

$commentText = $null
if ($commentBox -and $commentBox.Text) {
  $commentText = $commentBox.Text.Trim()
}
if (-not $commentText) { return }

$baseUrl = Get-ApiBaseUrl -Url $ApiUrl
if (-not $baseUrl) { Write-CommentLog 'Failed to derive API base URL'; return }

$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
try {
  $loginBody = @{ username = $CommentUser; password = $CommentPassword } | ConvertTo-Json
  $loginResp = Invoke-RestMethod -Uri "$baseUrl/api/login" -Method Post -ContentType 'application/json' -Body $loginBody -WebSession $session
  if (-not $loginResp -or -not $loginResp.ok) {
    Write-CommentLog 'Login failed for comment update'
    return
  }
  Write-CommentLog 'Login ok for comment update'
} catch {
  $statusLine = $null
  try {
    if ($_.Exception.Response) {
      $statusLine = "$([int]$_.Exception.Response.StatusCode) $($_.Exception.Response.StatusDescription)"
    }
  } catch { }
  if ($statusLine) {
    Write-CommentLog "Login error ($statusLine): $($_.Exception.Message)"
  } else {
    Write-CommentLog "Login error: $($_.Exception.Message)"
  }
  return
}

try {
  $commentBody = @{ comment = $commentText } | ConvertTo-Json
  Invoke-RestMethod -Uri "$baseUrl/api/machines/$ReportId/comment" -Method Put -ContentType 'application/json' -Body $commentBody -WebSession $session | Out-Null
  Write-CommentLog 'Comment updated via API'
} catch {
  $statusLine = $null
  $responseBody = $null
  try {
    if ($_.Exception.Response) {
      $statusLine = "$([int]$_.Exception.Response.StatusCode) $($_.Exception.Response.StatusDescription)"
      $stream = $_.Exception.Response.GetResponseStream()
      if ($stream) {
        $reader = New-Object System.IO.StreamReader($stream)
        $responseBody = $reader.ReadToEnd()
      }
    }
  } catch { }
  if ($statusLine -and $responseBody) {
    Write-CommentLog "Comment update failed ($statusLine): $responseBody"
  } elseif ($statusLine) {
    Write-CommentLog "Comment update failed ($statusLine): $($_.Exception.Message)"
  } else {
    Write-CommentLog "Comment update failed: $($_.Exception.Message)"
  }
}
'@
  Set-Content -Path $scriptPath -Value $commentScript -Encoding UTF8

  try {
    $psCmd = Get-Command powershell.exe -ErrorAction SilentlyContinue
    if (-not $psCmd) { return $false }
    $args = @(
      '-NoProfile',
      '-ExecutionPolicy', 'Bypass',
      '-STA',
      '-File', $scriptPath,
      '-ApiUrl', $ApiUrl,
      '-ReportId', $ReportId,
      '-TestsPath', $testsPath,
      '-CommentUser', $CommentUser,
      '-CommentPassword', $CommentPassword,
      '-LogPath', $LogPath,
      '-TimeoutSec', $TimeoutSec
    )
    Start-Process -FilePath $psCmd.Source -ArgumentList $args -WindowStyle Normal | Out-Null
    return $true
  } catch {
    Write-Log "Failed to start comment capture: $($_.Exception.Message)" 'WARN'
    return $false
  }
}

function Convert-WinsatNumber {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
  $clean = $Value.Trim()
  if (-not $clean) { return $null }
  $clean = $clean -replace '\s', ''
  $clean = $clean.Replace(',', '.')
  try {
    return [double]::Parse($clean, [System.Globalization.CultureInfo]::InvariantCulture)
  } catch {
    return $null
  }
}

function Update-MaxValue {
  param(
    [System.Collections.IDictionary]$Target,
    [string]$Key,
    $Value
  )

  if ($null -eq $Value) { return }
  if ($null -eq $Target) { return }
  $hasKey = $false
  try {
    if ($Target -is [System.Collections.Specialized.OrderedDictionary]) {
      $hasKey = $Target.Contains($Key)
    } else {
      $hasKey = $Target.ContainsKey($Key)
    }
  } catch {
    try { $hasKey = $Target.Contains($Key) } catch { $hasKey = $false }
  }
  if (-not $hasKey -or $Target[$Key] -lt $Value) {
    $Target[$Key] = $Value
  }
}

function Read-WinsatXml {
  param([string]$Path)

  if (-not (Test-Path $Path)) { return $null }
  try {
    return [xml](Get-Content -Path $Path -Raw -Encoding Unicode)
  } catch {
    try {
      return [xml](Get-Content -Path $Path -Raw)
    } catch {
      Write-Log "WinSAT XML parse failed: $Path ($($_.Exception.Message))" 'WARN'
      return $null
    }
  }
}

function Get-WinsatXmlFiles {
  param([string[]]$Roots)

  $files = @()
  foreach ($root in $Roots) {
    if (-not $root) { continue }
    if (-not (Test-Path $root)) { continue }
    try {
      $files += Get-ChildItem -Path $root -Filter '*.WinSAT.xml' -File -ErrorAction SilentlyContinue
    } catch { }
  }
  return $files
}

function Select-WinsatFilesByType {
  param([System.IO.FileInfo[]]$Files)

  $selected = @{}
  foreach ($file in $Files) {
    $name = $file.Name
    $type = $null
    if ($name -match '(?i)Formal\\.Assessment') { $type = 'formal' }
    elseif ($name -match '(?i)Cpu\\.Assessment') { $type = 'cpu' }
    elseif ($name -match '(?i)Mem\\.Assessment') { $type = 'mem' }
    elseif ($name -match '(?i)Disk\\.Assessment') { $type = 'disk' }
    elseif ($name -match '(?i)Graphics3D\\.Assessment') { $type = 'graphics3d' }
    elseif ($name -match '(?i)DWM\\.Assessment') { $type = 'dwm' }
    if (-not $type) { continue }
    if (-not $selected.ContainsKey($type) -or $file.LastWriteTime -gt $selected[$type].LastWriteTime) {
      $selected[$type] = $file
    }
  }
  return $selected
}

function Get-XmlText {
  param(
    [xml]$Doc,
    [string]$XPath
  )

  if (-not $Doc) { return $null }
  try {
    $node = $Doc.SelectSingleNode($XPath)
    if ($node -and $node.InnerText) { return $node.InnerText.Trim() }
  } catch { }
  return $null
}

function Get-WinsatFromDataStore {
  param(
    [string[]]$Paths
  )

  if ($SkipWinSatDataStore) { return $null }

  $roots = @()
  if ($Paths) {
    foreach ($path in $Paths) {
      if ($path) { $roots += $path }
    }
  }
  $files = Get-WinsatXmlFiles -Roots $roots
  if (-not $files -or $files.Count -eq 0) { return $null }

  $selected = Select-WinsatFilesByType -Files $files
  if (-not $selected -or $selected.Count -eq 0) { return $null }

  $winsat = [ordered]@{
    source = $null
    files = @{}
    winSPR = [ordered]@{}
    metrics = [ordered]@{}
    cpu = [ordered]@{}
    cpuStatus = $null
    memory = [ordered]@{}
    disk = [ordered]@{}
    graphics = [ordered]@{}
    limitsApplied = @()
    hasNoD3DTest = $false
  }

  $limits = New-Object 'System.Collections.Generic.HashSet[string]'
  $sprFields = @('SystemScore', 'MemoryScore', 'CpuScore', 'CPUSubAggScore', 'VideoEncodeScore', 'GraphicsScore', 'GamingScore', 'DiskScore', 'Dx9SubScore', 'Dx10SubScore')

  foreach ($entry in $selected.GetEnumerator()) {
    $file = $entry.Value
    $doc = Read-WinsatXml -Path $file.FullName
    if (-not $doc) { continue }
    if (-not $winsat.source) { $winsat.source = $file.Directory.FullName }
    $winsat.files[$entry.Key] = $file.Name

    foreach ($field in $sprFields) {
      $raw = Get-XmlText -Doc $doc -XPath "//WinSPR/$field"
      $value = Convert-WinsatNumber $raw
      Update-MaxValue -Target $winsat.winSPR -Key $field -Value $value
    }

    if ($entry.Key -eq 'cpu' -and -not $winsat.cpuStatus) {
      $cpuNode = $null
      try { $cpuNode = $doc.SelectSingleNode('/WinSAT/CompletionStatus') } catch { }
      if ($cpuNode) {
        $cpuCode = $null
        $cpuDesc = $null
        try { $cpuCode = $cpuNode.InnerText.Trim() } catch { }
        try { $cpuDesc = $cpuNode.GetAttribute('description') } catch { }
        $cpuOk = $false
        if ($cpuCode -eq '0') { $cpuOk = $true }
        elseif ($cpuDesc -and $cpuDesc -match '(?i)réussite|success') { $cpuOk = $true }
        $winsat.cpuStatus = if ($cpuOk) { 'ok' } else { 'nok' }
      }
    }

    $limitNodes = $doc.SelectNodes('//WinSPR/LimitsApplied//LimitApplied')
    if ($limitNodes) {
      foreach ($node in $limitNodes) {
        $limitText = $null
        try {
          $friendly = $node.GetAttribute('Friendly')
          if ($friendly) { $limitText = $friendly.Trim() }
        } catch { }
        if (-not $limitText -and $node.InnerText) { $limitText = $node.InnerText.Trim() }
        if ($limitText) { [void]$limits.Add($limitText) }
      }
    }

    $cpuNodes = $doc.SelectNodes('//Metrics/CPUMetrics/*')
    if ($cpuNodes) {
      foreach ($node in $cpuNodes) {
        $value = Convert-WinsatNumber $node.InnerText
        Update-MaxValue -Target $winsat.cpu -Key $node.Name -Value $value
      }
    }

    $memBandwidth = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/MemoryMetrics/Bandwidth')
    Update-MaxValue -Target $winsat.memory -Key 'bandwidthMBps' -Value $memBandwidth

    $diskNodes = $doc.SelectNodes('//Metrics/DiskMetrics/AvgThroughput')
    if ($diskNodes) {
      foreach ($node in $diskNodes) {
        $kind = $null
        try { $kind = $node.GetAttribute('kind') } catch { }
        $value = Convert-WinsatNumber $node.InnerText
        if (-not $kind) { continue }
        if ($kind -match 'Sequential\s+Read|Séquentielles\s+Lire') {
          Update-MaxValue -Target $winsat.disk -Key 'seqReadMBps' -Value $value
        } elseif ($kind -match 'Sequential\s+Write|Séquentielles\s+Ecriture|Séquentielles\s+Écriture') {
          Update-MaxValue -Target $winsat.disk -Key 'seqWriteMBps' -Value $value
        } elseif ($kind -match 'Random\s+Read|Aléatoires\s+Lire') {
          Update-MaxValue -Target $winsat.disk -Key 'randReadMBps' -Value $value
        } elseif ($kind -match 'Random\s+Write|Aléatoires\s+Ecriture|Aléatoires\s+Écriture') {
          Update-MaxValue -Target $winsat.disk -Key 'randWriteMBps' -Value $value
        }
      }
    }

    $dwmFps = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/GraphicsMetrics/DWMFps')
    Update-MaxValue -Target $winsat.graphics -Key 'dwmFps' -Value $dwmFps
    $videoMem = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/GraphicsMetrics/VideoMemBandwidth')
    Update-MaxValue -Target $winsat.graphics -Key 'videoMemBandwidthMBps' -Value $videoMem
  }

  if ($limits.Count -gt 0) {
    $winsat.limitsApplied = @($limits)
    foreach ($limit in $winsat.limitsApplied) {
      if ($limit -match 'NoD3DTestRun|D3D|D3D test') {
        $winsat.hasNoD3DTest = $true
        break
      }
    }
  }

  if ($winsat.cpu.Count -gt 0) {
    $cpuValues = @()
    foreach ($val in $winsat.cpu.Values) {
      if ($val -is [double]) { $cpuValues += $val }
    }
    if ($cpuValues.Count -gt 0) {
      $winsat.cpu.maxMBps = [math]::Round(($cpuValues | Measure-Object -Maximum).Maximum, 1)
    }
  }

  return $winsat
}

function Get-WinsatFromXmlFile {
  param([string]$Path)

  if (-not $Path -or -not (Test-Path -Path $Path)) { return $null }
  $doc = Read-WinsatXml -Path $Path
  if (-not $doc) { return $null }

  $winsat = [ordered]@{
    source = (Split-Path $Path -Parent)
    files = @{}
    winSPR = [ordered]@{}
    metrics = [ordered]@{}
    cpu = [ordered]@{}
    cpuStatus = $null
    memory = [ordered]@{}
    disk = [ordered]@{}
    graphics = [ordered]@{}
    limitsApplied = @()
    hasNoD3DTest = $false
  }

  $winsat.files['formal'] = (Split-Path $Path -Leaf)
  $limits = New-Object 'System.Collections.Generic.HashSet[string]'
  $sprFields = @('SystemScore', 'MemoryScore', 'CpuScore', 'CPUSubAggScore', 'VideoEncodeScore', 'GraphicsScore', 'GamingScore', 'DiskScore', 'Dx9SubScore', 'Dx10SubScore')

  foreach ($field in $sprFields) {
    $raw = Get-XmlText -Doc $doc -XPath "//WinSPR/$field"
    $value = Convert-WinsatNumber $raw
    Update-MaxValue -Target $winsat.winSPR -Key $field -Value $value
  }

  try {
    $cpuNode = $doc.SelectSingleNode('/WinSAT/CompletionStatus')
    if ($cpuNode) {
      $cpuCode = $null
      $cpuDesc = $null
      try { $cpuCode = $cpuNode.InnerText.Trim() } catch { }
      try { $cpuDesc = $cpuNode.GetAttribute('description') } catch { }
      $cpuOk = $false
      if ($cpuCode -eq '0') { $cpuOk = $true }
      elseif ($cpuDesc -and $cpuDesc -match '(?i)réussite|success') { $cpuOk = $true }
      $winsat.cpuStatus = if ($cpuOk) { 'ok' } else { 'nok' }
    }
  } catch { }

  $limitNodes = $doc.SelectNodes('//WinSPR/LimitsApplied//LimitApplied')
  if ($limitNodes) {
    foreach ($node in $limitNodes) {
      $limitText = $null
      try {
        $friendly = $node.GetAttribute('Friendly')
        if ($friendly) { $limitText = $friendly.Trim() }
      } catch { }
      if (-not $limitText -and $node.InnerText) { $limitText = $node.InnerText.Trim() }
      if ($limitText) { [void]$limits.Add($limitText) }
    }
  }

  $cpuNodes = $doc.SelectNodes('//Metrics/CPUMetrics/*')
  if ($cpuNodes) {
    foreach ($node in $cpuNodes) {
      $value = Convert-WinsatNumber $node.InnerText
      Update-MaxValue -Target $winsat.cpu -Key $node.Name -Value $value
    }
  }

  $memBandwidth = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/MemoryMetrics/Bandwidth')
  Update-MaxValue -Target $winsat.memory -Key 'bandwidthMBps' -Value $memBandwidth

  $diskNodes = $doc.SelectNodes('//Metrics/DiskMetrics/AvgThroughput')
  if ($diskNodes) {
    foreach ($node in $diskNodes) {
      $kind = $null
      try { $kind = $node.GetAttribute('kind') } catch { }
      $value = Convert-WinsatNumber $node.InnerText
      if (-not $kind) { continue }
      if ($kind -match 'Sequential\s+Read|Séquentielles\s+Lire') {
        Update-MaxValue -Target $winsat.disk -Key 'seqReadMBps' -Value $value
      } elseif ($kind -match 'Sequential\s+Write|Séquentielles\s+Ecriture|Séquentielles\s+Écriture') {
        Update-MaxValue -Target $winsat.disk -Key 'seqWriteMBps' -Value $value
      } elseif ($kind -match 'Random\s+Read|Aléatoires\s+Lire') {
        Update-MaxValue -Target $winsat.disk -Key 'randReadMBps' -Value $value
      } elseif ($kind -match 'Random\s+Write|Aléatoires\s+Ecriture|Aléatoires\s+Écriture') {
        Update-MaxValue -Target $winsat.disk -Key 'randWriteMBps' -Value $value
      }
    }
  }

  $dwmFps = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/GraphicsMetrics/DWMFps')
  Update-MaxValue -Target $winsat.graphics -Key 'dwmFps' -Value $dwmFps
  $videoMem = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//Metrics/GraphicsMetrics/VideoMemBandwidth')
  Update-MaxValue -Target $winsat.graphics -Key 'videoMemBandwidthMBps' -Value $videoMem

  if ($limits.Count -gt 0) {
    $winsat.limitsApplied = @($limits)
    foreach ($limit in $winsat.limitsApplied) {
      if ($limit -match 'NoD3DTestRun|D3D|D3D test') {
        $winsat.hasNoD3DTest = $true
        break
      }
    }
  }

  if ($winsat.cpu.Count -gt 0) {
    $cpuValues = @()
    foreach ($val in $winsat.cpu.Values) {
      if ($val -is [double]) { $cpuValues += $val }
    }
    if ($cpuValues.Count -gt 0) {
      $winsat.cpu.maxMBps = [math]::Round(($cpuValues | Measure-Object -Maximum).Maximum, 1)
    }
  }

  return $winsat
}

function Get-WinsatDiskFromXml {
  param([string]$Path)

  if (-not $Path -or -not (Test-Path -Path $Path)) { return $null }
  $doc = Read-WinsatXml -Path $Path
  if (-not $doc) { return $null }

  $disk = [ordered]@{}
  $diskNodes = $doc.SelectNodes('//Metrics/DiskMetrics/AvgThroughput')
  if ($diskNodes) {
    foreach ($node in $diskNodes) {
      $kind = $null
      try { $kind = $node.GetAttribute('kind') } catch { }
      $value = Convert-WinsatNumber $node.InnerText
      if (-not $kind) { continue }
      if ($kind -match 'Sequential\s+Read|Séquentielles\s+Lire') {
        $disk['seqReadMBps'] = $value
      } elseif ($kind -match 'Sequential\s+Write|Séquentielles\s+Ecriture|Séquentielles\s+Écriture') {
        $disk['seqWriteMBps'] = $value
      } elseif ($kind -match 'Random\s+Read|Aléatoires\s+Lire') {
        $disk['randReadMBps'] = $value
      } elseif ($kind -match 'Random\s+Write|Aléatoires\s+Ecriture|Aléatoires\s+Écriture') {
        $disk['randWriteMBps'] = $value
      }
    }
  }
  if ($disk.Count -gt 0) { return $disk }
  return $null
}

function Get-WinsatCpuFallback {
  param([string[]]$Roots)

  $files = Get-WinsatXmlFiles -Roots $Roots
  if (-not $files -or $files.Count -eq 0) { return $null }
  $selected = Select-WinsatFilesByType -Files $files
  if (-not $selected -or $selected.Count -eq 0) { return $null }
  $cpuFile = $null
  if ($selected.ContainsKey('cpu')) { $cpuFile = $selected['cpu'] }
  elseif ($selected.ContainsKey('formal')) { $cpuFile = $selected['formal'] }
  if (-not $cpuFile) { return $null }

  $doc = Read-WinsatXml -Path $cpuFile.FullName
  if (-not $doc) { return $null }

  $cpuStatus = $null
  try {
    $node = $doc.SelectSingleNode('/WinSAT/CompletionStatus')
    if ($node) {
      $code = $null
      $desc = $null
      try { $code = $node.InnerText.Trim() } catch { }
      try { $desc = $node.GetAttribute('description') } catch { }
      if ($code -eq '0' -or ($desc -and $desc -match '(?i)réussite|success')) {
        $cpuStatus = 'ok'
      } else {
        $cpuStatus = 'nok'
      }
    }
  } catch { }

  $cpuScore = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//WinSPR/CpuScore')
  $cpuMax = $null
  $cpuNodes = $doc.SelectNodes('//Metrics/CPUMetrics/*')
  if ($cpuNodes) {
    foreach ($node in $cpuNodes) {
      $value = Convert-WinsatNumber $node.InnerText
      if ($null -eq $value) { continue }
      if ($cpuMax -eq $null -or $value -gt $cpuMax) { $cpuMax = $value }
    }
  }

  return @{
    status = $cpuStatus
    score = $cpuScore
    maxMBps = if ($cpuMax -ne $null) { [math]::Round($cpuMax, 1) } else { $null }
    file = $cpuFile.Name
  }
}

function Get-WinsatCpuFallback {
  param([string[]]$Roots)

  $files = Get-WinsatXmlFiles -Roots $Roots
  if (-not $files -or $files.Count -eq 0) { return $null }
  $selected = Select-WinsatFilesByType -Files $files
  if (-not $selected -or $selected.Count -eq 0) { return $null }
  $cpuFile = $null
  if ($selected.ContainsKey('cpu')) { $cpuFile = $selected['cpu'] }
  elseif ($selected.ContainsKey('formal')) { $cpuFile = $selected['formal'] }
  if (-not $cpuFile) { return $null }

  $doc = Read-WinsatXml -Path $cpuFile.FullName
  if (-not $doc) { return $null }

  $cpuStatus = $null
  try {
    $node = $doc.SelectSingleNode('/WinSAT/CompletionStatus')
    if ($node) {
      $code = $null
      $desc = $null
      try { $code = $node.InnerText.Trim() } catch { }
      try { $desc = $node.GetAttribute('description') } catch { }
      if ($code -eq '0' -or ($desc -and $desc -match '(?i)réussite|success')) {
        $cpuStatus = 'ok'
      } else {
        $cpuStatus = 'nok'
      }
    }
  } catch { }

  $cpuScore = Convert-WinsatNumber (Get-XmlText -Doc $doc -XPath '//WinSPR/CpuScore')
  $cpuMax = $null
  $cpuNodes = $doc.SelectNodes('//Metrics/CPUMetrics/*')
  if ($cpuNodes) {
    foreach ($node in $cpuNodes) {
      $value = Convert-WinsatNumber $node.InnerText
      if ($null -eq $value) { continue }
      if ($cpuMax -eq $null -or $value -gt $cpuMax) { $cpuMax = $value }
    }
  }

  return @{
    status = $cpuStatus
    score = $cpuScore
    maxMBps = if ($cpuMax -ne $null) { [math]::Round($cpuMax, 1) } else { $null }
    file = $cpuFile.Name
  }
}

function Build-ArgumentList {
  param([hashtable]$BoundParams)

  $args = @()
  if (-not $BoundParams) { return $args }
  foreach ($entry in $BoundParams.GetEnumerator()) {
    $name = $entry.Key
    $value = $entry.Value
    if ($name -eq 'SkipElevation') { continue }
    if ($value -is [System.Management.Automation.SwitchParameter]) {
      if ($value.IsPresent) { $args += "-$name" }
      continue
    }
    if ($value -is [bool]) {
      if ($value) { $args += "-$name" }
      continue
    }
    if ($null -eq $value) { continue }
    if ($value -is [System.Array] -and -not ($value -is [string])) {
      $args += "-$name"
      foreach ($item in $value) { $args += "$item" }
      continue
    }
    $args += "-$name"
    $args += "$value"
  }
  return $args
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

function Resolve-CameraTestExe {
  param([string]$Value)

  $candidate = $Value
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    $candidate = Join-Path $PSScriptRoot 'camera.exe'
  }

  if (-not [System.IO.Path]::IsPathRooted($candidate)) {
    $candidate = Join-Path (Get-Location) $candidate
  }

  if (-not (Test-Path $candidate)) {
    if ($candidate -match 'camera\\.exe$') {
      $fallback = $candidate -replace 'camera\\.exe$', 'camera_capture.exe'
      if (Test-Path $fallback) {
        $candidate = $fallback
      }
    }
    if (-not [System.IO.Path]::GetExtension($candidate)) {
      $exeCandidate = "${candidate}.exe"
      if (Test-Path $exeCandidate) {
        $candidate = $exeCandidate
      }
    }
  }

  if (-not (Test-Path $candidate)) { return $null }
  if ([System.IO.Path]::GetExtension($candidate).ToLowerInvariant() -ne '.exe') {
    Write-Log "Camera test must be .exe, skipping: $candidate" 'WARN'
    return $null
  }
  return $candidate
}

function Resolve-CameraOutputDir {
  param([string]$Value)

  $candidate = $Value
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    $candidate = $env:CAMERA_OUTDIR
  }
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    $candidate = $env:DIAG_TOOL_OUTDIR
  }
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    if ($env:TEMP) {
      $candidate = Join-Path $env:TEMP 'mdt-camera'
    } else {
      $candidate = Join-Path $PSScriptRoot 'mdt-camera'
    }
  }
  if (-not [System.IO.Path]::IsPathRooted($candidate)) {
    $candidate = Join-Path (Get-Location) $candidate
  }
  return $candidate
}

function Ensure-Directory {
  param([string]$Path)

  if (-not $Path) { return $null }
  try {
    if (-not (Test-Path -Path $Path)) {
      New-Item -Path $Path -ItemType Directory -Force | Out-Null
    }
    return $Path
  } catch {
    Write-Log "Ensure-Directory failed: $($_.Exception.Message)" 'WARN'
    return $null
  }
}

function Resolve-ArtifactRoot {
  param([string]$Value)

  $candidate = $Value
  if ([string]::IsNullOrWhiteSpace($candidate)) {
    if ($env:TEMP) {
      $candidate = Join-Path $env:TEMP 'mdt-fusion\artifacts'
    } else {
      $candidate = Join-Path $PSScriptRoot 'mdt-fusion\artifacts'
    }
  }
  $candidate = [Environment]::ExpandEnvironmentVariables($candidate)
  if (-not [System.IO.Path]::IsPathRooted($candidate)) {
    $candidate = Join-Path (Get-Location) $candidate
  }
  return $candidate
}

function Get-SafeKey {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) { return 'unknown' }
  return ($Value -replace '[^A-Za-z0-9._-]', '-')
}

function Get-MacSerialKey {
  param(
    [string]$MacAddress,
    [string]$SerialNumber,
    [string]$Hostname
  )

  if ($SerialNumber -and $MacAddress) { return "sn:$SerialNumber|mac:$MacAddress" }
  if ($SerialNumber) { return "sn:$SerialNumber" }
  if ($MacAddress) { return "mac:$MacAddress" }
  if ($Hostname) { return "host:$Hostname" }
  return 'unknown'
}

function New-ArtifactRunDir {
  param(
    [string]$ArtifactRoot,
    [string]$MacSerialKey,
    [string]$ClientRunId
  )

  if (-not $ArtifactRoot) { return $null }
  $safeKey = Get-SafeKey -Value $MacSerialKey
  $runDir = Join-Path $ArtifactRoot (Join-Path $safeKey $ClientRunId)
  return (Ensure-Directory -Path $runDir)
}

function Copy-FileSafe {
  param(
    [string]$Source,
    [string]$Destination
  )

  if (-not $Source -or -not (Test-Path $Source)) { return $false }
  try {
    if ($Destination -and (Test-Path $Destination)) {
      try {
        $srcItem = Get-Item -Path $Source -ErrorAction Stop
        $dstItem = Get-Item -Path $Destination -ErrorAction Stop
        if ($srcItem.FullName -eq $dstItem.FullName) { return $true }
      } catch { }
    }
    $targetDir = Split-Path $Destination -Parent
    if ($targetDir) { Ensure-Directory -Path $targetDir | Out-Null }
    Copy-Item -Path $Source -Destination $Destination -Force -ErrorAction Stop
    return $true
  } catch {
    Write-Log "Copy failed ($Source): $($_.Exception.Message)" 'WARN'
    return $false
  }
}

function Copy-DirContents {
  param(
    [string]$SourceDir,
    [string]$DestinationDir
  )

  if (-not $SourceDir -or -not (Test-Path $SourceDir)) { return $false }
  try {
    Ensure-Directory -Path $DestinationDir | Out-Null
    Copy-Item -Path (Join-Path $SourceDir '*') -Destination $DestinationDir -Recurse -Force -ErrorAction Stop
    return $true
  } catch {
    Write-Log "Copy dir failed ($SourceDir): $($_.Exception.Message)" 'WARN'
    return $false
  }
}

function Write-JsonFile {
  param(
    [string]$Path,
    [object]$Object,
    [int]$Depth = 10
  )

  if (-not $Path) { return $false }
  try {
    $targetDir = Split-Path $Path -Parent
    if ($targetDir) { Ensure-Directory -Path $targetDir | Out-Null }
    $Object | ConvertTo-Json -Depth $Depth | Out-File -FilePath $Path -Encoding UTF8 -Force
    return $true
  } catch {
    Write-Log "Write JSON failed ($Path): $($_.Exception.Message)" 'WARN'
    return $false
  }
}

function Invoke-JsonPost {
  param(
    [string]$Url,
    [string]$Json,
    [int]$TimeoutSec
  )

  if (-not $Url -or -not $Json) { return $null }
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($Json)
  return Invoke-RestMethod -Uri $Url -Method Post -ContentType 'application/json; charset=utf-8' -Body $bytes -TimeoutSec $TimeoutSec
}

function Resolve-McPath {
  param([string]$Value)

  if ($Value -and (Test-Path $Value)) { return $Value }
  if (-not $env:TEMP) { return $null }
  $binRoot = Join-Path $env:TEMP 'mdt-fusion\bin'
  Ensure-Directory -Path $binRoot | Out-Null
  $mcPath = Join-Path $binRoot 'mc.exe'
  if (-not (Test-Path $mcPath)) {
    try {
      Invoke-WebRequest -Uri 'https://dl.min.io/client/mc/release/windows-amd64/mc.exe' -OutFile $mcPath -UseBasicParsing -ErrorAction Stop
      Unblock-File -Path $mcPath -ErrorAction SilentlyContinue
    } catch {
      Write-Log "mc.exe download failed: $($_.Exception.Message)" 'WARN'
    }
  }
  if (Test-Path $mcPath) { return $mcPath }
  return $null
}

function Stage-RunArtifacts {
  param(
    [string]$RunDir,
    [string]$LogPath,
    [string]$PayloadJson,
    [string]$CameraOutputDir,
    [string]$KeyboardLogPath,
    [hashtable]$WinsatStore,
    [hashtable]$Manifest
  )

  if (-not $RunDir) { return $false }
  Ensure-Directory -Path $RunDir | Out-Null

  if ($Manifest) {
    Write-JsonFile -Path (Join-Path $RunDir 'run_manifest.json') -Object $Manifest | Out-Null
  }
  if ($LogPath) {
    Copy-FileSafe -Source $LogPath -Destination (Join-Path $RunDir 'mdt-report.log') | Out-Null
  }
  if ($PayloadJson) {
    try {
      $payloadPath = Join-Path $RunDir 'payload.json'
      $PayloadJson | Out-File -FilePath $payloadPath -Encoding UTF8 -Force
    } catch {
      Write-Log "Payload write failed: $($_.Exception.Message)" 'WARN'
    }
  }
  if ($CameraOutputDir) {
    Copy-DirContents -SourceDir $CameraOutputDir -DestinationDir (Join-Path $RunDir 'camera') | Out-Null
  }
  if ($KeyboardLogPath) {
    $dest = Join-Path $RunDir 'keyboard'
    $destFile = Join-Path $dest (Split-Path $KeyboardLogPath -Leaf)
    Copy-FileSafe -Source $KeyboardLogPath -Destination $destFile | Out-Null
  }
  if ($WinsatStore -and $WinsatStore.files) {
    $winsatDir = Join-Path $RunDir 'winsat'
    Ensure-Directory -Path $winsatDir | Out-Null
    $sourceRoot = $WinsatStore.source
    $names = @($WinsatStore.files.Values) | Sort-Object -Unique
    foreach ($name in $names) {
      if (-not $name) { continue }
      $sourcePath = $null
      if ($sourceRoot) {
        $sourcePath = Join-Path $sourceRoot $name
      } elseif (Test-Path $name) {
        $sourcePath = $name
      }
      if ($sourcePath -and (Test-Path $sourcePath)) {
        Copy-FileSafe -Source $sourcePath -Destination (Join-Path $winsatDir (Split-Path $sourcePath -Leaf)) | Out-Null
      }
    }
  }
  return $true
}

function Upload-RunArtifacts {
  param(
    [string]$RunDir,
    [string]$Endpoint,
    [string]$Bucket,
    [string]$AccessKey,
    [string]$SecretKey,
    [string]$Prefix,
    [string]$Tag,
    [string]$MacSerialKey,
    [string]$ClientRunId,
    [string]$McPath
  )

  $result = [ordered]@{
    attempted = $false
    success = $false
  }

  if (-not $RunDir -or -not (Test-Path $RunDir)) {
    $result.error = 'run_dir_missing'
    return $result
  }
  if (-not $Endpoint -or -not $Bucket -or -not $AccessKey -or -not $SecretKey) {
    $result.error = 'missing_object_storage_config'
    return $result
  }

  $rootPrefix = if ($Prefix) { $Prefix.Trim('/','\') } else { 'run' }
  $tagSegment = if ($Tag) { Get-SafeKey -Value $Tag } else { '' }
  if ($tagSegment) { $rootPrefix = "$rootPrefix/$tagSegment" }
  $safeKey = Get-SafeKey -Value $MacSerialKey
  $destPath = ("{0}/{1}/{2}/{3}/{4}" -f 'diagobj', $Bucket, $rootPrefix, $safeKey, $ClientRunId)
  $result.attempted = $true
  $result.bucket = $Bucket
  $result.prefix = $rootPrefix
  $result.destination = $destPath

  $resolvedMcPath = Resolve-McPath -Value $McPath
  if (-not $resolvedMcPath) {
    $result.error = 'mc_missing'
    return $result
  }

  try {
    & $resolvedMcPath alias set diagobj $Endpoint $AccessKey $SecretKey *> $null
    if ($LASTEXITCODE -ne 0) { throw ("mc alias set failed (" + $LASTEXITCODE + ")") }
    & $resolvedMcPath mirror --overwrite "$RunDir" "$destPath" *> $null
    if ($LASTEXITCODE -ne 0) { throw ("mc mirror failed (" + $LASTEXITCODE + ")") }
    Write-Log "Raw artifacts mirrored to $destPath"
    $result.success = $true
    return $result
  } catch {
    $result.error = $_.Exception.Message
    Write-Log "Raw artifact upload failed: $($result.error)" 'WARN'
    return $result
  }
}

function Get-CameraStatusFromOutput {
  param(
    [string]$OutputDir,
    [string]$FallbackStatus
  )

  if (-not $OutputDir -or -not (Test-Path $OutputDir)) {
    if ($FallbackStatus -eq 'ok') { return 'ok' }
    return 'nok'
  }

  $errorLog = Join-Path $OutputDir 'camera_error.log'
  if (Test-Path $errorLog) { return 'nok' }

  $problemFile = Get-ChildItem -Path $OutputDir -Filter '*.problem.txt' -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($problemFile) { return 'nok' }

  $photoFile = Get-ChildItem -Path $OutputDir -Filter '*.jpg' -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($photoFile) { return 'ok' }

  if ($FallbackStatus -eq 'ok') { return 'ok' }
  return 'nok'
}

function Invoke-CameraTest {
  param(
    [string]$Path,
    [string[]]$Arguments = @(),
    [int]$TimeoutSec = 20,
    [string]$OutputBaseDir
  )

  if (-not $Path) {
    return @{ status = 'not_tested'; outputDir = $null; exitCode = $null }
  }

  $baseDir = Resolve-CameraOutputDir -Value $OutputBaseDir
  $runDir = Join-Path $baseDir ("run-" + [guid]::NewGuid().ToString('N'))
  try { New-Item -ItemType Directory -Path $runDir -Force | Out-Null } catch { }

  $args = Normalize-ArgumentList $Arguments
  $hasOutdir = $false
  foreach ($arg in $args) {
    if ($arg -match '^-{1,2}outdir($|=)') {
      $hasOutdir = $true
      break
    }
  }
  if (-not $hasOutdir -and $runDir) {
    $args = @('--outdir', $runDir) + $args
  }

  Write-Log "Camera test output dir: $runDir"
  $result = Invoke-ExternalCommand -Path $Path -Arguments $args -TimeoutSec $TimeoutSec -Name 'Camera'
  $status = Get-CameraStatusFromOutput -OutputDir $runDir -FallbackStatus $result.status

  return @{ status = $status; outputDir = $runDir; exitCode = $result.exitCode }
}

function Invoke-FactoryResetPrompt {
  param(
    [switch]$ForceReset,
    [string]$ConfirmToken
  )

  if ($SkipFactoryResetPrompt) {
    Write-Log 'Factory reset prompt skipped (SkipFactoryResetPrompt set).' 'WARN'
    return
  }

  $shouldReset = $false
  $forcePrompt = $false
  if ($ForceReset) {
    if ($ConfirmToken -and $ConfirmToken.ToUpperInvariant() -eq 'RESET') {
      $shouldReset = $true
    } else {
      Write-Log 'Factory reset confirmation missing; falling back to prompt.' 'WARN'
      $forcePrompt = $true
    }
  }
  if (-not $ForceReset -or $forcePrompt) {
    $prompted = $false
    $answerYes = $false
    $promptTitle = 'MDT Live Ops'
    $promptText = 'Reset usine maintenant ?'
    try {
      $shell = New-Object -ComObject WScript.Shell
      $popupResult = $shell.Popup($promptText, 0, $promptTitle, 0x4 + 0x30 + 0x1000)
      $prompted = $true
      if ($popupResult -eq 6) { $answerYes = $true }
    } catch {
      try {
        Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
        $result = [System.Windows.Forms.MessageBox]::Show(
          $promptText,
          $promptTitle,
          [System.Windows.Forms.MessageBoxButtons]::YesNo,
          [System.Windows.Forms.MessageBoxIcon]::Warning,
          [System.Windows.Forms.MessageBoxDefaultButton]::Button2,
          [System.Windows.Forms.MessageBoxOptions]::ServiceNotification
        )
        $prompted = $true
        if ($result -eq [System.Windows.Forms.DialogResult]::Yes) { $answerYes = $true }
      } catch {
        $isInteractive = $true
        try { $isInteractive = [Environment]::UserInteractive } catch { }
        if (-not $isInteractive) {
          Write-Log 'Factory reset skipped (non-interactive session).' 'WARN'
          return
        }
        $answer = Read-Host 'Reset usine maintenant ? (oui/non)'
        $prompted = $true
        if ($answer -match '^(o|oui|y|yes)$') { $answerYes = $true }
      }
    }

    if (-not $prompted) {
      Write-Log 'Factory reset prompt failed.' 'WARN'
      return
    }
    if (-not $answerYes) {
      Write-Log 'Factory reset cancelled by user.' 'WARN'
      return
    }
    $shouldReset = $true
  }

  if (-not $shouldReset) { return }

  $resetCmd = Get-Command systemreset.exe -ErrorAction SilentlyContinue
  if (-not $resetCmd) {
    $candidates = @()
    if ($env:SystemRoot) {
      $candidates += Join-Path $env:SystemRoot 'System32\\systemreset.exe'
      $candidates += Join-Path $env:SystemRoot 'SysNative\\systemreset.exe'
    }
    foreach ($candidate in $candidates) {
      if ($candidate -and (Test-Path $candidate)) {
        $resetCmd = [pscustomobject]@{ Source = $candidate }
        break
      }
    }
  }
  if (-not $resetCmd) {
    Write-Log 'Factory reset requested but systemreset.exe not found. Opening Settings > Recovery.' 'WARN'
    try {
      Start-Process -FilePath 'ms-settings:recovery' | Out-Null
    } catch {
      try { Start-Process -FilePath 'explorer.exe' -ArgumentList 'ms-settings:recovery' | Out-Null } catch { }
    }
    Send-ResetUiKeys -WindowTitle 'Paramètres' -DelaySec 20
    return
  }

  Write-Log 'Factory reset requested. Launching systemreset.exe -factoryreset' 'WARN'
  try {
    $resetProc = $null
    if (-not $script:IsAdmin) {
      Write-Log 'Factory reset requires elevation; requesting UAC.' 'WARN'
      $resetProc = Start-Process -FilePath $resetCmd.Source -ArgumentList '-factoryreset' -Verb RunAs -WindowStyle Normal -PassThru
    } else {
      $resetProc = Start-Process -FilePath $resetCmd.Source -ArgumentList '-factoryreset' -WindowStyle Normal -PassThru
    }
    if ($resetProc) {
      Write-Log "Factory reset process started (PID=$($resetProc.Id))." 'WARN'
      Send-ResetUiKeys -ProcessId $resetProc.Id -DelaySec 20
      Start-Sleep -Seconds 2
      $stillRunning = $null
      try { $stillRunning = Get-Process -Id $resetProc.Id -ErrorAction SilentlyContinue } catch { }
      if (-not $stillRunning) {
        Write-Log 'Factory reset process exited quickly; opening Settings > Recovery.' 'WARN'
        try { Start-Process -FilePath 'ms-settings:recovery' | Out-Null } catch { }
      }
    }
  } catch {
    Write-Log "Factory reset launch failed: $($_.Exception.Message)" 'WARN'
    try { Start-Process -FilePath 'ms-settings:recovery' | Out-Null } catch { }
  }
}

function Copy-ScriptsForElevation {
  param([string]$ScriptPath)

  if (-not $ScriptPath) { return $null }
  $sourceDir = Split-Path $ScriptPath -Parent
  if (-not $sourceDir) { return $null }
  $targetRoot = Join-Path $env:TEMP ("mdt-elev-{0}" -f [guid]::NewGuid().ToString('N'))
  $targetDir = Join-Path $targetRoot (Split-Path $sourceDir -Leaf)
  try {
    Copy-Item -Path $sourceDir -Destination $targetDir -Recurse -Force -ErrorAction Stop
    $targetScript = Join-Path $targetDir (Split-Path $ScriptPath -Leaf)
    if (Test-Path $targetScript) { return $targetScript }
  } catch {
    Write-Log "Failed to stage scripts for elevation: $($_.Exception.Message)" 'WARN'
  }
  return $null
}

function Invoke-KeyboardCapture {
  param(
    [string]$ScriptPath,
    [string]$LogPath,
    [string]$ConfigDir,
    [string]$Layout,
    [string]$LayoutConfig,
    [switch]$BlockInput,
    [int]$TimeoutSec = 600
  )

  if (-not $ScriptPath) { return @{ status = 'not_tested'; exitCode = $null } }
  if (-not (Test-Path $ScriptPath)) {
    Write-Log "Keyboard capture script not found: $ScriptPath" 'WARN'
    return @{ status = 'not_tested'; exitCode = $null }
  }

  $isInteractive = $true
  try { $isInteractive = [Environment]::UserInteractive } catch { }
  if (-not $isInteractive) {
    Write-Log 'Keyboard capture skipped (non-interactive session).' 'WARN'
    return @{ status = 'not_tested'; exitCode = $null }
  }

  $psCmd = Get-Command powershell.exe -ErrorAction SilentlyContinue
  if (-not $psCmd) {
    Write-Log 'powershell.exe not found for keyboard capture' 'WARN'
    return @{ status = 'not_tested'; exitCode = $null }
  }

  if (-not $LogPath) {
    $LogPath = Join-Path $env:TEMP ("mdt-keyboard-{0}.jsonl" -f [guid]::NewGuid().ToString('N'))
  }

  $args = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-STA', '-NoLogo', '-File', $ScriptPath)
  if ($LogPath) { $args += @('-LogPath', $LogPath) }
  if ($ConfigDir) { $args += @('-ConfigDir', $ConfigDir) }
  if ($Layout) { $args += @('-Layout', $Layout) }
  if ($LayoutConfig) { $args += @('-LayoutConfig', $LayoutConfig) }
  if ($BlockInput) { $args += '-BlockInput' }

  try {
    $workingDir = Split-Path $ScriptPath -Parent
    $proc = Start-Process -FilePath $psCmd.Source -ArgumentList $args -WindowStyle Normal -WorkingDirectory $workingDir -PassThru
    if (-not $proc) {
      Write-Log 'Keyboard capture failed to start.' 'WARN'
      return @{ status = 'not_tested'; exitCode = $null }
    }
    try {
      $null = $proc.WaitForInputIdle(5000)
      $wshell = New-Object -ComObject WScript.Shell
      $null = $wshell.AppActivate($proc.Id)
    } catch {
      Write-Log "Keyboard capture focus hint failed: $($_.Exception.Message)" 'WARN'
    }
    if ($TimeoutSec -gt 0) {
      if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
        Write-Log "Keyboard capture timed out after ${TimeoutSec}s." 'WARN'
        try { $proc.Kill() } catch { }
        return @{ status = 'not_tested'; exitCode = $null; timedOut = $true }
      }
    } else {
      $proc.WaitForExit()
    }
    $exitCode = $proc.ExitCode
    $status = switch ($exitCode) {
      0 { 'ok' }
      1 { 'nok' }
      2 { 'not_tested' }
      default { 'not_tested' }
    }
    Write-Log "Keyboard capture finished: status=$status exitCode=$exitCode"
    $logResult = Get-KeyboardResultFromLog -Path $LogPath
    if ($status -ne 'ok' -and $status -ne 'nok' -and $logResult -and $logResult.status) {
      Write-Log "Keyboard capture status recovered from log: $($logResult.status)"
      return @{ status = $logResult.status; padStatus = $logResult.padStatus; exitCode = $exitCode; fromLog = $true }
    }
    return @{ status = $status; padStatus = $logResult.padStatus; exitCode = $exitCode }
  } catch {
    Write-Log "Keyboard capture launch failed: $($_.Exception.Message)" 'WARN'
    return @{ status = 'not_tested'; exitCode = $null }
  }
}

function Get-KeyboardResultFromLog {
  param([string]$Path)

  if (-not $Path -or -not (Test-Path $Path)) { return $null }
  try {
    $lines = Get-Content -Path $Path -Tail 200
  } catch {
    return $null
  }
  if (-not $lines) { return $null }
  $reversed = [System.Collections.ArrayList]@($lines)
  $reversed.Reverse()
  foreach ($line in $reversed) {
    if (-not $line) { continue }
    try {
      $obj = $line | ConvertFrom-Json -ErrorAction Stop
      if ($obj -and ($obj.status -or $obj.padStatus)) {
        $statusValue = $null
        $padValue = $null
        if ($obj.status) { $statusValue = $obj.status.ToString().Trim().ToLowerInvariant() }
        if ($obj.padStatus) { $padValue = $obj.padStatus.ToString().Trim().ToLowerInvariant() }
        return @{
          status = $statusValue
          padStatus = $padValue
        }
      }
    } catch { }
  }
  return $null
}

function Start-KeyboardCaptureAsync {
  param(
    [string]$ScriptPath,
    [string]$LogPath,
    [string]$ConfigDir,
    [string]$Layout,
    [string]$LayoutConfig,
    [switch]$BlockInput,
    [int]$TimeoutSec = 600
  )

  if (-not $ScriptPath) { return @{ started = $false; status = 'not_tested' } }
  if (-not (Test-Path $ScriptPath)) {
    Write-Log "Keyboard capture script not found: $ScriptPath" 'WARN'
    return @{ started = $false; status = 'not_tested' }
  }

  $isInteractive = $true
  try { $isInteractive = [Environment]::UserInteractive } catch { }
  if (-not $isInteractive) {
    Write-Log 'Keyboard capture skipped (non-interactive session).' 'WARN'
    return @{ started = $false; status = 'not_tested' }
  }

  $psCmd = Get-Command powershell.exe -ErrorAction SilentlyContinue
  if (-not $psCmd) {
    Write-Log 'powershell.exe not found for keyboard capture' 'WARN'
    return @{ started = $false; status = 'not_tested' }
  }

  if (-not $LogPath) {
    $LogPath = Join-Path $env:TEMP ("mdt-keyboard-{0}.jsonl" -f [guid]::NewGuid().ToString('N'))
  }

  $args = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-STA', '-NoLogo', '-File', $ScriptPath)
  if ($LogPath) { $args += @('-LogPath', $LogPath) }
  if ($ConfigDir) { $args += @('-ConfigDir', $ConfigDir) }
  if ($Layout) { $args += @('-Layout', $Layout) }
  if ($LayoutConfig) { $args += @('-LayoutConfig', $LayoutConfig) }
  if ($BlockInput) { $args += '-BlockInput' }

  try {
    $workingDir = Split-Path $ScriptPath -Parent
    $proc = Start-Process -FilePath $psCmd.Source -ArgumentList $args -WindowStyle Normal -WorkingDirectory $workingDir -PassThru
    if (-not $proc) {
      Write-Log 'Keyboard capture failed to start.' 'WARN'
      return @{ started = $false; status = 'not_tested' }
    }
    try {
      $null = $proc.WaitForInputIdle(5000)
      $wshell = New-Object -ComObject WScript.Shell
      $null = $wshell.AppActivate($proc.Id)
    } catch {
      Write-Log "Keyboard capture focus hint failed: $($_.Exception.Message)" 'WARN'
    }
    Write-Log "Keyboard capture started (PID=$($proc.Id))."
    return @{
      started = $true
      process = $proc
      timeoutSec = $TimeoutSec
      startedAt = Get-Date
      logPath = $LogPath
    }
  } catch {
    Write-Log "Keyboard capture launch failed: $($_.Exception.Message)" 'WARN'
    return @{ started = $false; status = 'not_tested' }
  }
}

function Complete-KeyboardCapture {
  param([hashtable]$State)

  if (-not $State -or -not $State.started -or -not $State.process) {
    return @{ status = 'not_tested'; exitCode = $null }
  }

  $proc = $State.process
  $timeoutSec = if ($State.timeoutSec -ne $null) { [int]$State.timeoutSec } else { 0 }
  $elapsed = 0
  if ($State.startedAt) {
    $elapsed = [int]([DateTime]::Now - $State.startedAt).TotalSeconds
  }
  $remaining = if ($timeoutSec -gt 0) { [math]::Max(0, $timeoutSec - $elapsed) } else { 0 }

  try {
    if ($timeoutSec -gt 0) {
      if (-not $proc.WaitForExit($remaining * 1000)) {
        Write-Log "Keyboard capture timed out after ${timeoutSec}s." 'WARN'
        try { $proc.Kill() } catch { }
        $logResult = Get-KeyboardResultFromLog -Path $State.logPath
        if ($logResult -and $logResult.status) {
          Write-Log "Keyboard capture status recovered from log: $($logResult.status)"
          return @{ status = $logResult.status; padStatus = $logResult.padStatus; exitCode = $null; timedOut = $true; fromLog = $true }
        }
        return @{ status = 'not_tested'; exitCode = $null; timedOut = $true }
      }
    } else {
      $proc.WaitForExit()
    }
    $exitCode = $proc.ExitCode
    $status = switch ($exitCode) {
      0 { 'ok' }
      1 { 'nok' }
      2 { 'not_tested' }
      default { 'not_tested' }
    }
    Write-Log "Keyboard capture finished: status=$status exitCode=$exitCode"
    $logResult = Get-KeyboardResultFromLog -Path $State.logPath
    if ($status -ne 'ok' -and $status -ne 'nok' -and $logResult -and $logResult.status) {
      Write-Log "Keyboard capture status recovered from log: $($logResult.status)"
      return @{ status = $logResult.status; padStatus = $logResult.padStatus; exitCode = $exitCode; fromLog = $true }
    }
    return @{ status = $status; padStatus = $logResult.padStatus; exitCode = $exitCode }
  } catch {
    Write-Log "Keyboard capture wait failed: $($_.Exception.Message)" 'WARN'
    $logResult = Get-KeyboardResultFromLog -Path $State.logPath
    if ($logResult -and $logResult.status) {
      Write-Log "Keyboard capture status recovered from log: $($logResult.status)"
      return @{ status = $logResult.status; padStatus = $logResult.padStatus; exitCode = $null; fromLog = $true }
    }
    return @{ status = 'not_tested'; exitCode = $null }
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

$script:IsWinPE = $false
try {
  if ($env:SystemDrive -eq 'X:' -or $env:WinPE -eq 'Yes') {
    $script:IsWinPE = $true
  }
} catch { }

$script:IsAdmin = Test-IsAdmin
$script:IsInteractive = $true
try { $script:IsInteractive = [Environment]::UserInteractive } catch { }
if (-not $script:IsAdmin -and -not $SkipElevation) {
  if ($script:IsWinPE) {
    Write-Log 'WinPE detected; skipping elevation prompt.' 'WARN'
  } elseif (-not $script:IsInteractive) {
    Write-Log 'Non-interactive session; skipping elevation prompt.' 'WARN'
  } else {
    Write-Log 'Elevation required, requesting admin rights...' 'WARN'
    $scriptPath = $MyInvocation.MyCommand.Path
    $elevatedScriptPath = $scriptPath
    $forceLogPath = $false
    if ($scriptPath -and $scriptPath.StartsWith('\\')) {
      Write-Log 'Script launched from a network path; staging local copy for elevation.' 'WARN'
      $localCopy = Copy-ScriptsForElevation -ScriptPath $scriptPath
      if ($localCopy) {
        $elevatedScriptPath = $localCopy
        if (-not $PSBoundParameters.ContainsKey('LogPath')) {
          $forceLogPath = $true
          $localLogPath = Join-Path (Split-Path $elevatedScriptPath -Parent) 'mdt-report.log'
          Write-Log "Elevated run will log locally: $localLogPath" 'WARN'
        }
      } else {
        Write-Log 'Local staging failed; elevation may fail due to UNC access.' 'WARN'
      }
    }
    $psPath = $null
    $psCmd = Get-Command powershell.exe -ErrorAction SilentlyContinue
    if ($psCmd) { $psPath = $psCmd.Source }
    if (-not $psPath) {
      $psPath = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    }

    if (-not $psPath -or -not (Test-Path $psPath)) {
      Write-Log 'Unable to resolve PowerShell path for elevation.' 'ERROR'
    } elseif (-not $elevatedScriptPath) {
      Write-Log 'Unable to resolve script path for elevation.' 'ERROR'
    } else {
      $argList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $elevatedScriptPath)
      $argList += Build-ArgumentList -BoundParams $PSBoundParameters
      if ($forceLogPath -and $localLogPath) { $argList += @('-LogPath', $localLogPath) }
      $argList += '-SkipElevation'
      try {
        $proc = Start-Process -FilePath $psPath -ArgumentList $argList -Verb RunAs -PassThru
        if ($proc) {
          Write-Log "Elevation launched (PID=$($proc.Id)). Exiting current session."
          exit 0
        }
        Write-Log 'Elevation did not start; continuing without admin rights.' 'WARN'
      } catch {
        Write-Log "Elevation cancelled or failed: $($_.Exception.Message)" 'WARN'
      }
    }
  }
  Write-Log 'Continuing without admin rights.' 'WARN'
}

$clientRunId = [guid]::NewGuid().ToString('D')
$artifactRootResolved = Resolve-ArtifactRoot -Value $ArtifactRoot
$artifactRunDir = $null
$macSerialKey = $null
$rawUploadResult = $null

Set-ConsoleFullscreen
Write-Log "Start script version $scriptVersion"
Write-Log "ApiUrl=$ApiUrl Category=$Category TestMode=$TestMode"
Step-Progress -Status 'Initialisation'

$winsatRoots = @()
if (-not $SkipWinSatDataStore) {
  if ($WinSatDataStorePath) { $winsatRoots += $WinSatDataStorePath }
  if ($env:SystemRoot) {
    try {
      $winsatDefault = Join-Path $env:SystemRoot 'Performance\\WinSAT\\DataStore'
      if ($winsatDefault -and ($winsatRoots -notcontains $winsatDefault)) { $winsatRoots += $winsatDefault }
    } catch { }
    try {
      $winsatAlt = Join-Path $env:SystemRoot 'Performance\\DataStore'
      if ($winsatAlt -and ($winsatRoots -notcontains $winsatAlt)) { $winsatRoots += $winsatAlt }
    } catch { }
  }
  if ($winsatRoots -notcontains $PSScriptRoot) { $winsatRoots += $PSScriptRoot }
}

$winsatStore = $null
if (-not $SkipWinSatDataStore -and $winsatRoots.Count -gt 0) {
  $winsatStore = Get-WinsatFromDataStore -Paths $winsatRoots
}

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
      DxDiagTimeoutSec = $DxDiagTimeoutSec
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
      ArtifactRoot = $ArtifactRoot
      ObjectStorageEndpoint = $ObjectStorageEndpoint
      ObjectStorageBucket = $ObjectStorageBucket
      ObjectStorageAccessKey = $ObjectStorageAccessKey
      ObjectStorageSecretKey = $ObjectStorageSecretKey
      ObjectStoragePrefix = $ObjectStoragePrefix
      ObjectStorageMcPath = $ObjectStorageMcPath
    }
    if ($SkipTlsValidation) { $delegateParams.SkipTlsValidation = $true }
    if ($SkipWinSatDataStore) { $delegateParams.SkipWinSatDataStore = $true }
    if ($SkipGpuAssessment) { $delegateParams.SkipGpuAssessment = $true }
    if ($SkipRawUpload) { $delegateParams.SkipRawUpload = $true }
  & $stressScriptPath @delegateParams
  Complete-Progress
  Invoke-FactoryResetPrompt -ForceReset:$true -ConfirmToken 'RESET'
  exit $LASTEXITCODE
  } else {
    Write-Log "Stress script not found: $stressScriptPath" 'WARN'
  }
}

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
  param(
    [int]$TimeoutSec = 30,
    [string]$OutputDir
  )

  if ($TimeoutSec -le 0) { return @() }

  $cmd = Get-Command msinfo32 -ErrorAction SilentlyContinue
  if (-not $cmd) { return @() }

  if ($OutputDir) {
    $OutputDir = Ensure-Directory -Path $OutputDir
  }
  if ($OutputDir) {
    $reportPath = Join-Path $OutputDir 'msinfo.txt'
  } else {
    $reportPath = Join-Path ([System.IO.Path]::GetTempPath()) ("mdt-msinfo-{0}.txt" -f [guid]::NewGuid().ToString('N'))
  }
  $script:MsinfoReportPath = $reportPath
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
    [string]$WifiPattern,
    [string]$CellularPattern
  )

  $label = (($Name, $Description, $MediaType, $PhysicalMediaType) -join ' ').Trim()
  $isCellular = $false
  if ($CellularPattern) { $isCellular = ($label -match $CellularPattern) }
  return [pscustomobject]@{
    Mac = $Mac
    Name = $Name
    Description = $Description
    IsUp = $IsUp
    IsCellular = $isCellular
    IsEthernet = (($label -match $EthernetPattern) -and -not $isCellular)
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
    $type = if ($_.IsEthernet) { 'eth' } elseif ($_.IsWifi) { 'wifi' } elseif ($_.IsCellular) { 'cell' } else { 'other' }
    $state = if ($_.IsUp) { 'up' } else { 'down' }
    "$($_.Mac) [$type,$state,$($_.Name)]"
  }
  Write-Log ("MAC candidates from {0}: {1}" -f $Source, ($lines -join ' | '))
}

function Get-PrimaryMac {
  $skipPattern = 'Virtual|VPN|Loopback|Bluetooth|Wi-Fi Direct|TAP|Hyper-V|Pseudo|WAN Miniport|RAS'
  $ethernetPattern = '(?i)ethernet|lan|gigabit|gbe|realtek|intel|broadcom|pci'
  $wifiPattern = '(?i)wi-?fi|wireless|802\.11|wlan'
  $cellularPattern = '(?i)cellular|cellulaire|wwan|mobile|lte|5g|4g|broadband|modem'

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
          -WifiPattern $wifiPattern `
          -CellularPattern $cellularPattern
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
      -WifiPattern $wifiPattern `
      -CellularPattern $cellularPattern
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
      -WifiPattern $wifiPattern `
      -CellularPattern $cellularPattern
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

function Pick-MacCandidate {
  param([array]$Candidates)

  if (-not $Candidates -or $Candidates.Count -eq 0) { return $null }
  $up = $Candidates | Where-Object { $_.IsUp }
  $selected = if ($up -and $up.Count -gt 0) { $up | Select-Object -First 1 } else { $Candidates | Select-Object -First 1 }
  if ($selected -and $selected.Mac) { return $selected.Mac }
  return $null
}

function Get-PreferredMacs {
  param([string]$PrimaryMac)

  $skipPattern = 'Virtual|VPN|Loopback|Bluetooth|Wi-Fi Direct|TAP|Hyper-V|Pseudo|WAN Miniport|RAS'
  $ethernetPattern = '(?i)ethernet|lan|gigabit|gbe|realtek|intel|broadcom|pci'
  $wifiPattern = '(?i)wi-?fi|wireless|802\.11|wlan'
  $cellularPattern = '(?i)cellular|cellulaire|wwan|mobile|lte|5g|4g|broadband|modem'
  $macs = New-Object System.Collections.Generic.List[string]

  function Add-UniqueMac {
    param([string]$Value)
    $normalized = Normalize-MacAddress $Value
    if ($normalized -and -not $macs.Contains($normalized)) { [void]$macs.Add($normalized) }
  }

  if ($PrimaryMac) { Add-UniqueMac $PrimaryMac }

  $candidates = @()
  if (Get-Command Get-NetAdapter -ErrorAction SilentlyContinue) {
    try {
      $netAdapters = Get-NetAdapter -ErrorAction Stop | Where-Object {
        $_.MacAddress -and ($_.HardwareInterface -eq $true -or $_.Virtual -eq $false)
      }
      foreach ($adapter in $netAdapters) {
        $label = (($adapter.Name, $adapter.InterfaceDescription, $adapter.MediaType, $adapter.PhysicalMediaType) -join ' ').Trim()
        if ($label -match $skipPattern) { continue }
        $candidates += New-MacCandidate `
          -Mac $adapter.MacAddress `
          -Name $adapter.Name `
          -Description $adapter.InterfaceDescription `
          -MediaType $adapter.MediaType `
          -PhysicalMediaType $adapter.PhysicalMediaType `
          -IsUp ($adapter.Status -eq 'Up') `
          -EthernetPattern $ethernetPattern `
          -WifiPattern $wifiPattern `
          -CellularPattern $cellularPattern
      }
    } catch {
      Write-Log "Get-NetAdapter failed: $($_.Exception.Message)" 'WARN'
    }
  }

  if (-not $candidates -or $candidates.Count -eq 0) {
    $configs = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapterConfiguration'
    foreach ($cfg in $configs) {
      if (-not $cfg.MACAddress) { continue }
      if ($cfg.Description -and $cfg.Description -match $skipPattern) { continue }
      $candidates += New-MacCandidate `
        -Mac $cfg.MACAddress `
        -Name $cfg.Caption `
        -Description $cfg.Description `
        -MediaType $null `
        -PhysicalMediaType $null `
        -IsUp ($cfg.IPEnabled -eq $true) `
        -EthernetPattern $ethernetPattern `
        -WifiPattern $wifiPattern `
        -CellularPattern $cellularPattern
    }
  }

  if (-not $candidates -or $candidates.Count -eq 0) {
    $adapters = Get-CimInstanceSafe -ClassName 'Win32_NetworkAdapter'
    foreach ($adapter in $adapters) {
      if (-not $adapter.MACAddress) { continue }
      if ($adapter.Description -and $adapter.Description -match $skipPattern) { continue }
      if ($adapter.PhysicalAdapter -ne $true -and ($adapter.PNPDeviceID -and $adapter.PNPDeviceID -match '^ROOT\\')) { continue }
      $candidates += New-MacCandidate `
        -Mac $adapter.MACAddress `
        -Name $adapter.NetConnectionID `
        -Description $adapter.Description `
        -MediaType $null `
        -PhysicalMediaType $null `
        -IsUp ($adapter.NetConnectionStatus -eq 2) `
        -EthernetPattern $ethernetPattern `
        -WifiPattern $wifiPattern `
        -CellularPattern $cellularPattern
    }
  }

  if ($candidates -and $candidates.Count -gt 0) {
    $ethernetMac = Pick-MacCandidate -Candidates ($candidates | Where-Object { $_.IsEthernet })
    $wifiMac = Pick-MacCandidate -Candidates ($candidates | Where-Object { $_.IsWifi })
    $cellMac = Pick-MacCandidate -Candidates ($candidates | Where-Object { $_.IsCellular })
    Add-UniqueMac $ethernetMac
    Add-UniqueMac $wifiMac
    if ($macs.Count -lt 2) { Add-UniqueMac $cellMac }
  }

  if ($macs.Count -lt 2) {
    foreach ($value in (Get-AllMacs)) {
      Add-UniqueMac $value
      if ($macs.Count -ge 2) { break }
    }
  }

  if ($macs.Count -gt 2) { return $macs[0..1] }
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

function Invoke-BatteryReport {
  param(
    [string]$OutputDir
  )

  $result = [ordered]@{
    status = 'not_tested'
    reportPath = $null
  }

  $cmd = Get-Command powercfg -ErrorAction SilentlyContinue
  if (-not $cmd) {
    $result.status = 'absent'
    return $result
  }

  if (-not $OutputDir) {
    if ($env:TEMP) {
      $OutputDir = Join-Path $env:TEMP 'mdt-fusion\\battery'
    } else {
      $OutputDir = Join-Path $PSScriptRoot 'battery'
    }
  }
  $OutputDir = Ensure-Directory -Path $OutputDir
  if (-not $OutputDir) {
    $result.status = 'nok'
    return $result
  }

  $reportPath = Join-Path $OutputDir 'battery-report.html'
  $result.reportPath = $reportPath

  try {
    & $cmd.Source /batteryreport /output $reportPath | Out-Null
    if (Test-Path $reportPath) {
      $result.status = 'ok'
    } else {
      $result.status = 'nok'
    }
  } catch {
    Write-Log "Battery report failed: $($_.Exception.Message)" 'WARN'
    $result.status = 'nok'
  }

  return $result
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
    [int]$TimeoutSec,
    [string]$OutputXmlPath
  )

  $cmd = Get-Command winsat -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent'; mbps = $null } }
  $argList = @()
  if ($Arguments) { $argList += $Arguments }
  if ($OutputXmlPath) { $argList += @('-xml', ('"' + $OutputXmlPath + '"')) }
  $argString = ($argList -join ' ')
  $exitCode = 1
  $proc = $null
  try {
    $proc = Start-Process -FilePath $cmd.Source -ArgumentList $argString -NoNewWindow -PassThru
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

  $exitCode = if ($proc) { $proc.ExitCode } else { 1 }

  $status = $null
  if ($OutputXmlPath -and (Test-Path $OutputXmlPath)) {
    $status = 'ok'
  } else {
    $status = if ($exitCode -eq 0) { 'ok' } else { 'nok' }
  }
  if ($status -ne 'ok') {
    Write-Log "WinSAT exit code $exitCode" 'WARN'
  }

  return @{ status = $status; mbps = $null }
}

function Invoke-WinsatAssessmentWithXml {
  param(
    [string]$AssessmentKey,
    [string[]]$Arguments,
    [int]$TimeoutSec
  )

  $safeKey = if ($AssessmentKey) { [regex]::Replace($AssessmentKey, '[^A-Za-z0-9_-]', '_') } else { 'assessment' }
  $runToken = if ($clientRunId) { $clientRunId } else { [guid]::NewGuid().ToString() }
  $outputDir = $null
  if ($script:WinSatLogDir) {
    $outputDir = $script:WinSatLogDir
  } elseif ($env:TEMP) {
    $outputDir = Join-Path $env:TEMP 'mdt-fusion\\winsat'
  } else {
    $outputDir = $PSScriptRoot
  }

  $outputXmlPath = $null
  if ($outputDir) {
    try {
      Ensure-Directory -Path $outputDir | Out-Null
      $outputXmlPath = Join-Path $outputDir ("{0}.Assessment.{1}.WinSAT.xml" -f $safeKey, $runToken)
    } catch {
      $outputXmlPath = $null
    }
  }

  $capture = Invoke-WinsatCapture -Arguments $Arguments -TimeoutSec $TimeoutSec -OutputXmlPath $outputXmlPath
  $parsed = $null
  if ($outputXmlPath -and (Test-Path -Path $outputXmlPath)) {
    $parsed = Get-WinsatFromXmlFile -Path $outputXmlPath
  }

  return [ordered]@{
    status = if ($capture) { $capture.status } else { $null }
    xmlPath = $outputXmlPath
    winsat = $parsed
  }
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

function Run-WinsatCpuLoop {
  param(
    [int]$Loops,
    [int]$TimeoutSec
  )

  if ($Loops -le 0) { return $null }
  Write-Log 'WinSAT CPU run: encryption'
  $primary = Run-WinsatLoop -Arguments @('cpu', '-encryption') -Loops $Loops -TimeoutSec $TimeoutSec
  if ($primary -and $primary.status -eq 'ok') { return $primary }
  Write-Log 'WinSAT CPU run: compression'
  $secondary = Run-WinsatLoop -Arguments @('cpu', '-compression') -Loops $Loops -TimeoutSec $TimeoutSec
  if ($secondary) { return $secondary }
  return $primary
}

function Invoke-WinsatScore {
  param(
    [string[]]$Arguments,
    [int]$TimeoutSec
  )

  $cmd = Get-Command winsat -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent'; score = $null } }
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

function Get-WinSatNote {
  param([double]$Score)

  if ($Score -eq $null) { return $null }
  if ([double]::IsNaN($Score) -or [double]::IsInfinity($Score)) { return $null }

  if ($Score -lt 3.0) { return 'Horrible' }
  if ($Score -lt 4.5) { return 'Mauvais' }
  if ($Score -lt 6.0) { return 'Moyen' }
  if ($Score -lt 7.5) { return 'Bon' }
  return 'Excellent'
}

function Get-FeatureRank {
  param([string]$Feature)

  if (-not $Feature) { return 0 }
  $m = [regex]::Match($Feature, '(\d+)[\._]?(\d+)?')
  if (-not $m.Success) { return 0 }
  $maj = [int]$m.Groups[1].Value
  $min = 0
  if ($m.Groups[2].Success) { $min = [int]$m.Groups[2].Value }
  return $maj + ($min / 10)
}

function Read-DxDiagInfo {
  param([string]$Path)

  $info = [ordered]@{
    hasDxDiag = $false
    gpuName = $null
    featureLevels = @()
  }

  if (-not $Path -or -not (Test-Path -Path $Path)) { return $info }
  $content = Get-Content -Path $Path -Raw -ErrorAction SilentlyContinue
  if (-not $content) { return $info }

  $info.hasDxDiag = $true
  $lines = $content -split "`n"
  $gpuNameLine = $lines | Where-Object { $_ -match 'Card name:' -or $_ -match 'Device:' } | Select-Object -First 1
  if ($gpuNameLine -match ':\s*(.+)') { $info.gpuName = $Matches[1].Trim() }
  $featLine = $lines | Where-Object { $_ -match 'Feature Levels' } | Select-Object -First 1
  if ($featLine -match 'Feature Levels:\s*(.+)') {
    $rawLevels = $Matches[1] -split ','
    $info.featureLevels = $rawLevels | ForEach-Object { $_.Trim().TrimStart('D','3','D','_') } | Where-Object { $_ }
  }

  return $info
}

function Invoke-DxDiagCapture {
  param(
    [string]$OutputDir,
    [int]$TimeoutSec = 300
  )

  $result = [ordered]@{
    status = 'not_tested'
    txtPath = $null
    logPath = $null
  }

  $dxdiag = $null
  if ($env:SystemRoot) {
    $dxdiag = Join-Path $env:SystemRoot 'System32\\dxdiag.exe'
  }
  if (-not $dxdiag -or -not (Test-Path -Path $dxdiag)) {
    $result.status = 'absent'
    return $result
  }

  if (-not $OutputDir) {
    if ($env:TEMP) {
      $OutputDir = Join-Path $env:TEMP 'mdt-fusion\\dxdiag'
    } else {
      $OutputDir = Join-Path $PSScriptRoot 'dxdiag'
    }
  }
  $OutputDir = Ensure-Directory -Path $OutputDir
  if (-not $OutputDir) {
    $result.status = 'nok'
    return $result
  }
  if (-not $TimeoutSec -or $TimeoutSec -le 0) { $TimeoutSec = 300 }

  $txtPath = Join-Path $OutputDir 'dxdiag.txt'
  $logPath = Join-Path $OutputDir 'dxdiag.log'
  $result.txtPath = $txtPath
  $result.logPath = $logPath

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $dxdiag
  $psi.Arguments = "/t `"$txtPath`""
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.UseShellExecute = $false
  $psi.WorkingDirectory = $OutputDir

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  try {
    [void]$proc.Start()
  } catch {
    Write-Log "DxDiag start failed: $($_.Exception.Message)" 'WARN'
    $result.status = 'nok'
    return $result
  }

  $completed = $false
  try {
    $completed = $proc.WaitForExit($TimeoutSec * 1000)
  } catch {
    Write-Log "DxDiag wait failed: $($_.Exception.Message)" 'WARN'
  }

  if (-not $completed) {
    try { $proc.Kill() } catch { }
    $result.status = 'timeout'
  } else {
    $result.status = if ($proc.ExitCode -eq 0) { 'ok' } else { 'nok' }
  }

  $output = $proc.StandardOutput.ReadToEnd() + "`n" + $proc.StandardError.ReadToEnd()
  if ($output) {
    try { [IO.File]::WriteAllText($logPath, $output) } catch { }
  }

  if (-not (Test-Path -Path $txtPath)) {
    Set-Content -Path $txtPath -Value 'DxDiag produced no output' -Force -Encoding UTF8
  }
  if (-not (Test-Path -Path $logPath)) {
    Set-Content -Path $logPath -Value 'DxDiag produced no log output' -Force -Encoding UTF8
  }

  return $result
}

function Get-GpuAssessment {
  param(
    [string]$DxDiagPath,
    [hashtable]$WinSatStore,
    [double]$GpuScore,
    [string]$GpuNameFallback
  )

  $dxInfo = Read-DxDiagInfo -Path $DxDiagPath
  $featureLevels = @()
  if ($dxInfo.featureLevels) { $featureLevels = @($dxInfo.featureLevels) }

  $gpuName = $dxInfo.gpuName
  if (-not $gpuName -and $GpuNameFallback) { $gpuName = $GpuNameFallback }

  $graphicsScore = $null
  $gamingScore = $null
  $cpuScore = $null
  $memScore = $null
  $rawScores = $null
  $hasWinSat = $false
  $winSatParseOk = $false
  $winSatLogTail = ''

  if ($WinSatStore -and $WinSatStore.winSPR) {
    $hasWinSat = $true
    $winSatParseOk = $true
    $rawScores = @{
      Graphics = $WinSatStore.winSPR.GraphicsScore
      Gaming = $WinSatStore.winSPR.GamingScore
      CPU = $WinSatStore.winSPR.CpuScore
      Memory = $WinSatStore.winSPR.MemoryScore
    }
    $graphicsScore = $WinSatStore.winSPR.GraphicsScore
    $gamingScore = $WinSatStore.winSPR.GamingScore
    $cpuScore = $WinSatStore.winSPR.CpuScore
    $memScore = $WinSatStore.winSPR.MemoryScore
  }

  if ($graphicsScore -eq $null -and $GpuScore -ne $null) {
    $graphicsScore = [double]$GpuScore
  }
  if (-not $rawScores -and $graphicsScore -ne $null) {
    $rawScores = @{ Graphics = $graphicsScore }
  }
  if (-not $hasWinSat -and $graphicsScore -ne $null) { $hasWinSat = $true }

  $maxFeature = $null
  $maxRank = 0
  foreach ($f in $featureLevels) {
    $r = Get-FeatureRank -Feature $f
    if ($r -gt $maxRank) { $maxRank = $r; $maxFeature = $f }
  }

  $category = 'unknown'
  if ($graphicsScore -ne $null) {
    $g = [double]$graphicsScore
    if ($maxRank -ge 12.0 -and $g -ge 4.5) {
      $category = 'igpu_ok'
    } elseif ($maxRank -ge 11.0 -and $g -ge 3.5) {
      $category = 'igpu_moyen'
    } else {
      $category = 'igpu_bas'
    }
  } elseif ($maxRank -ge 11.0) {
    $category = 'igpu_moyen'
  } elseif ($maxRank -gt 0) {
    $category = 'igpu_bas'
  }

  if ($category -ne 'igpu_ok' -and $cpuScore -and $memScore) {
    $cpuVal = [double]$cpuScore
    $memVal = [double]$memScore
    if ($cpuVal -ge 8.5 -and $memVal -ge 8.5) {
      if ($category -eq 'igpu_bas') { $category = 'igpu_moyen' }
      elseif ($category -eq 'igpu_moyen') { $category = 'igpu_ok' }
    }
  }

  return [ordered]@{
    gpu_name = $gpuName
    feature_levels = $featureLevels
    max_feature = $maxFeature
    graphics_score = $graphicsScore
    gaming_score = $gamingScore
    cpu_score = $cpuScore
    memory_score = $memScore
    has_dxdiag = $dxInfo.hasDxDiag
    has_winsat = $hasWinSat
    winsat_parse_ok = $winSatParseOk
    winsat_raw = $rawScores
    winsat_log_tail = $winSatLogTail
    category = $category
  }
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

function Get-WifiStandardCode {
  param([object]$Value)

  if ($null -eq $Value) { return $null }
  $raw = [string]$Value
  if (-not $raw) { return $null }
  $cleaned = $raw.ToLowerInvariant()

  $matches = [regex]::Matches($cleaned, '(?i)(?:802[\.\-\s]*11|11)\s*(be|ax|ac|n|g|a|b)\b')
  if ($matches.Count -gt 0) {
    $suffix = $matches[0].Groups[1].Value.ToLowerInvariant()
    if ($suffix) { return "802.11$suffix" }
  }

  if ($cleaned -match '\bwi-?fi\s*7\b') { return '802.11be' }
  if ($cleaned -match '\bwi-?fi\s*6(?:e)?\b') { return '802.11ax' }
  if ($cleaned -match '\bwi-?fi\s*5\b') { return '802.11ac' }
  if ($cleaned -match '\bwi-?fi\s*4\b') { return '802.11n' }
  if ($cleaned -match '\bwi-?fi\s*3\b') { return '802.11g' }
  if ($cleaned -match '\bwi-?fi\s*2\b') { return '802.11a' }
  if ($cleaned -match '\bwi-?fi\s*1\b') { return '802.11b' }
  return $null
}

function Get-WifiStandardRank {
  param([string]$Standard)

  switch ($Standard) {
    '802.11be' { return 7 }
    '802.11ax' { return 6 }
    '802.11ac' { return 5 }
    '802.11n' { return 4 }
    '802.11g' { return 3 }
    '802.11a' { return 2 }
    '802.11b' { return 1 }
    default { return 0 }
  }
}

function Get-WifiStandardCandidatesFromText {
  param([string]$Text)

  $candidates = @()
  if (-not $Text) { return $candidates }

  $linePatterns = @(
    '(?im)^\s*(?:radio\s*type|type\s+de\s+radio)\s*:\s*(?<value>.+)$',
    '(?im)^\s*(?:radio\s*types?\s*supported|types?\s+de\s+radio\s+pris\s+en\s+charge)\s*:\s*(?<value>.+)$'
  )
  foreach ($pattern in $linePatterns) {
    $matches = [regex]::Matches($Text, $pattern)
    foreach ($match in $matches) {
      $lineValue = $match.Groups['value'].Value
      if (-not $lineValue) { continue }
      $standardMatches = [regex]::Matches($lineValue, '(?i)(?:802[\.\-\s]*11|11)\s*(be|ax|ac|n|g|a|b)\b')
      foreach ($standardMatch in $standardMatches) {
        $candidate = "802.11$($standardMatch.Groups[1].Value.ToLowerInvariant())"
        if ($candidate -and ($candidates -notcontains $candidate)) {
          $candidates += $candidate
        }
      }
      $singleCandidate = Get-WifiStandardCode -Value $lineValue
      if ($singleCandidate -and ($candidates -notcontains $singleCandidate)) {
        $candidates += $singleCandidate
      }
    }
  }

  if (-not $candidates -or $candidates.Count -eq 0) {
    $allMatches = [regex]::Matches($Text, '(?i)(?:802[\.\-\s]*11|11)\s*(be|ax|ac|n|g|a|b)\b')
    foreach ($standardMatch in $allMatches) {
      $candidate = "802.11$($standardMatch.Groups[1].Value.ToLowerInvariant())"
      if ($candidate -and ($candidates -notcontains $candidate)) {
        $candidates += $candidate
      }
    }
    $fallback = Get-WifiStandardCode -Value $Text
    if ($fallback -and ($candidates -notcontains $fallback)) {
      $candidates += $fallback
    }
  }

  return $candidates
}

function Get-WifiStandardStatus {
  $result = [ordered]@{
    status = 'not_tested'
    standard = $null
    standards = @()
    source = 'netsh_interfaces'
  }

  $netsh = Get-Command netsh -ErrorAction SilentlyContinue
  if (-not $netsh) {
    $result.status = 'absent'
    $result.source = 'command_missing'
    return $result
  }

  $output = $null
  try {
    $output = (& $netsh.Source wlan show interfaces 2>&1 | Out-String).Trim()
  } catch {
    Write-Log "Wi-Fi detection failed: $($_.Exception.Message)" 'WARN'
    return $result
  }
  if (-not $output) {
    $result.source = 'empty_output'
    return $result
  }

  $candidates = Get-WifiStandardCandidatesFromText -Text $output

  if (-not $candidates -or $candidates.Count -eq 0) {
    $driversOutput = $null
    try {
      $driversOutput = (& $netsh.Source wlan show drivers 2>&1 | Out-String).Trim()
    } catch {
      Write-Log "Wi-Fi driver fallback failed: $($_.Exception.Message)" 'WARN'
    }
    if ($driversOutput) {
      $driverCandidates = Get-WifiStandardCandidatesFromText -Text $driversOutput
      if ($driverCandidates -and $driverCandidates.Count -gt 0) {
        $candidates = $driverCandidates
        $result.source = 'netsh_drivers'
      } else {
        $output = "$output`n$driversOutput"
      }
    }
  }

  if (-not $candidates -or $candidates.Count -eq 0) {
    $normalizedOutput = $output.ToLowerInvariant()
    if (
      $normalizedOutput -match 'no wireless interface' -or
      $normalizedOutput -match 'there is no wireless interface' -or
      $normalizedOutput -match 'aucune interface sans fil' -or
      $normalizedOutput -match 'aucune interface reseau sans fil'
    ) {
      $result.status = 'nok'
    }
    return $result
  }

  $bestStandard = $null
  $bestRank = 0
  foreach ($candidate in $candidates) {
    $rank = Get-WifiStandardRank -Standard $candidate
    if ($rank -gt $bestRank) {
      $bestRank = $rank
      $bestStandard = $candidate
    }
  }
  if (-not $bestStandard) {
    $bestStandard = $candidates[0]
  }

  $result.standards = $candidates
  $result.standard = $bestStandard
  if ($bestStandard -in @('802.11be', '802.11ax', '802.11ac', '802.11n')) {
    $result.status = 'ok'
  } elseif ($bestStandard -in @('802.11g', '802.11a', '802.11b')) {
    $result.status = 'nok'
  } else {
    $result.status = 'not_tested'
  }
  return $result
}

function Get-AutopilotHardwareHash {
  $result = [ordered]@{
    status = 'not_tested'
    value = $null
    source = 'mdm_dmmap'
  }

  try {
    $queryPlans = @(
      @{ className = 'MDM_DevDetail_Ext01'; filter = "InstanceID='Ext' AND ParentID='./DevDetail'"; source = 'mdm_dmmap_filtered' },
      @{ className = 'MDM_DevDetail_Ext01'; filter = $null; source = 'mdm_dmmap' },
      @{ className = 'MDM_DevDetailExt01'; filter = "InstanceID='Ext' AND ParentID='./DevDetail'"; source = 'mdm_dmmap_alt_filtered' },
      @{ className = 'MDM_DevDetailExt01'; filter = $null; source = 'mdm_dmmap_alt' }
    )

    foreach ($plan in $queryPlans) {
      $records = Get-CimInstanceSafe -Namespace 'root/cimv2/mdm/dmmap' -ClassName $plan.className -Filter $plan.filter
      if (-not $records -or $records.Count -eq 0) {
        continue
      }
      foreach ($record in $records) {
        if (-not $record) { continue }
        $rawValue = $null
        try {
          $rawValue = $record.DeviceHardwareData
        } catch {
          $rawValue = $null
        }
        if ($rawValue -eq $null) { continue }
        $cleanValue = ([string]$rawValue).Trim()
        if (-not $cleanValue) { continue }
        $result.status = 'ok'
        $result.value = ($cleanValue -replace '\s+', '')
        $result.source = $plan.source
        return $result
      }
    }

    $result.source = 'empty_value'
    return $result
  } catch {
    Write-Log "Autopilot hash query failed: $($_.Exception.Message)" 'WARN'
    $result.source = 'query_error'
    return $result
  }
}

function Get-BiosLanguageStatus {
  param([object]$BiosInfo)

  $result = [ordered]@{
    status = 'not_tested'
    value = $null
    source = $null
  }

  $candidates = @()
  if ($BiosInfo) {
    if ($BiosInfo.CurrentLanguage) {
      $candidates += [string]$BiosInfo.CurrentLanguage
    }
    if ($BiosInfo.ListOfLanguages) {
      foreach ($language in @($BiosInfo.ListOfLanguages)) {
        if ($language) { $candidates += [string]$language }
      }
    }
  }
  try {
    $culture = Get-Culture
    if ($culture -and $culture.Name) {
      $candidates += [string]$culture.Name
    }
  } catch { }

  foreach ($candidate in $candidates) {
    $raw = ([string]$candidate).Trim()
    if (-not $raw) { continue }
    $normalized = $raw.ToLowerInvariant()

    if (
      $normalized -match '(^|[^a-z])fr($|[^a-z])' -or
      $normalized -match 'fr[-_]?fr' -or
      $normalized -match 'french' -or
      $normalized -match 'francais' -or
      $normalized -match 'français'
    ) {
      $result.status = 'fr'
      $result.value = 'FR'
      $result.source = $raw
      return $result
    }
    if (
      $normalized -match '(^|[^a-z])en($|[^a-z])' -or
      $normalized -match 'en[-_]?us' -or
      $normalized -match 'en[-_]?gb' -or
      $normalized -match 'english' -or
      $normalized -match 'anglais'
    ) {
      $result.status = 'en'
      $result.value = 'EN'
      $result.source = $raw
      return $result
    }
  }

  return $result
}

function Get-BiosPasswordStatus {
  param([object]$ComputerSystem)

  $result = [ordered]@{
    status = 'not_tested'
    isSet = $null
    source = $null
  }

  if (-not $ComputerSystem) { return $result }

  $sources = @()
  $statusValues = @()
  if (
    $ComputerSystem.PSObject.Properties.Name -contains 'PowerOnPasswordStatus' -and
    $ComputerSystem.PowerOnPasswordStatus -ne $null
  ) {
    $statusValues += @{ key = 'PowerOnPasswordStatus'; value = $ComputerSystem.PowerOnPasswordStatus }
  }
  if (
    $ComputerSystem.PSObject.Properties.Name -contains 'AdminPasswordStatus' -and
    $ComputerSystem.AdminPasswordStatus -ne $null
  ) {
    $statusValues += @{ key = 'AdminPasswordStatus'; value = $ComputerSystem.AdminPasswordStatus }
  }

  $hasEnabled = $false
  $hasDisabled = $false
  foreach ($entry in $statusValues) {
    $numeric = $null
    if (-not [int]::TryParse([string]$entry.value, [ref]$numeric)) {
      continue
    }
    if ($numeric -eq 1) {
      $hasEnabled = $true
      $sources += "$($entry.key)=1"
    } elseif ($numeric -eq 0) {
      $hasDisabled = $true
      $sources += "$($entry.key)=0"
    } elseif ($numeric -eq 2 -or $numeric -eq 3) {
      $sources += "$($entry.key)=$numeric"
    }
  }

  if ($hasEnabled) {
    $result.status = 'nok'
    $result.isSet = $true
  } elseif ($hasDisabled) {
    $result.status = 'ok'
    $result.isSet = $false
  }
  if ($sources.Count -gt 0) {
    $result.source = $sources -join ', '
  }
  return $result
}

function Get-BiosBatteryStatus {
  $result = [ordered]@{
    status = 'not_tested'
    source = 'unknown'
    note = $null
  }

  $startTime = (Get-Date).AddDays(-30)
  try {
    $events = Get-WinEvent -FilterHashtable @{ LogName = 'System'; StartTime = $startTime } -MaxEvents 800 -ErrorAction Stop
    $alert = $events | Where-Object {
      ($_.LevelDisplayName -in @('Critical', 'Error', 'Warning')) -and
      $_.Message -and
      $_.Message -match '(?i)(cmos|rtc|bios)\s*(battery|pile)|(battery|pile)\s*(cmos|rtc|bios)'
    } | Select-Object -First 1
    if ($alert) {
      $result.status = 'nok'
      $result.source = 'eventlog'
      $result.note = 'cmos_or_rtc_alert_detected'
      return $result
    }
    $result.status = 'ok'
    $result.source = 'eventlog'
    $result.note = 'no_cmos_or_rtc_alert_last_30d'
    return $result
  } catch {
    Write-Log "BIOS battery check via event log failed: $($_.Exception.Message)" 'WARN'
  }

  $portableBattery = Get-CimInstanceSafe -ClassName 'Win32_Battery' | Select-Object -First 1
  if ($portableBattery) {
    $result.source = 'eventlog_unavailable'
    $result.note = 'portable_battery_detected_cmos_unknown'
  } else {
    $result.source = 'eventlog_unavailable'
    $result.note = 'no_cmos_signal'
  }
  return $result
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
    [int]$TimeoutSec,
    [ValidateSet('scan', 'dirty')][string]$Mode = 'scan'
  )

  if ($Mode -eq 'dirty') {
    $cmd = Get-Command fsutil -ErrorAction SilentlyContinue
    if (-not $cmd) { return @{ status = 'absent' } }
    $result = Invoke-ExternalCommand -Path $cmd.Source -Arguments @('dirty', 'query', $DriveLetter) -TimeoutSec 10 -Name 'fsutil dirty'
    if ($result.status -ne 'ok') { return @{ status = $result.status; output = $result.output } }
    $output = $result.output
    $outputLower = $output.ToLowerInvariant()
    if ($outputLower -match 'is dirty|est sale') {
      return @{ status = 'nok'; output = $output }
    }
    if ($outputLower -match 'is not dirty|n''est pas sale') {
      return @{ status = 'ok'; output = $output }
    }
    return @{ status = 'ok'; output = $output }
  }

  $cmd = Get-Command chkdsk -ErrorAction SilentlyContinue
  if (-not $cmd) { return @{ status = 'absent' } }
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
  $outputLower = $output.ToLowerInvariant()
  if ($outputLower -match 'access is denied|acc[eè]s refus[eé]|permission|administrateur|administrator|elevation') {
    return @{ status = 'denied'; output = $output }
  }
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

function Normalize-InventoryString {
  param([object]$Value)

  if ($Value -eq $null) { return $null }
  $text = [string]$Value
  $text = $text.Trim()
  if (-not $text) { return $null }
  return $text
}

function Get-BatteryInventory {
  $items = Get-CimInstanceSafe -ClassName 'Win32_Battery'
  if (-not $items) { return @() }
  $list = @()
  foreach ($item in $items) {
    $serial = Normalize-InventoryString $item.SerialNumber
    $deviceId = Normalize-InventoryString $item.DeviceID
    if ($serial -or $deviceId) {
      $list += [ordered]@{ serialNumber = $serial; deviceId = $deviceId }
    }
  }
  return $list
}

function Get-PhysicalMediaInventory {
  $items = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMedia'
  if (-not $items) { return @() }
  $list = @()
  foreach ($item in $items) {
    $tag = Normalize-InventoryString $item.Tag
    $serial = Normalize-InventoryString $item.SerialNumber
    if ($tag -or $serial) {
      $list += [ordered]@{ tag = $tag; serialNumber = $serial }
    }
  }
  return $list
}

function Get-MemorySerialInventory {
  param([object[]]$Modules)

  $items = $Modules
  if (-not $items) {
    $items = Get-CimInstanceSafe -ClassName 'Win32_PhysicalMemory'
  }
  if (-not $items) { return @() }
  $list = @()
  foreach ($module in $items) {
    $bank = Normalize-InventoryString $module.BankLabel
    $manufacturer = Normalize-InventoryString $module.Manufacturer
    $part = Normalize-InventoryString $module.PartNumber
    $serial = Normalize-InventoryString $module.SerialNumber
    if ($bank -or $manufacturer -or $part -or $serial) {
      $list += [ordered]@{
        bankLabel = $bank
        manufacturer = $manufacturer
        partNumber = $part
        serialNumber = $serial
      }
    }
  }
  return $list
}

function Get-BaseboardInventory {
  $items = Get-CimInstanceSafe -ClassName 'Win32_BaseBoard'
  if (-not $items) { return $null }
  $board = $items | Select-Object -First 1
  if (-not $board) { return $null }
  return [ordered]@{
    manufacturer = Normalize-InventoryString $board.Manufacturer
    product = Normalize-InventoryString $board.Product
    serialNumber = Normalize-InventoryString $board.SerialNumber
  }
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

$forceStress = -not $SkipStressScript
$testLoops = 0
$skipWinsatLive = $false
if (-not $forceStress -and $TestMode -eq 'quick' -and $winsatStore) {
  Write-Log 'WinSAT datastore present; keeping live WinSAT tests enabled.' 'INFO'
}
if ($forceStress) {
  $testLoops = [math]::Max($StressLoops, 2)
  Write-Log "Stress tests forced (loops=$testLoops)." 'INFO'
} elseif ($TestMode -eq 'quick' -and -not $skipWinsatLive) {
  $testLoops = 1
} elseif ($TestMode -eq 'stress') {
  $testLoops = [math]::Max($StressLoops, 2)
}

$categoryValue = if ($Category -eq 'auto') { Get-ChassisCategory } else { $Category }
$keyboardCaptureState = $null
if (-not $SkipKeyboardCapture -and $categoryValue -eq 'laptop') {
  $keyboardScript = Resolve-KeyboardCapturePath -Value $KeyboardCapturePath
  $layoutValue = if ($KeyboardCaptureLayout) { $KeyboardCaptureLayout } else { $null }
  $layoutConfigValue = if ($KeyboardCaptureLayoutConfig) { $KeyboardCaptureLayoutConfig } else { $null }
  $configDirValue = if ($KeyboardCaptureConfigDir) { $KeyboardCaptureConfigDir } else { $null }
  $logPathValue = if ($KeyboardCaptureLogPath) { $KeyboardCaptureLogPath } else { $null }
  Write-Log 'Launching keyboard capture for laptop (async)'
  $keyboardCaptureState = Start-KeyboardCaptureAsync `
    -ScriptPath $keyboardScript `
    -LogPath $logPathValue `
    -ConfigDir $configDirValue `
    -Layout $layoutValue `
    -LayoutConfig $layoutConfigValue `
    -BlockInput:$KeyboardCaptureBlockInput `
    -TimeoutSec $KeyboardCaptureTimeoutSec
}

$system = Get-CimInstanceSafe -ClassName 'Win32_ComputerSystem'
$systemInfo = $system | Select-Object -First 1
$vendor = $systemInfo.Manufacturer
$model = $systemInfo.Model

$baseboard = Get-CimInstanceSafe -ClassName 'Win32_BaseBoard' | Select-Object -First 1
$baseboardInventory = Get-BaseboardInventory
$biosInfo = Get-CimInstanceSafe -ClassName 'Win32_BIOS' | Select-Object -First 1
$biosLanguageInfo = [ordered]@{
  status = 'not_tested'
  value = $null
  source = 'manual_only'
}
$biosPasswordInfo = [ordered]@{
  status = 'not_tested'
  isSet = $null
  source = 'manual_only'
}
$biosBatteryInfo = Get-BiosBatteryStatus
Write-Log 'BIOS language/password set to not_tested (manual-only fields).'
Write-Log ("BIOS battery heuristic status={0} source={1} note={2}" -f `
  $biosBatteryInfo.status, `
  $biosBatteryInfo.source, `
  $(if ($biosBatteryInfo.note) { $biosBatteryInfo.note } else { 'none' }))

$hostname = $env:COMPUTERNAME
$macAddress = Get-PrimaryMac
$macAddresses = Get-PreferredMacs -PrimaryMac $macAddress
$macAddressesLog = if ($macAddresses) { $macAddresses -join ', ' } else { $null }
if ($macAddressesLog) { Write-Log ("MAC list (primary+secondary): {0}" -f $macAddressesLog) }
$serialNumber = Get-SerialNumber
$macSerialKey = Get-MacSerialKey -MacAddress $macAddress -SerialNumber $serialNumber -Hostname $hostname
$artifactRunDir = New-ArtifactRunDir -ArtifactRoot $artifactRootResolved -MacSerialKey $macSerialKey -ClientRunId $clientRunId
if ($artifactRunDir) {
  Write-Log "Artifact run dir: $artifactRunDir"
  $script:WinSatLogDir = Join-Path $artifactRunDir 'winsat'
  Ensure-Directory -Path $script:WinSatLogDir | Out-Null
}
if ($script:WinSatLogDir -and ($winsatRoots -notcontains $script:WinSatLogDir)) {
  $winsatRoots += $script:WinSatLogDir
}
$msinfoReportMacs = @()
if ($artifactRunDir) {
  $msinfoDir = Join-Path $artifactRunDir 'Msinfo'
  $msinfoReportMacs = Get-MacsFromMsinfo -TimeoutSec $MsinfoTimeoutSec -OutputDir $msinfoDir
}
$osVersion = Get-OsVersion
$ramMb = Get-RamMb
$slotsInfo = Get-RamSlots
$batteryHealth = Get-BatteryHealth
$batteryInfo = Get-BatteryInfo
$batteryInventory = Get-BatteryInventory
$batteryReport = $null
if ($artifactRunDir -and ($batteryInfo -or $batteryHealth -ne $null)) {
  $batteryReport = Invoke-BatteryReport -OutputDir (Join-Path $artifactRunDir 'Battery')
}
Step-Progress -Status 'Inventaire materiel'

$skipPeripheralTests = $categoryValue -eq 'desktop'
$cameraStatus = $null
$usbStatus = $null
$keyboardStatus = $null
$padStatus = $null
$badgeStatus = $null
$wifiStandardInfo = $null
$wifiStandardStatus = 'not_tested'
$wifiStandardValue = $null
$autopilotHashInfo = $null
$autopilotHashValue = $null
$cameraTestStatus = $null
$cameraTestOutputDir = $null

if ($skipPeripheralTests) {
  Write-Log 'Desktop category detected; skipping peripheral tests (camera/usb/keyboard/pad/badge).' 'INFO'
  Step-Progress -Status 'Peripheriques (skip)'
  $cameraStatus = 'not_tested'
  $usbStatus = 'not_tested'
  $keyboardStatus = 'not_tested'
  $padStatus = 'not_tested'
  $badgeStatus = 'not_tested'
} else {
  $cameraDevices = @()
  $cameraDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='Camera'"
  $cameraDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='Image'"
  $cameraPresence = Get-StatusFromDevices $cameraDevices
  $cameraStatus = $null
  $cameraTestPathValue = Resolve-CameraTestExe -Value $CameraTestPath
  if ($cameraPresence -eq 'absent' -or $cameraPresence -eq 'nok') {
    $cameraStatus = $cameraPresence
  } else {
    if (-not $cameraTestPathValue) {
      $cameraStatus = 'not_tested'
    } else {
      $cameraTestResult = Invoke-CameraTest `
        -Path $cameraTestPathValue `
        -Arguments (Normalize-ArgumentList $CameraTestArguments) `
        -TimeoutSec $CameraTestTimeoutSec `
        -OutputBaseDir $CameraTestOutputDir
      if ($cameraTestResult -and $cameraTestResult.status) {
        $cameraStatus = $cameraTestResult.status
        $cameraTestStatus = $cameraTestResult.status
        $cameraTestOutputDir = $cameraTestResult.outputDir
      } else {
        $cameraStatus = 'not_tested'
      }
    }
  }
  Write-Log ("Camera presence={0} TestPath={1} TestStatus={2} OutputDir={3} Final={4}" -f $cameraPresence, $cameraTestPathValue, $cameraTestStatus, $cameraTestOutputDir, $cameraStatus)
  Step-Progress -Status 'Peripheriques'

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
  if (-not $padStatus -or $padStatus -eq 'absent') {
    $padStatus = 'ok'
    Write-Log 'Pad status defaulted to ok'
  }

  $badgeDevices = @()
  $badgeDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='SmartCardReader'"
  $badgeDevices += Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' -Filter "PNPClass='SecurityDevices'"
  if (-not $badgeDevices -or $badgeDevices.Count -eq 0) {
    $badgeDevices = Get-CimInstanceSafe -ClassName 'Win32_PnPEntity' | Where-Object {
      $_.Name -match 'smart card|badge|rfid|smartcard'
    }
  }
  $badgeStatus = Get-StatusFromDevices $badgeDevices
}

$wifiStandardInfo = Get-WifiStandardStatus
if ($wifiStandardInfo -and $wifiStandardInfo.status) {
  $wifiStandardStatus = $wifiStandardInfo.status
}
if ($wifiStandardInfo -and $wifiStandardInfo.standard) {
  $wifiStandardValue = $wifiStandardInfo.standard
}
Write-Log ("Wi-Fi standard status={0} standard={1}" -f $wifiStandardStatus, $wifiStandardValue)

$autopilotHashInfo = Get-AutopilotHardwareHash
if ($autopilotHashInfo -and $autopilotHashInfo.value) {
  $autopilotHashValue = $autopilotHashInfo.value
}
$autopilotHashLen = if ($autopilotHashValue) { $autopilotHashValue.Length } else { 0 }
$autopilotHashSource = if ($autopilotHashInfo -and $autopilotHashInfo.source) { $autopilotHashInfo.source } else { 'unknown' }
$autopilotHashStatus = if ($autopilotHashInfo -and $autopilotHashInfo.status) { $autopilotHashInfo.status } else { 'not_tested' }
Write-Log ("Autopilot hash status={0} length={1} source={2}" -f $autopilotHashStatus, $autopilotHashLen, $autopilotHashSource)

$diskSmart = $null
$diskInventory = Get-DiskInventory
$physicalMediaInventory = Get-PhysicalMediaInventory
$volumeInventory = Get-VolumeInventory
Step-Progress -Status 'Stockage'

$systemDrive = $env:SystemDrive
if (-not $systemDrive) { $systemDrive = 'C:' }
$driveLetter = $systemDrive.TrimEnd('\')

$diskReadTest = $null
$diskWriteTest = $null
$memTest = $null
$cpuTest = $null
$gpuTest = $null
$formalTest = $null
$formalAttempted = $false
$formalStatus = $null
$formalTimeoutSec = [int](($DiskTestTimeoutSec, $MemTestTimeoutSec, $CpuTestTimeoutSec, $GpuTestTimeoutSec | Measure-Object -Maximum).Maximum)
if ($testLoops -gt 0) {
  $formalAttempted = $true
  Write-Log 'WinSAT formal run'
  $formalXmlPath = $null
  if ($script:WinSatLogDir) {
    $formalXmlPath = Join-Path $script:WinSatLogDir ("Formal.Assessment.{0}.WinSAT.xml" -f $clientRunId)
    Write-Log ("WinSAT formal xml: {0}" -f $formalXmlPath)
  }
  $script:WinSatFormalXmlPath = $formalXmlPath
  $formalTest = Invoke-WinsatCapture -Arguments @('formal') -TimeoutSec $formalTimeoutSec -OutputXmlPath $formalXmlPath
}
if ($formalTest -and $formalTest.status -eq 'timeout') {
  Write-Log 'WinSAT formal timeout; marking as not_tested.' 'WARN'
  $formalTest.status = 'not_tested'
}
if ($formalTest -and $formalTest.status -ne 'ok' -and $script:WinSatFormalXmlPath -and (Test-Path $script:WinSatFormalXmlPath)) {
  Write-Log 'WinSAT formal XML present; treating status as ok.' 'INFO'
  $formalTest.status = 'ok'
}
if ($formalTest -and $formalTest.status -and $formalTest.status -ne 'ok') {
  $formalStatus = $formalTest.status
}

$cpuExternalStatus = $null
if ($CpuTestPath) {
  $cpuExternalStatus = Invoke-ExternalTest -Path $CpuTestPath -Arguments (Normalize-ArgumentList $CpuTestArguments) -TimeoutSec $CpuTestTimeoutSec -Name 'CPU'
}

$gpuExternalStatus = $null
if ($GpuTestPath) {
  $gpuExternalStatus = Invoke-ExternalTest -Path $GpuTestPath -Arguments (Normalize-ArgumentList $GpuTestArguments) -TimeoutSec $GpuTestTimeoutSec -Name 'GPU'
}

$memDiagStatus = $null

$networkPingTargetValue = if ($NetworkPingTarget) { $NetworkPingTarget } else { Get-DefaultGateway }
$networkPingResult = Test-NetworkPing -Target $networkPingTargetValue -Count $NetworkPingCount
$iperfResult = $null

$fsCheckModeValue = $FsCheckMode
if ($FsCheckMode -eq 'auto') {
  $fsCheckModeValue = if ($TestMode -eq 'stress') { 'scan' } else { 'dirty' }
}
$fsCheckResult = if ($fsCheckModeValue -eq 'none') {
  @{ status = 'not_tested' }
} else {
  Invoke-FsCheck -DriveLetter $driveLetter -TimeoutSec $FsCheckTimeoutSec -Mode $fsCheckModeValue
}
Step-Progress -Status 'Diagnostics'

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
$memorySerials = Get-MemorySerialInventory -Modules $memoryModules
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
$thermalInfo = $null

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
if ($clientRunId) { $diag.clientRunId = $clientRunId }
if ($macSerialKey) { $diag.macSerialKey = $macSerialKey }

$tests = [ordered]@{
  diskRead = 'not_tested'
  diskWrite = 'not_tested'
  ramTest = 'not_tested'
  cpuTest = 'not_tested'
  gpuTest = 'not_tested'
  networkPing = 'not_tested'
  fsCheck = 'not_tested'
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

$refreshWinsat = $false
if (-not $winsatStore -or $formalAttempted) { $refreshWinsat = $true }
if ($refreshWinsat -and -not $SkipWinSatDataStore -and $winsatRoots.Count -gt 0) {
  $winsatStore = Get-WinsatFromDataStore -Paths $winsatRoots
  if ($winsatStore) { Write-Log 'WinSAT datastore refreshed after tests.' 'INFO' }
}
if (-not $winsatStore -and $script:WinSatFormalXmlPath) {
  $winsatStore = Get-WinsatFromXmlFile -Path $script:WinSatFormalXmlPath
  if ($winsatStore) {
    Write-Log ("WinSAT parsed from formal XML: {0}" -f $script:WinSatFormalXmlPath)
  }
}

$winsatCpuScore = $null
$winsatMemScore = $null
$winsatGraphicsScore = $null

if ($winsatStore) {
  if ($winsatStore.files -and $winsatStore.files.Keys.Count -gt 0) {
    $fileList = ($winsatStore.files.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ', '
    Write-Log "WinSAT datastore parsed: $fileList"
  } else {
    Write-Log 'WinSAT datastore parsed with no file list.' 'WARN'
  }
  $cpuMax = $null
  if ($winsatStore.cpu -and $winsatStore.cpu.maxMBps -ne $null) { $cpuMax = $winsatStore.cpu.maxMBps }
  if ($winsatStore.winSPR -and $winsatStore.winSPR.CpuScore -ne $null) { $winsatCpuScore = $winsatStore.winSPR.CpuScore }
  $cpuStatus = $winsatStore.cpuStatus
  Write-Log ("WinSAT CPU summary: maxMBps={0} cpuScore={1} cpuStatus={2}" -f $cpuMax, $winsatCpuScore, $cpuStatus)
  $cpuOk = $false
  if ($cpuMax -ne $null) { $cpuOk = $true }
  elseif ($winsatCpuScore -ne $null) { $cpuOk = $true }
  elseif ($cpuStatus -eq 'ok') { $cpuOk = $true }
  if ($cpuOk) {
    if (-not $tests.cpuTest -or $tests.cpuTest -ne 'ok') { $tests.cpuTest = 'ok' }
    if ($tests.cpuMBps -eq $null -and $cpuMax -ne $null) { $tests.cpuMBps = $cpuMax }
  }

  $memBandwidth = $null
  if ($winsatStore.memory -and $winsatStore.memory.bandwidthMBps -ne $null) { $memBandwidth = $winsatStore.memory.bandwidthMBps }
  if ($winsatStore.winSPR -and $winsatStore.winSPR.MemoryScore -ne $null) { $winsatMemScore = $winsatStore.winSPR.MemoryScore }
  if ($memBandwidth -ne $null) {
    if (-not $tests.ramTest -or $tests.ramTest -ne 'ok') { $tests.ramTest = 'ok' }
    if ($tests.ramMBps -eq $null) { $tests.ramMBps = [math]::Round($memBandwidth, 1) }
  }

  if (($winsatStore.disk -eq $null -or $winsatStore.disk.Count -eq 0) -and $script:WinSatFormalXmlPath) {
    $diskFromXml = Get-WinsatDiskFromXml -Path $script:WinSatFormalXmlPath
    if ($diskFromXml) {
      $winsatStore.disk = $diskFromXml
      Write-Log ("WinSAT disk metrics from XML: seqRead={0} seqWrite={1}" -f $diskFromXml.seqReadMBps, $diskFromXml.seqWriteMBps)
    }
  }

  $diskSeqRead = $null
  if ($winsatStore.disk -and $winsatStore.disk.seqReadMBps -ne $null) { $diskSeqRead = $winsatStore.disk.seqReadMBps }
  if ($diskSeqRead -ne $null) {
    if (-not $tests.diskRead -or $tests.diskRead -ne 'ok') { $tests.diskRead = 'ok' }
    if ($tests.diskReadMBps -eq $null) { $tests.diskReadMBps = [math]::Round($diskSeqRead, 1) }
  }

  if ($winsatStore.disk -and $winsatStore.disk.seqWriteMBps -ne $null -and $tests.diskWriteMBps -eq $null) {
    $tests.diskWriteMBps = [math]::Round($winsatStore.disk.seqWriteMBps, 1)
    if (-not $tests.diskWrite -or $tests.diskWrite -ne 'ok') { $tests.diskWrite = 'ok' }
  }
  if ($winsatStore.winSPR -and $winsatStore.winSPR.DiskScore -ne $null) {
    if (-not $tests.diskRead -or $tests.diskRead -ne 'ok') { $tests.diskRead = 'ok' }
    if (-not $tests.diskWrite -or $tests.diskWrite -ne 'ok') { $tests.diskWrite = 'ok' }
  }

  if ($winsatStore.winSPR -and $winsatStore.winSPR.GamingScore -ne $null) { $winsatGraphicsScore = $winsatStore.winSPR.GamingScore }
  if ($winsatGraphicsScore -eq $null -and $winsatStore.winSPR -and $winsatStore.winSPR.GraphicsScore -ne $null) {
    $winsatGraphicsScore = $winsatStore.winSPR.GraphicsScore
  }
  if ($winsatGraphicsScore -ne $null) {
    if ($winsatStore.hasNoD3DTest) {
      Write-Log 'WinSAT D3D test skipped; using WinSPR GraphicsScore/GamingScore for GPU.' 'WARN'
    }
    if (-not $tests.gpuTest -or $tests.gpuTest -ne 'ok') { $tests.gpuTest = 'ok' }
    if ($tests.gpuScore -eq $null) { $tests.gpuScore = $winsatGraphicsScore }
  }
} else {
  if (-not $SkipWinSatDataStore) {
    $rootsLabel = if ($winsatRoots.Count -gt 0) { $winsatRoots -join ', ' } else { 'none' }
    Write-Log "WinSAT datastore not found in: $rootsLabel" 'WARN'
  }
}

if ($tests.cpuTest -ne 'ok' -and $winsatRoots.Count -gt 0) {
  $cpuFallback = Get-WinsatCpuFallback -Roots $winsatRoots
  if ($cpuFallback) {
    Write-Log ("WinSAT CPU fallback: file={0} status={1} score={2} maxMBps={3}" -f $cpuFallback.file, $cpuFallback.status, $cpuFallback.score, $cpuFallback.maxMBps)
    if ($cpuFallback.status -eq 'ok' -or $cpuFallback.score -ne $null -or $cpuFallback.maxMBps -ne $null) {
      $tests.cpuTest = 'ok'
      if ($tests.cpuMBps -eq $null -and $cpuFallback.maxMBps -ne $null) { $tests.cpuMBps = $cpuFallback.maxMBps }
    }
  }
}

$winsatDriveArg = if ($driveLetter) { $driveLetter.TrimEnd(':') } else { 'C' }
if ($tests.ramTest -ne 'ok' -or $tests.ramMBps -eq $null) {
  Write-Log 'WinSAT memory fallback run (winsat mem).' 'WARN'
  $memFallback = Invoke-WinsatAssessmentWithXml -AssessmentKey 'Mem' -Arguments @('mem') -TimeoutSec $MemTestTimeoutSec
  $memFallbackBandwidth = $null
  if ($memFallback -and $memFallback.winsat -and $memFallback.winsat.memory -and $memFallback.winsat.memory.bandwidthMBps -ne $null) {
    $memFallbackBandwidth = $memFallback.winsat.memory.bandwidthMBps
  }
  Write-Log ("WinSAT memory fallback status={0} bandwidth={1} xml={2}" -f $memFallback.status, $memFallbackBandwidth, $memFallback.xmlPath)
  if ($memFallbackBandwidth -ne $null) {
    $tests.ramTest = 'ok'
    $tests.ramMBps = [math]::Round($memFallbackBandwidth, 1)
  } elseif ($memFallback.status -eq 'ok' -and $tests.ramTest -eq 'not_tested') {
    $tests.ramTest = 'ok'
  }
}

if ($tests.diskRead -ne 'ok' -or $tests.diskReadMBps -eq $null) {
  Write-Log ("WinSAT disk read fallback run (drive={0})." -f $winsatDriveArg) 'WARN'
  $diskReadFallback = Invoke-WinsatAssessmentWithXml -AssessmentKey 'DiskRead' -Arguments @('disk', '-seq', '-read', '-drive', $winsatDriveArg) -TimeoutSec $DiskTestTimeoutSec
  $diskReadMbps = $null
  if ($diskReadFallback -and $diskReadFallback.winsat -and $diskReadFallback.winsat.disk -and $diskReadFallback.winsat.disk.seqReadMBps -ne $null) {
    $diskReadMbps = $diskReadFallback.winsat.disk.seqReadMBps
  }
  Write-Log ("WinSAT disk read fallback status={0} mbps={1} xml={2}" -f $diskReadFallback.status, $diskReadMbps, $diskReadFallback.xmlPath)
  if ($diskReadMbps -ne $null) {
    $tests.diskRead = 'ok'
    $tests.diskReadMBps = [math]::Round($diskReadMbps, 1)
  } elseif ($diskReadFallback.status -eq 'ok' -and $tests.diskRead -eq 'not_tested') {
    $tests.diskRead = 'ok'
  }
}

if ($tests.diskWrite -ne 'ok' -or $tests.diskWriteMBps -eq $null) {
  Write-Log ("WinSAT disk write fallback run (drive={0})." -f $winsatDriveArg) 'WARN'
  $diskWriteFallback = Invoke-WinsatAssessmentWithXml -AssessmentKey 'DiskWrite' -Arguments @('disk', '-seq', '-write', '-drive', $winsatDriveArg) -TimeoutSec $DiskTestTimeoutSec
  $diskWriteMbps = $null
  if ($diskWriteFallback -and $diskWriteFallback.winsat -and $diskWriteFallback.winsat.disk -and $diskWriteFallback.winsat.disk.seqWriteMBps -ne $null) {
    $diskWriteMbps = $diskWriteFallback.winsat.disk.seqWriteMBps
  }
  Write-Log ("WinSAT disk write fallback status={0} mbps={1} xml={2}" -f $diskWriteFallback.status, $diskWriteMbps, $diskWriteFallback.xmlPath)
  if ($diskWriteMbps -ne $null) {
    $tests.diskWrite = 'ok'
    $tests.diskWriteMBps = [math]::Round($diskWriteMbps, 1)
  } elseif ($diskWriteFallback.status -eq 'ok' -and $tests.diskWrite -eq 'not_tested') {
    $tests.diskWrite = 'ok'
  }
}

if ($formalStatus -and $formalStatus -ne 'ok') {
  if (-not $tests.diskRead -or $tests.diskRead -eq 'not_tested') { $tests.diskRead = $formalStatus }
  if (-not $tests.diskWrite -or $tests.diskWrite -eq 'not_tested') { $tests.diskWrite = $formalStatus }
  if (-not $tests.ramTest -or $tests.ramTest -eq 'not_tested') { $tests.ramTest = $formalStatus }
  if (-not $tests.cpuTest -or $tests.cpuTest -eq 'not_tested') { $tests.cpuTest = $formalStatus }
  if (-not $tests.gpuTest -or $tests.gpuTest -eq 'not_tested') { $tests.gpuTest = $formalStatus }
}

if ($tests.cpuTest -ne 'ok' -and $winsatRoots.Count -gt 0) {
  $cpuFallback = Get-WinsatCpuFallback -Roots $winsatRoots
  if ($cpuFallback) {
    Write-Log ("WinSAT CPU fallback: file={0} status={1} score={2} maxMBps={3}" -f $cpuFallback.file, $cpuFallback.status, $cpuFallback.score, $cpuFallback.maxMBps)
    if ($cpuFallback.status -eq 'ok' -or $cpuFallback.score -ne $null -or $cpuFallback.maxMBps -ne $null) {
      $tests.cpuTest = 'ok'
      if ($tests.cpuMBps -eq $null -and $cpuFallback.maxMBps -ne $null) { $tests.cpuMBps = $cpuFallback.maxMBps }
    }
  }
}

if ($tests.cpuTest -eq 'not_tested') {
  Write-Log 'CPU still not_tested after WinSAT parse; running cpuformal fallback.' 'WARN'
  $cpuOnlyTest = Invoke-WinsatCapture -Arguments @('cpuformal') -TimeoutSec $CpuTestTimeoutSec
  if ($cpuOnlyTest -and $cpuOnlyTest.status) {
    if ($cpuOnlyTest.status -eq 'ok') {
      $tests.cpuTest = 'ok'
      Write-Log 'CPU fallback cpuformal succeeded.'
    } elseif ($cpuOnlyTest.status -ne 'not_tested') {
      $tests.cpuTest = $cpuOnlyTest.status
      Write-Log ("CPU fallback cpuformal status={0}" -f $cpuOnlyTest.status) 'WARN'
    }
  }
}

if ($tests.cpuTest -eq 'not_tested' -and $formalTest -and $formalTest.status -eq 'ok') {
  $tests.cpuTest = 'ok'
  Write-Log 'CPU marked ok because WinSAT formal completed successfully.' 'INFO'
}

if (-not $tests.cpuNote -and $winsatCpuScore -ne $null) {
  $tests.cpuNote = Get-WinSatNote -Score $winsatCpuScore
}
if (-not $tests.ramNote -and $winsatMemScore -ne $null) {
  $tests.ramNote = Get-WinSatNote -Score $winsatMemScore
}
if (-not $tests.gpuNote) {
  $gpuNoteScore = if ($winsatGraphicsScore -ne $null) { $winsatGraphicsScore } else { $tests.gpuScore }
  if ($gpuNoteScore -ne $null) { $tests.gpuNote = Get-WinSatNote -Score $gpuNoteScore }
}

$gpuAssessment = $null
$dxDiagResult = $null
if (-not $SkipGpuAssessment) {
  $dxDiagOutputDir = $null
  if ($artifactRunDir) {
    $dxDiagOutputDir = Join-Path $artifactRunDir 'DxDiag'
  } elseif ($env:TEMP) {
    $dxDiagOutputDir = Join-Path $env:TEMP 'mdt-fusion\\dxdiag'
  }
  $dxDiagResult = Invoke-DxDiagCapture -OutputDir $dxDiagOutputDir -TimeoutSec $DxDiagTimeoutSec
  if ($dxDiagResult -and $dxDiagResult.status -and $dxDiagResult.status -ne 'ok') {
    Write-Log "DxDiag status=$($dxDiagResult.status)" 'WARN'
  }
  $dxDiagPath = if ($dxDiagResult) { $dxDiagResult.txtPath } else { $null }
  $gpuAssessment = Get-GpuAssessment -DxDiagPath $dxDiagPath -WinSatStore $winsatStore -GpuScore $tests.gpuScore -GpuNameFallback $gpuInfo.name
  if ($gpuAssessment -and $artifactRunDir) {
    $gpuAssessmentDir = Join-Path $artifactRunDir 'GpuAssessment'
    Ensure-Directory -Path $gpuAssessmentDir | Out-Null
    Write-JsonFile -Path (Join-Path $gpuAssessmentDir 'gpu_assessment.json') -Object $gpuAssessment | Out-Null
    $summary = @()
    $summary += "GPU: $($gpuAssessment.gpu_name)"
    $levels = @($gpuAssessment.feature_levels) | Where-Object { $_ }
    $summary += "FeatureLevels: " + ($levels -join ", ")
    $summary += "GraphicsScore: $($gpuAssessment.graphics_score) GamingScore: $($gpuAssessment.gaming_score) CPU: $($gpuAssessment.cpu_score) MEM: $($gpuAssessment.memory_score)"
    $summary += "has_dxdiag=$($gpuAssessment.has_dxdiag) has_winsat=$($gpuAssessment.has_winsat) winsat_parse_ok=$($gpuAssessment.winsat_parse_ok)"
    $tail = $gpuAssessment.winsat_log_tail
    if ($tail) {
      $summary += "WinSATLogTail: " + ($tail.Substring([Math]::Max(0, $tail.Length - 4000)))
    } else {
      $summary += "WinSATLogTail: "
    }
    $summary += "Category: $($gpuAssessment.category)"
    Set-Content -Path (Join-Path $gpuAssessmentDir 'summary.txt') -Value ($summary -join "`n") -Encoding UTF8 -Force
  }
}

$commentCaptureStarted = $false
$nonOkTests = @()
if ($artifactRunDir) {
  $componentStatuses = @{}
  if ($cameraStatus) { $componentStatuses.cameraStatus = $cameraStatus }
  if ($usbStatus) { $componentStatuses.usbStatus = $usbStatus }
  if ($keyboardStatus) { $componentStatuses.keyboardStatus = $keyboardStatus }
  if ($padStatus) { $componentStatuses.padStatus = $padStatus }
  if ($badgeStatus) { $componentStatuses.badgeReaderStatus = $badgeStatus }
  if ($wifiStandardStatus) { $componentStatuses.wifiStandard = $wifiStandardStatus }
  if ($gpuStatus) { $componentStatuses.gpuStatus = $gpuStatus }
  if ($thermalInfo -and $thermalInfo.status) { $componentStatuses.thermalStatus = $thermalInfo.status }

  $nonOkTests = Get-NonOkTests -Tests $tests -Extras $componentStatuses
}

$diag.diagnosticsPerformed = $tests.Keys.Count

$stopwatch.Stop()
$diag.completedAt = (Get-Date).ToString('o')
$diag.durationSec = [int]$stopwatch.Elapsed.TotalSeconds

$payload = [ordered]@{}
if ($clientRunId) { $payload.reportId = $clientRunId }
$technicianValue = if ($Technician) { $Technician.Trim() } else { $null }
if ($hostname) { $payload.hostname = $hostname }
if ($macAddress) { $payload.macAddress = $macAddress }
if ($macAddresses -and $macAddresses.Count -gt 0) { $payload.macAddresses = $macAddresses }
if ($serialNumber) { $payload.serialNumber = $serialNumber }
if ($categoryValue) { $payload.category = $categoryValue }
if ($ReportTag) { $payload.tag = $ReportTag }
if ($ReportTagId) { $payload.tagId = $ReportTagId }
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
if ($wifiStandardStatus) { $payload.wifiStandardStatus = $wifiStandardStatus }
if ($wifiStandardValue) { $payload.wifiStandard = $wifiStandardValue }
if ($biosLanguageInfo -and $biosLanguageInfo.status) { $payload.biosLanguageStatus = $biosLanguageInfo.status }
if ($biosLanguageInfo -and $biosLanguageInfo.value) { $payload.biosLanguage = $biosLanguageInfo.value }
if ($biosPasswordInfo -and $biosPasswordInfo.status) { $payload.biosPasswordStatus = $biosPasswordInfo.status }
if ($biosPasswordInfo -and $biosPasswordInfo.isSet -ne $null) { $payload.biosPasswordSet = [bool]$biosPasswordInfo.isSet }
if ($biosBatteryInfo -and $biosBatteryInfo.status) { $payload.biosBatteryStatus = $biosBatteryInfo.status }
if ($autopilotHashValue) { $payload.autopilotHash = $autopilotHashValue }
if ($wifiStandardStatus) {
  $payload.wifi = [ordered]@{
    status = $wifiStandardStatus
    standard = $wifiStandardValue
    standards = if ($wifiStandardInfo -and $wifiStandardInfo.standards) { @($wifiStandardInfo.standards) } else { @() }
  }
}
$payload.autopilot = [ordered]@{
  status = $autopilotHashStatus
  hardwareHash = $autopilotHashValue
  source = $autopilotHashSource
}
if ($diskInventory) {
  $diskList = @($diskInventory)
  if ($diskList.Count -gt 0) { $payload.disks = $diskList }
}
if ($volumeInventory) {
  $volumeList = @($volumeInventory)
  if ($volumeList.Count -gt 0) { $payload.volumes = $volumeList }
}

$inventory = [ordered]@{}
if ($batteryInventory -and $batteryInventory.Count -gt 0) { $inventory.battery = $batteryInventory }
if ($physicalMediaInventory -and $physicalMediaInventory.Count -gt 0) { $inventory.disks = $physicalMediaInventory }
if ($memorySerials -and $memorySerials.Count -gt 0) { $inventory.memory = $memorySerials }
if ($baseboardInventory) { $inventory.baseboard = $baseboardInventory }
if ($autopilotHashValue) { $inventory.autopilotHash = $autopilotHashValue }
if ($inventory.Count -gt 0) { $payload.inventory = $inventory }


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
  language = if ($biosLanguageInfo -and $biosLanguageInfo.value) { $biosLanguageInfo.value } else { $null }
  passwordStatus = if ($biosPasswordInfo) { $biosPasswordInfo.status } else { $null }
  batteryStatus = if ($biosBatteryInfo) { $biosBatteryInfo.status } else { $null }
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
if ($gpuAssessment) { $payload.gpuAssessment = $gpuAssessment }

$payload.memory = [ordered]@{
  totalGb = if ($ramMb) { [math]::Round($ramMb / 1024, 1) } else { $null }
  type = $memType
  speedMHz = $memSpeed
  usage = $memoryUsage
  pagefile = $pageFileInfo
}

if ($memorySlots) {
  $memorySlotList = @($memorySlots)
  if ($memorySlotList.Count -gt 0) { $payload.memorySlots = $memorySlotList }
}
if ($displayInfo.Count -gt 0) { $payload.display = $displayInfo }
if ($thermalInfo) { $payload.thermal = $thermalInfo }
if ($tests.Count -gt 0) { $payload.tests = $tests }
if ($winsatStore) { $payload.winsat = $winsatStore }

$components = [ordered]@{
  biosBattery = if ($biosBatteryInfo -and $biosBatteryInfo.status) { $biosBatteryInfo.status } else { 'not_tested' }
  biosLanguage = if ($biosLanguageInfo -and $biosLanguageInfo.status) { $biosLanguageInfo.status } else { 'not_tested' }
  biosPassword = if ($biosPasswordInfo -and $biosPasswordInfo.status) { $biosPasswordInfo.status } else { 'not_tested' }
  wifiStandard = if ($wifiStandardStatus) { $wifiStandardStatus } else { 'not_tested' }
  diskReadTest = if ($tests.diskRead) { $tests.diskRead } else { 'not_tested' }
  diskWriteTest = if ($tests.diskWrite) { $tests.diskWrite } else { 'not_tested' }
  ramTest = if ($tests.ramTest) { $tests.ramTest } else { 'not_tested' }
  cpuTest = if ($tests.cpuTest) { $tests.cpuTest } else { 'not_tested' }
  gpuTest = if ($tests.gpuTest) { $tests.gpuTest } else { 'not_tested' }
  networkPing = if ($tests.networkPing) { $tests.networkPing } else { 'not_tested' }
  fsCheck = if ($tests.fsCheck) { $tests.fsCheck } else { 'not_tested' }
}
if ($cameraStatus) { $components.camera = $cameraStatus }
if ($usbStatus) { $components.usb = $usbStatus }
if ($keyboardStatus) { $components.keyboard = $keyboardStatus }
if ($padStatus) { $components.pad = $padStatus }
if ($badgeStatus) { $components.badgeReader = $badgeStatus }
if ($diskSmart) { $components.diskSmart = $diskSmart }
if ($tests.cpuStress) { $components.cpuStress = $tests.cpuStress }
if ($tests.gpuStress) { $components.gpuStress = $tests.gpuStress }
if ($tests.network) { $components.networkTest = $tests.network }
if ($tests.memDiag) { $components.memDiag = $tests.memDiag }
if ($tests.cpuTest) { $components.cpu = $tests.cpuTest }
if ($gpuStatus) { $components.gpu = $gpuStatus } elseif ($tests.gpuTest) { $components.gpu = $tests.gpuTest }
if ($thermalInfo -and $thermalInfo.status) { $components.thermal = $thermalInfo.status }

if ($components.Count -gt 0) {
  $payload.components = $components
}

$keyboardLogPathValue = $null
if ($keyboardCaptureState -and $keyboardCaptureState.logPath) {
  $keyboardLogPathValue = $keyboardCaptureState.logPath
}

if ($artifactRunDir) {
  $manifest = @{
    mac_serial_key = $macSerialKey
    mac_address = $macAddress
    serial_number = $serialNumber
    client_run_id = $clientRunId
    host_name = $hostname
    script_version = $scriptVersion
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
  }
  $payload.rawArtifacts = [ordered]@{
    clientRunId = $clientRunId
    macSerialKey = $macSerialKey
  }
  if ($clientRunId) { $payload.rawArtifacts.reportId = $clientRunId }
  if ($ObjectStorageEndpoint) { $payload.rawArtifacts.endpoint = $ObjectStorageEndpoint }
  if ($ObjectStorageBucket) { $payload.rawArtifacts.bucket = $ObjectStorageBucket }
  if ($ObjectStoragePrefix) { $payload.rawArtifacts.prefix = $ObjectStoragePrefix }
  if ($ReportTag) { $payload.rawArtifacts.tag = $ReportTag }
  if ($ReportTagId) { $payload.rawArtifacts.tagId = $ReportTagId }

  $payloadJson = $payload | ConvertTo-Json -Depth 10
  Stage-RunArtifacts `
    -RunDir $artifactRunDir `
    -LogPath $LogPath `
    -PayloadJson $payloadJson `
    -CameraOutputDir $cameraTestOutputDir `
    -KeyboardLogPath $keyboardLogPathValue `
    -WinsatStore $winsatStore `
    -Manifest $manifest | Out-Null

  if (-not $SkipRawUpload) {
    $rawUploadResult = Upload-RunArtifacts `
      -RunDir $artifactRunDir `
      -Endpoint $ObjectStorageEndpoint `
      -Bucket $ObjectStorageBucket `
      -AccessKey $ObjectStorageAccessKey `
      -SecretKey $ObjectStorageSecretKey `
      -Prefix $ObjectStoragePrefix `
      -Tag $ReportTag `
      -MacSerialKey $macSerialKey `
      -ClientRunId $clientRunId `
      -McPath $ObjectStorageMcPath
    if ($rawUploadResult) {
      $payload.rawArtifacts.uploaded = $rawUploadResult.success
      if ($rawUploadResult.destination) { $payload.rawArtifacts.destination = $rawUploadResult.destination }
      if ($rawUploadResult.error) { $payload.rawArtifacts.error = $rawUploadResult.error }
    }
  } else {
    $payload.rawArtifacts.uploaded = $false
    $payload.rawArtifacts.error = 'raw_upload_disabled'
  }
  Write-JsonFile -Path (Join-Path $artifactRunDir 'payload.json') -Object $payload | Out-Null
}

$json = $payload | ConvertTo-Json -Depth 10
Step-Progress -Status 'Assemblage payload'

Write-Log "Payload size=$($json.Length)"

$ingestOk = $false
Step-Progress -Status 'Envoi API'
try {
  $response = Invoke-JsonPost -Url $ApiUrl -Json $json -TimeoutSec $TimeoutSec
  Write-Log 'Ingest OK'
  $ingestOk = $true
  Write-Output $response
} catch {
  Write-Log $_.Exception.Message 'ERROR'
  if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
    Write-Log $_.ErrorDetails.Message 'ERROR'
  }
  if ($FailTaskSequenceOnError) {
    Write-Error $_
    Complete-Progress
    Invoke-FactoryResetPrompt -ForceReset:$true -ConfirmToken 'RESET'
    exit 1
  }
  Write-Log 'Ingest failed but task sequence failure is disabled; continuing deployment.' 'WARN'
  $ingestOk = $false
}

if ($ingestOk -and $artifactRunDir -and $nonOkTests -and $nonOkTests.Count -gt 0) {
  $commentUser = if ($env:MDT_COMMENT_USER) { $env:MDT_COMMENT_USER } elseif ($env:MDT_HYDRA_USER) { $env:MDT_HYDRA_USER } else { 'admin' }
  $commentPassword = if ($env:MDT_COMMENT_PASSWORD) { $env:MDT_COMMENT_PASSWORD } elseif ($env:MDT_HYDRA_PASSWORD) { $env:MDT_HYDRA_PASSWORD } else { 'admin' }
  if (-not $commentUser -or -not $commentPassword) {
    Write-Log 'Comment capture skipped: missing MDT_COMMENT_USER/MDT_COMMENT_PASSWORD.' 'WARN'
  } else {
    $reportIdValue = $null
    if ($response -and $response.reportId) { $reportIdValue = $response.reportId }
    elseif ($response -and $response.id) { $reportIdValue = $response.id }
    elseif ($clientRunId) { $reportIdValue = $clientRunId }
    if ($reportIdValue) {
      $commentCaptureStarted = Start-TestCommentCapture `
        -Tests $nonOkTests `
        -RunDir $artifactRunDir `
        -ApiUrl $ApiUrl `
        -ReportId $reportIdValue `
        -CommentUser $commentUser `
        -CommentPassword $commentPassword `
        -LogPath $LogPath `
        -TimeoutSec 120
      if ($commentCaptureStarted) {
        $labels = ($nonOkTests | ForEach-Object { $_.label }) -join ', '
        Write-Log "NOK tests detected; comment capture started (120s): $labels"
      }
    } else {
      Write-Log 'Comment capture skipped: reportId missing.' 'WARN'
    }
  }
}

if ($ingestOk -and -not $SkipKeyboardCapture -and $categoryValue -eq 'laptop') {
  $keyboardResult = if ($keyboardCaptureState -and $keyboardCaptureState.started) {
    Complete-KeyboardCapture -State $keyboardCaptureState
  } else {
    Invoke-KeyboardCapture `
      -ScriptPath (Resolve-KeyboardCapturePath -Value $KeyboardCapturePath) `
      -LogPath $KeyboardCaptureLogPath `
      -ConfigDir $KeyboardCaptureConfigDir `
      -Layout $KeyboardCaptureLayout `
      -LayoutConfig $KeyboardCaptureLayoutConfig `
      -BlockInput:$KeyboardCaptureBlockInput `
      -TimeoutSec $KeyboardCaptureTimeoutSec
  }
  if ($keyboardResult -and $keyboardResult.status -and $keyboardResult.status -ne 'not_tested') {
    $kbPayload = [ordered]@{}
    if ($clientRunId) { $kbPayload.reportId = $clientRunId }
    if ($hostname) { $kbPayload.hostname = $hostname }
    if ($macAddress) { $kbPayload.macAddress = $macAddress }
    if ($serialNumber) { $kbPayload.serialNumber = $serialNumber }
    $kbPayload.payloadMode = 'skip'
    $kbPayload.keyboardStatus = $keyboardResult.status
    $kbPayload.components = @{ keyboard = $keyboardResult.status }
    if ($keyboardResult.padStatus -and $keyboardResult.padStatus -ne 'not_tested') {
      $kbPayload.padStatus = $keyboardResult.padStatus
      $kbPayload.components.pad = $keyboardResult.padStatus
      $padStatus = $keyboardResult.padStatus
    }
    $kbJson = $kbPayload | ConvertTo-Json -Depth 4
    try {
      Invoke-JsonPost -Url $ApiUrl -Json $kbJson -TimeoutSec $TimeoutSec | Out-Null
      Write-Log "Keyboard status updated: $($keyboardResult.status)"
    } catch {
      Write-Log "Keyboard status update failed: $($_.Exception.Message)" 'WARN'
    }
  }
}

if ($artifactRunDir) {
  $kbLogPath = $keyboardLogPathValue
  if (-not $kbLogPath -and $keyboardCaptureState -and $keyboardCaptureState.logPath) {
    $kbLogPath = $keyboardCaptureState.logPath
  }
  Stage-RunArtifacts -RunDir $artifactRunDir -LogPath $LogPath -KeyboardLogPath $kbLogPath | Out-Null
  if (-not $SkipRawUpload) {
    $postUpload = Upload-RunArtifacts `
      -RunDir $artifactRunDir `
      -Endpoint $ObjectStorageEndpoint `
      -Bucket $ObjectStorageBucket `
      -AccessKey $ObjectStorageAccessKey `
      -SecretKey $ObjectStorageSecretKey `
      -Prefix $ObjectStoragePrefix `
      -Tag $ReportTag `
      -MacSerialKey $macSerialKey `
      -ClientRunId $clientRunId `
      -McPath $ObjectStorageMcPath
    if ($postUpload -and -not $postUpload.success) {
      Write-Log "Raw artifact mirror after keyboard failed: $($postUpload.error)" 'WARN'
    }
  }
}

Step-Progress -Status 'Finalisation'
Complete-Progress
Invoke-FactoryResetPrompt -ForceReset:$true -ConfirmToken 'RESET'
