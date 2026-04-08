[CmdletBinding()]
param()

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

. (Join-Path $PSScriptRoot 'MmaMdtRunner.Common.ps1')

$config = Get-MdtRunnerConfig -RootPath $PSScriptRoot
$technicians = Get-MdtRunnerTechnicians -RootPath $PSScriptRoot

$colors = @{
  Background = [System.Drawing.Color]::FromArgb(245, 247, 250)
  Surface = [System.Drawing.Color]::White
  Border = [System.Drawing.Color]::FromArgb(216, 223, 232)
  Accent = [System.Drawing.Color]::FromArgb(28, 59, 92)
  AccentSoft = [System.Drawing.Color]::FromArgb(227, 238, 248)
  Primary = [System.Drawing.Color]::FromArgb(21, 114, 184)
  Success = [System.Drawing.Color]::FromArgb(35, 139, 89)
  Danger = [System.Drawing.Color]::FromArgb(182, 59, 59)
  Warning = [System.Drawing.Color]::FromArgb(196, 121, 31)
  Text = [System.Drawing.Color]::FromArgb(31, 43, 56)
  Muted = [System.Drawing.Color]::FromArgb(101, 116, 133)
}

$state = [ordered]@{
  Mode = $null
  Process = $null
  ActiveLogPath = $null
  LastLogContent = ''
}

function Add-UiLog {
  param(
    [System.Windows.Forms.TextBox]$Target,
    [string]$Message,
    [switch]$Clear
  )

  if ($Clear) {
    $Target.Clear()
  }
  if ($Message -eq $null) { return }
  if ($Target.TextLength -gt 0) {
    $Target.AppendText([Environment]::NewLine)
  }
  $Target.AppendText($Message)
  $Target.SelectionStart = $Target.TextLength
  $Target.ScrollToCaret()
}

function Set-RunnerBusy {
  param([bool]$Busy)

  $comboTechnician.Enabled = -not $Busy
  $comboCategory.Enabled = -not $Busy
  $comboMode.Enabled = -not $Busy
  $checkReset.Enabled = -not $Busy
  $buttonLaunch.Enabled = -not $Busy
  $buttonSync.Enabled = -not $Busy
}

function Refresh-OutboxView {
  $listQueue.Items.Clear()
  $stats = Get-MdtRunnerOutboxStats -OutboxRoot $config.outboxRoot
  $labelQueueStats.Text = "En attente: $($stats.Pending)   Envoyes: $($stats.Sent)   Echecs: $($stats.Failed)"

  foreach ($item in Get-MdtRunnerOutboxItems -OutboxRoot $config.outboxRoot) {
    $row = New-Object System.Windows.Forms.ListViewItem($item.Name)
    [void]$row.SubItems.Add($item.Technician)
    [void]$row.SubItems.Add($item.Hostname)
    [void]$row.SubItems.Add($item.QueuedAt)
    [void]$row.SubItems.Add([string]$item.Attempts)
    [void]$row.SubItems.Add($item.LastError)
    [void]$listQueue.Items.Add($row)
  }
}

function Start-BackgroundProcess {
  param(
    [string]$Mode,
    [string]$ScriptPath,
    [string[]]$ArgumentList,
    [string]$LogPath,
    [string]$StatusText
  )

  if (-not (Test-Path -Path $ScriptPath)) {
    throw "Script introuvable: $ScriptPath"
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = 'powershell.exe'
  $psi.WorkingDirectory = Split-Path -Path $ScriptPath -Parent
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true
  $psi.Arguments = ($ArgumentList | ForEach-Object {
      if ($_ -match '\s') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
    }) -join ' '

  $process = New-Object System.Diagnostics.Process
  $process.StartInfo = $psi

  if (-not $process.Start()) {
    throw "Echec du lancement de $ScriptPath"
  }

  $state.Mode = $Mode
  $state.Process = $process
  $state.ActiveLogPath = $LogPath
  $state.LastLogContent = ''

  $labelStatus.Text = $StatusText
  Set-RunnerBusy -Busy $true
  $timer.Start()
}

function Start-DiagnosticsRun {
  if ([string]::IsNullOrWhiteSpace($comboTechnician.Text)) {
    [System.Windows.Forms.MessageBox]::Show('Selectionne un technicien.', $config.title, 'OK', 'Warning') | Out-Null
    return
  }

  $categoryMap = @{
    'Auto' = 'auto'
    'Portable' = 'laptop'
    'Tour' = 'desktop'
  }
  $modeMap = @{
    'Rapide' = 'quick'
    'Stress' = 'stress'
  }

  $category = $categoryMap[$comboCategory.Text]
  if (-not $category) { $category = 'auto' }
  $mode = $modeMap[$comboMode.Text]
  if (-not $mode) { $mode = 'quick' }

  $logPath = New-MdtRunnerLogPath -LogsRoot $config.logsRoot -Prefix 'runner'
  $args = New-MdtRunnerProcessArguments `
    -ScriptPath $config.reportScriptPath `
    -ApiUrl $config.apiUrl `
    -Technician $comboTechnician.Text.Trim() `
    -Category $category `
    -TestMode $mode `
    -LogPath $logPath `
    -OutboxRoot $config.outboxRoot `
    -SkipTlsValidation:$config.skipTlsValidation `
    -FactoryReset:$checkReset.Checked

  Add-UiLog -Target $textLog -Clear -Message ("Execution lancee pour {0} ({1}/{2})" -f $comboTechnician.Text.Trim(), $comboCategory.Text, $comboMode.Text)
  Start-BackgroundProcess `
    -Mode 'run' `
    -ScriptPath $config.reportScriptPath `
    -ArgumentList $args `
    -LogPath $logPath `
    -StatusText 'Checks en cours...'
}

function Start-OutboxSync {
  $logPath = New-MdtRunnerLogPath -LogsRoot $config.logsRoot -Prefix 'sync'
  $args = @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $config.syncScriptPath,
    '-ApiUrl', $config.apiUrl,
    '-OutboxRoot', $config.outboxRoot,
    '-TimeoutSec', [string]$config.syncTimeoutSec,
    '-LogPath', $logPath
  )
  if ($config.skipTlsValidation) {
    $args += '-SkipTlsValidation'
  }

  Add-UiLog -Target $textLog -Clear -Message 'Synchronisation de la file en cours...'
  Start-BackgroundProcess `
    -Mode 'sync' `
    -ScriptPath $config.syncScriptPath `
    -ArgumentList $args `
    -LogPath $logPath `
    -StatusText 'Synchronisation de la file...'
}

$form = New-Object System.Windows.Forms.Form
$form.Text = $config.title
$form.StartPosition = 'CenterScreen'
$form.Size = New-Object System.Drawing.Size(1100, 760)
$form.MinimumSize = New-Object System.Drawing.Size(980, 680)
$form.BackColor = $colors.Background
$form.Font = New-Object System.Drawing.Font('Segoe UI', 9)

$panelHeader = New-Object System.Windows.Forms.Panel
$panelHeader.Dock = 'Top'
$panelHeader.Height = 92
$panelHeader.BackColor = $colors.Accent
$form.Controls.Add($panelHeader)

$labelTitle = New-Object System.Windows.Forms.Label
$labelTitle.Text = 'Relance des checks MDT'
$labelTitle.ForeColor = [System.Drawing.Color]::White
$labelTitle.Font = New-Object System.Drawing.Font('Segoe UI Semibold', 22)
$labelTitle.Location = New-Object System.Drawing.Point(24, 18)
$labelTitle.AutoSize = $true
$panelHeader.Controls.Add($labelTitle)

$labelSubtitle = New-Object System.Windows.Forms.Label
$labelSubtitle.Text = 'Application metier locale pour rejouer le diagnostic, conserver les rapports hors ligne et lancer la reinitialisation uniquement sur demande.'
$labelSubtitle.ForeColor = [System.Drawing.Color]::FromArgb(218, 229, 241)
$labelSubtitle.Font = New-Object System.Drawing.Font('Segoe UI', 10)
$labelSubtitle.Location = New-Object System.Drawing.Point(28, 58)
$labelSubtitle.AutoSize = $true
$panelHeader.Controls.Add($labelSubtitle)

$panelContent = New-Object System.Windows.Forms.Panel
$panelContent.Dock = 'Fill'
$panelContent.Padding = New-Object System.Windows.Forms.Padding(18, 18, 18, 18)
$form.Controls.Add($panelContent)

$panelSession = New-Object System.Windows.Forms.Panel
$panelSession.BackColor = $colors.Surface
$panelSession.BorderStyle = 'FixedSingle'
$panelSession.Height = 170
$panelSession.Dock = 'Top'
$panelSession.Padding = New-Object System.Windows.Forms.Padding(20, 18, 20, 18)
$panelContent.Controls.Add($panelSession)

$labelSection = New-Object System.Windows.Forms.Label
$labelSection.Text = 'Session de controle'
$labelSection.Font = New-Object System.Drawing.Font('Segoe UI Semibold', 13)
$labelSection.ForeColor = $colors.Text
$labelSection.AutoSize = $true
$labelSection.Location = New-Object System.Drawing.Point(16, 12)
$panelSession.Controls.Add($labelSection)

$labelTechnician = New-Object System.Windows.Forms.Label
$labelTechnician.Text = 'Technicien'
$labelTechnician.ForeColor = $colors.Muted
$labelTechnician.Location = New-Object System.Drawing.Point(20, 52)
$labelTechnician.AutoSize = $true
$panelSession.Controls.Add($labelTechnician)

$comboTechnician = New-Object System.Windows.Forms.ComboBox
$comboTechnician.DropDownStyle = 'DropDownList'
$comboTechnician.Location = New-Object System.Drawing.Point(20, 74)
$comboTechnician.Size = New-Object System.Drawing.Size(240, 30)
$comboTechnician.FlatStyle = 'Flat'
[void]$comboTechnician.Items.AddRange($technicians)
if ($comboTechnician.Items.Count -gt 0) { $comboTechnician.SelectedIndex = 0 }
$panelSession.Controls.Add($comboTechnician)

$labelCategory = New-Object System.Windows.Forms.Label
$labelCategory.Text = 'Type de poste'
$labelCategory.ForeColor = $colors.Muted
$labelCategory.Location = New-Object System.Drawing.Point(290, 52)
$labelCategory.AutoSize = $true
$panelSession.Controls.Add($labelCategory)

$comboCategory = New-Object System.Windows.Forms.ComboBox
$comboCategory.DropDownStyle = 'DropDownList'
$comboCategory.Location = New-Object System.Drawing.Point(290, 74)
$comboCategory.Size = New-Object System.Drawing.Size(170, 30)
$comboCategory.FlatStyle = 'Flat'
[void]$comboCategory.Items.AddRange(@('Auto', 'Portable', 'Tour'))
$categoryDefaultIndex = @{ auto = 0; laptop = 1; desktop = 2 }[$config.defaultCategory]
if ($categoryDefaultIndex -eq $null) { $categoryDefaultIndex = 0 }
$comboCategory.SelectedIndex = $categoryDefaultIndex
$panelSession.Controls.Add($comboCategory)

$labelMode = New-Object System.Windows.Forms.Label
$labelMode.Text = 'Mode de test'
$labelMode.ForeColor = $colors.Muted
$labelMode.Location = New-Object System.Drawing.Point(490, 52)
$labelMode.AutoSize = $true
$panelSession.Controls.Add($labelMode)

$comboMode = New-Object System.Windows.Forms.ComboBox
$comboMode.DropDownStyle = 'DropDownList'
$comboMode.Location = New-Object System.Drawing.Point(490, 74)
$comboMode.Size = New-Object System.Drawing.Size(150, 30)
$comboMode.FlatStyle = 'Flat'
[void]$comboMode.Items.AddRange(@('Rapide', 'Stress'))
$modeDefaultIndex = @{ quick = 0; stress = 1 }[$config.defaultTestMode]
if ($modeDefaultIndex -eq $null) { $modeDefaultIndex = 0 }
$comboMode.SelectedIndex = $modeDefaultIndex
$panelSession.Controls.Add($comboMode)

$checkReset = New-Object System.Windows.Forms.CheckBox
$checkReset.Text = 'Reinitialisation usine a la fin'
$checkReset.ForeColor = $colors.Text
$checkReset.Location = New-Object System.Drawing.Point(20, 118)
$checkReset.AutoSize = $true
$panelSession.Controls.Add($checkReset)

$buttonLaunch = New-Object System.Windows.Forms.Button
$buttonLaunch.Text = 'Lancer les checks'
$buttonLaunch.BackColor = $colors.Primary
$buttonLaunch.ForeColor = [System.Drawing.Color]::White
$buttonLaunch.FlatStyle = 'Flat'
$buttonLaunch.Location = New-Object System.Drawing.Point(720, 68)
$buttonLaunch.Size = New-Object System.Drawing.Size(150, 38)
$buttonLaunch.Add_Click({ Start-DiagnosticsRun })
$panelSession.Controls.Add($buttonLaunch)

$buttonSync = New-Object System.Windows.Forms.Button
$buttonSync.Text = 'Synchroniser la file'
$buttonSync.BackColor = $colors.Surface
$buttonSync.ForeColor = $colors.Accent
$buttonSync.FlatStyle = 'Flat'
$buttonSync.Location = New-Object System.Drawing.Point(884, 68)
$buttonSync.Size = New-Object System.Drawing.Size(160, 38)
$buttonSync.Add_Click({ Start-OutboxSync })
$panelSession.Controls.Add($buttonSync)

$buttonOpenLogs = New-Object System.Windows.Forms.Button
$buttonOpenLogs.Text = 'Ouvrir les logs'
$buttonOpenLogs.BackColor = $colors.Surface
$buttonOpenLogs.ForeColor = $colors.Text
$buttonOpenLogs.FlatStyle = 'Flat'
$buttonOpenLogs.Location = New-Object System.Drawing.Point(720, 114)
$buttonOpenLogs.Size = New-Object System.Drawing.Size(150, 32)
$buttonOpenLogs.Add_Click({
  Start-Process explorer.exe $config.logsRoot | Out-Null
})
$panelSession.Controls.Add($buttonOpenLogs)

$buttonOpenQueue = New-Object System.Windows.Forms.Button
$buttonOpenQueue.Text = 'Ouvrir la file'
$buttonOpenQueue.BackColor = $colors.Surface
$buttonOpenQueue.ForeColor = $colors.Text
$buttonOpenQueue.FlatStyle = 'Flat'
$buttonOpenQueue.Location = New-Object System.Drawing.Point(884, 114)
$buttonOpenQueue.Size = New-Object System.Drawing.Size(160, 32)
$buttonOpenQueue.Add_Click({
  Start-Process explorer.exe $config.outboxRoot | Out-Null
})
$panelSession.Controls.Add($buttonOpenQueue)

$labelStatus = New-Object System.Windows.Forms.Label
$labelStatus.Text = 'Pret'
$labelStatus.ForeColor = $colors.Success
$labelStatus.Font = New-Object System.Drawing.Font('Segoe UI Semibold', 10)
$labelStatus.Location = New-Object System.Drawing.Point(720, 24)
$labelStatus.AutoSize = $true
$panelSession.Controls.Add($labelStatus)

$tab = New-Object System.Windows.Forms.TabControl
$tab.Dock = 'Fill'
$tab.Location = New-Object System.Drawing.Point(0, 190)
$panelContent.Controls.Add($tab)

$tabLog = New-Object System.Windows.Forms.TabPage
$tabLog.Text = 'Journal'
$tab.Controls.Add($tabLog)

$textLog = New-Object System.Windows.Forms.TextBox
$textLog.Multiline = $true
$textLog.ReadOnly = $true
$textLog.ScrollBars = 'Vertical'
$textLog.Dock = 'Fill'
$textLog.Font = New-Object System.Drawing.Font('Consolas', 9)
$tabLog.Controls.Add($textLog)

$tabQueue = New-Object System.Windows.Forms.TabPage
$tabQueue.Text = 'File hors ligne'
$tab.Controls.Add($tabQueue)

$labelQueueStats = New-Object System.Windows.Forms.Label
$labelQueueStats.Dock = 'Top'
$labelQueueStats.Height = 28
$labelQueueStats.Padding = New-Object System.Windows.Forms.Padding(8, 6, 8, 6)
$labelQueueStats.ForeColor = $colors.Text
$tabQueue.Controls.Add($labelQueueStats)

$listQueue = New-Object System.Windows.Forms.ListView
$listQueue.View = 'Details'
$listQueue.FullRowSelect = $true
$listQueue.GridLines = $true
$listQueue.Dock = 'Fill'
[void]$listQueue.Columns.Add('Entree', 200)
[void]$listQueue.Columns.Add('Technicien', 120)
[void]$listQueue.Columns.Add('Poste', 160)
[void]$listQueue.Columns.Add('Mise en attente', 180)
[void]$listQueue.Columns.Add('Tentatives', 80)
[void]$listQueue.Columns.Add('Derniere erreur', 320)
$tabQueue.Controls.Add($listQueue)

$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 1000
$timer.Add_Tick({
  if ($state.ActiveLogPath -and (Test-Path -Path $state.ActiveLogPath)) {
    try {
      $content = Get-Content -Path $state.ActiveLogPath -Raw -ErrorAction Stop
      if ($content -ne $state.LastLogContent) {
        $state.LastLogContent = $content
        $textLog.Text = $content
        $textLog.SelectionStart = $textLog.TextLength
        $textLog.ScrollToCaret()
      }
    } catch { }
  }

  if ($state.Process -and $state.Process.HasExited) {
    $timer.Stop()
    $exitCode = $state.Process.ExitCode
    $finishedMode = $state.Mode
    $state.Process.Dispose()
    $state.Process = $null
    $state.Mode = $null
    Set-RunnerBusy -Busy $false
    Refresh-OutboxView

    if ($exitCode -eq 0) {
      $labelStatus.ForeColor = $colors.Success
      if ($finishedMode -eq 'sync') {
        $labelStatus.Text = 'Synchronisation terminee'
      } else {
        $labelStatus.Text = 'Checks termines'
      }
    } else {
      $labelStatus.ForeColor = $colors.Danger
      $labelStatus.Text = "Processus termine avec code $exitCode"
    }
  }
})

$form.Add_Shown({
  Refresh-OutboxView
  Add-UiLog -Target $textLog -Message ("API: {0}" -f $config.apiUrl)
  Add-UiLog -Target $textLog -Message ("Scripts: {0}" -f $config.scriptsDir)
  Add-UiLog -Target $textLog -Message ("File locale: {0}" -f $config.outboxRoot)
  if ($config.autoSyncOnStart) {
    Start-OutboxSync
  }
})

[void]$form.ShowDialog()
