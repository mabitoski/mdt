param(
  [string]$SharePath = "\\10.1.130.2\DeploymentShare$\Scripts\marl"
)

function Write-Info($Message) {
  Write-Host $Message -ForegroundColor Cyan
}

function Write-Warn($Message) {
  Write-Host $Message -ForegroundColor Yellow
}

if (-not (Test-Path -Path $SharePath)) {
  Write-Warn "Partage introuvable: $SharePath"
  exit 1
}

$items = Get-ChildItem -Path $SharePath -Filter "mdt-report-*.ps1" -File -ErrorAction Stop |
  Where-Object { $_.Name -notin @("mdt-report-admin.ps1", "mdt-report-launcher.ps1") } |
  Sort-Object Name

if (-not $items -or $items.Count -eq 0) {
  Write-Warn "Aucun script mdt-report- trouvé dans $SharePath"
  exit 1
}

Write-Info "Selectionne un script a lancer (mdt-report-*):"
for ($i = 0; $i -lt $items.Count; $i++) {
  Write-Host ("[{0}] {1}" -f ($i + 1), $items[$i].Name)
}

$selection = Read-Host "Numero du script"
$index = 0
if (-not [int]::TryParse($selection, [ref]$index)) {
  Write-Warn "Selection invalide."
  exit 1
}
$index = $index - 1
if ($index -lt 0 -or $index -ge $items.Count) {
  Write-Warn "Selection hors plage."
  exit 1
}

$scriptPath = $items[$index].FullName
Write-Info "Execution: $scriptPath"

try {
  Unblock-File -Path $scriptPath -ErrorAction SilentlyContinue
} catch {
  # Ignore unblock errors
}

$argList = @(
  "-NoProfile",
  "-ExecutionPolicy",
  "Bypass",
  "-File",
  $scriptPath
)
if ($args.Count -gt 0) {
  $argList += $args
}

$proc = Start-Process -FilePath "powershell.exe" -ArgumentList $argList -Wait -PassThru
exit $proc.ExitCode
