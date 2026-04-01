[CmdletBinding()]
param(
  [string]$ConfigPath = "$PSScriptRoot\mma-mdt-agent.json",
  [int]$LoopDelaySeconds = 15
)

$ErrorActionPreference = 'Stop'

function Write-AgentLog {
  param(
    [string]$Level,
    [string]$Message
  )

  $configDir = Split-Path -Parent $ConfigPath
  $logDir = Join-Path $configDir 'logs'
  if (-not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
  }
  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$timestamp] [$Level] $Message"
  Add-Content -Path (Join-Path $logDir 'agent.log') -Value $line
  Write-Host $line
}

function Enable-InsecureTlsIfNeeded {
  param(
    [bool]$IgnoreTlsErrors
  )

  if (-not $IgnoreTlsErrors) {
    return
  }

  try {
    Add-Type @"
using System.Net;
using System.Security.Cryptography.X509Certificates;
public static class TrustAllCertsPolicy {
  public static bool Callback(object sender, X509Certificate cert, X509Chain chain, System.Net.Security.SslPolicyErrors errors) {
    return true;
  }
}
"@ -ErrorAction SilentlyContinue | Out-Null
    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { param($sender, $certificate, $chain, $errors) return $true }
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
  } catch {
    Write-AgentLog -Level 'WARN' -Message "Impossible d'activer le mode TLS permissif: $($_.Exception.Message)"
  }
}

function Get-AgentConfig {
  if (-not (Test-Path $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
  }
  $raw = Get-Content $ConfigPath -Raw
  $config = $raw | ConvertFrom-Json
  foreach ($key in @('baseUrl', 'agentId', 'token', 'deploymentShareRoot', 'shareServerName')) {
    if (-not $config.$key) {
      throw "Missing config key: $key"
    }
  }
  if (-not $config.provisionScriptPath) {
    $config | Add-Member -NotePropertyName provisionScriptPath -NotePropertyValue (Join-Path $PSScriptRoot 'provision-mdt-beta.ps1')
  }
  if (-not $config.taskSequenceGroupName) {
    $config | Add-Member -NotePropertyName taskSequenceGroupName -NotePropertyValue 'MMA Beta'
  }
  if (-not $config.scriptsFolder) {
    $config | Add-Member -NotePropertyName scriptsFolder -NotePropertyValue 'beta'
  }
  if (-not $config.hostname) {
    $config | Add-Member -NotePropertyName hostname -NotePropertyValue $env:COMPUTERNAME
  }
  if (-not $config.loopDelaySeconds) {
    $config | Add-Member -NotePropertyName loopDelaySeconds -NotePropertyValue $LoopDelaySeconds
  }
  return $config
}

function Get-AgentHeaders {
  param($Config)
  return @{
    'x-mdt-beta-agent-token' = [string]$Config.token
    'x-mdt-beta-agent-id' = [string]$Config.agentId
  }
}

function Invoke-AgentRequest {
  param(
    $Config,
    [string]$Method,
    [string]$Path,
    $Body = @{}
  )

  $uri = ([string]$Config.baseUrl).TrimEnd('/') + $Path
  $json = $Body | ConvertTo-Json -Depth 8
  return Invoke-RestMethod -Uri $uri -Method $Method -Headers (Get-AgentHeaders -Config $Config) -ContentType 'application/json' -Body $json -TimeoutSec 30
}

function Send-AgentHeartbeat {
  param(
    $Config,
    [string]$Status = 'idle',
    [string]$LastJobId = $null,
    [string]$LastError = $null
  )

  $body = @{
    agentId = $Config.agentId
    hostname = $Config.hostname
    deploymentShareRoot = $Config.deploymentShareRoot
    taskSequenceGroupName = $Config.taskSequenceGroupName
    scriptsFolder = $Config.scriptsFolder
    status = $Status
  }
  if ($LastJobId) {
    $body.lastJobId = $LastJobId
  }
  if ($LastError) {
    $body.lastError = $LastError
  }
  Invoke-AgentRequest -Config $Config -Method 'POST' -Path '/api/mdt-beta-agent/heartbeat' -Body $body | Out-Null
}

function Claim-AgentJob {
  param($Config)
  $body = @{
    agentId = $Config.agentId
    hostname = $Config.hostname
    deploymentShareRoot = $Config.deploymentShareRoot
    taskSequenceGroupName = $Config.taskSequenceGroupName
    scriptsFolder = $Config.scriptsFolder
  }
  return Invoke-AgentRequest -Config $Config -Method 'POST' -Path '/api/mdt-beta-agent/jobs/claim' -Body $body
}

function Complete-AgentJob {
  param(
    $Config,
    [string]$JobId,
    $Result
  )

  $body = @{
    agentId = $Config.agentId
    hostname = $Config.hostname
    deploymentShareRoot = $Config.deploymentShareRoot
    taskSequenceGroupName = $Config.taskSequenceGroupName
    scriptsFolder = $Config.scriptsFolder
    result = $Result
  }
  Invoke-AgentRequest -Config $Config -Method 'POST' -Path "/api/mdt-beta-agent/jobs/$JobId/complete" -Body $body | Out-Null
}

function Fail-AgentJob {
  param(
    $Config,
    [string]$JobId,
    [string]$ErrorMessage,
    $Result = @{}
  )

  $body = @{
    agentId = $Config.agentId
    hostname = $Config.hostname
    deploymentShareRoot = $Config.deploymentShareRoot
    taskSequenceGroupName = $Config.taskSequenceGroupName
    scriptsFolder = $Config.scriptsFolder
    error = $ErrorMessage
    result = $Result
  }
  Invoke-AgentRequest -Config $Config -Method 'POST' -Path "/api/mdt-beta-agent/jobs/$JobId/fail" -Body $body | Out-Null
}

function Invoke-ProvisionJob {
  param(
    $Config,
    $Job
  )

  $payload = $Job.payload
  if (-not $payload) {
    throw 'Missing job payload'
  }
  if (-not (Test-Path $Config.provisionScriptPath)) {
    throw "Provision script not found: $($Config.provisionScriptPath)"
  }

  $params = @{
    DeploymentShareRoot = [string]$Config.deploymentShareRoot
    ShareServerName = [string]$Config.shareServerName
    SourceTaskSequenceId = [string]$payload.sourceTaskSequenceId
    DestinationTaskSequenceId = [string]$payload.destinationTaskSequenceId
    DestinationTaskSequenceName = [string]$payload.destinationTaskSequenceName
    TaskSequenceGroupName = [string]$payload.taskSequenceGroupName
    TechnicianDisplayName = [string]$payload.displayName
    BetaScriptsFolder = [string]$payload.betaScriptsFolder
  }

  $result = & $Config.provisionScriptPath @params
  if ($result -is [System.Array]) {
    $result = $result | Select-Object -Last 1
  }
  if (-not $result) {
    throw 'Provision script did not return a result'
  }

  return @{
    sourceTaskSequenceId = [string]$result.SourceTaskSequenceId
    destinationTaskSequenceId = [string]$result.DestinationTaskSequenceId
    destinationTaskSequenceName = [string]$result.DestinationTaskSequenceName
    technicianDisplayName = [string]$result.TechnicianDisplayName
    taskSequenceGroupName = [string]$result.TaskSequenceGroupName
    deploymentShareRoot = [string]$result.DeploymentShareRoot
    controlPath = [string]$result.ControlPath
    taskSequencesPath = [string]$result.TaskSequencesPath
    taskSequenceGroupsPath = [string]$result.TaskSequenceGroupsPath
    backupPath = [string]$result.BackupPath
    scriptsFolder = [string]$result.ScriptsFolder
  }
}

$config = Get-AgentConfig
Enable-InsecureTlsIfNeeded -IgnoreTlsErrors ([bool]$config.ignoreTlsErrors)
Write-AgentLog -Level 'INFO' -Message "Agent start ($($config.agentId))"

while ($true) {
  try {
    Send-AgentHeartbeat -Config $config -Status 'idle'
    $claim = Claim-AgentJob -Config $config
    if ($claim -and $claim.job -and $claim.job.id) {
      $jobId = [string]$claim.job.id
      $jobType = [string]$claim.job.type
      Write-AgentLog -Level 'INFO' -Message "Claimed job $jobId ($jobType)"
      try {
        Send-AgentHeartbeat -Config $config -Status 'running' -LastJobId $jobId
        $result = Invoke-ProvisionJob -Config $config -Job $claim.job
        Complete-AgentJob -Config $config -JobId $jobId -Result $result
        Send-AgentHeartbeat -Config $config -Status 'idle' -LastJobId $jobId
        Write-AgentLog -Level 'INFO' -Message "Job $jobId completed"
      } catch {
        $message = $_.Exception.Message
        Fail-AgentJob -Config $config -JobId $jobId -ErrorMessage $message
        Send-AgentHeartbeat -Config $config -Status 'idle' -LastJobId $jobId -LastError $message
        Write-AgentLog -Level 'ERROR' -Message "Job $jobId failed: $message"
      }
    }
  } catch {
    Write-AgentLog -Level 'ERROR' -Message $_.Exception.Message
  }
  Start-Sleep -Seconds ([int]$config.loopDelaySeconds)
}
