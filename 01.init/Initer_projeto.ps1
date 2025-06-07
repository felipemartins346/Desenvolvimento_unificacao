<#
.SYNOPSIS
    Orquestra todos os Initer_*.ps1 nesta pasta.
.DESCRIPTION
    - Bypass de ExecutionPolicy na sessão
    - Unblock-File no próprio script
    - UAC autoelevação para Administrador
    - Executa em ordem alfabética todos os Initer_*.ps1 (exceto Initer_projeto.ps1)
    - Log detalhado em Initer_projeto.log
#>

# 1) Preparação
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
Unblock-File -Path $PSCommandPath

# 2) UAC autoelevação
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    Write-Host 'Elevando privilégios para Administrador...' -ForegroundColor Yellow
    Start-Process PowerShell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# 3) Configuração de log
$root    = Split-Path -Parent $PSCommandPath
$logFile = Join-Path $root 'Initer_projeto.log'
"========== Initer_projeto Log ==========" | Out-File -FilePath $logFile -Encoding UTF8
"Log iniciado em: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append

function Write-Log {
    param(
        [string]$Msg,
        [ValidateSet('INFO','WARN','ERROR')][string]$Level = 'INFO'
    )
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $entry = "[$ts] [$Level] $Msg"
    switch ($Level) {
        'ERROR' { Write-Host $entry -ForegroundColor Red }
        'WARN'  { Write-Host $entry -ForegroundColor Yellow }
        default { Write-Host $entry -ForegroundColor Gray }
    }
    $entry | Out-File -FilePath $logFile -Append
}

# 4) Descobre e executa todos os Initer_*.ps1 exceto este
Push-Location $root
Get-ChildItem -Filter 'Initer_*.ps1' |
  Where-Object Name -ne 'Initer_projeto.ps1' |
  Sort-Object Name |
  ForEach-Object {
    $file = $_.Name
    Write-Log "Iniciando $file" 'INFO'
    try {
      & ".\$file" *>&1 | Tee-Object -FilePath $logFile -Append
      Write-Log "${file} concluído com sucesso" 'INFO'
    } catch {
      Write-Log "Erro ao executar ${file}: $($_.Exception.Message)" 'ERROR'
    }
  }
Pop-Location

# 5) Finalização
Write-Log 'Todas as etapas concluídas.' 'INFO'
"====================================" | Out-File -FilePath $logFile -Append
