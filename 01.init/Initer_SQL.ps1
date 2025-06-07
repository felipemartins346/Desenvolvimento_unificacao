<#
.SYNOPSIS
    Inicia e configura serviços do SQL Server com log detalhado.
.DESCRIPTION
    - Eleva privilégios para Administrador sem prompt extra.
    - Ajusta ExecutionPolicy para Bypass na sessão.
    - Desbloqueia o próprio script.
    - Configura StartupType para Automatic em MSSQLSERVER e SQLSERVERAGENT.
    - Inicia ambos os serviços.
    - Exibe status no console e registra tudo em arquivo de log organizado.
.EXAMPLE
    .\Initer_SQL.ps1
#>

# 1. Autoelevação para Administrador (se necessário)
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "Elevando privilégios para Administrador..." -ForegroundColor Yellow
    Start-Process -FilePath PowerShell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# 2. Ajusta ExecutionPolicy para esta sessão
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# 3. Desbloqueia o script atual, se bloqueado
Unblock-File -Path $PSCommandPath

# 4. Configura log
$scriptDir = Split-Path -Parent $PSCommandPath
$logFile   = Join-Path $scriptDir "Initer_SQL.log"
# Inicia novo log
"========== Initer_SQL Log ==========" | Out-File -FilePath $logFile -Encoding UTF8
"Log iniciado em: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding UTF8

function Write-Log {
    param(
        [string]$Msg,
        [ValidateSet('INFO','WARN','ERROR')]
        [string]$Level = 'INFO'
    )
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $entry = "[$timestamp] [$Level] $Msg"
    # Escreve no console e no log
    switch ($Level) {
        'ERROR' { Write-Host $entry -ForegroundColor Red }
        'WARN'  { Write-Host $entry -ForegroundColor Yellow }
        default { Write-Host $entry -ForegroundColor Gray }
    }
    $entry | Out-File -FilePath $logFile -Append -Encoding UTF8
}

# 5. Operações nos serviços SQL Server
Write-Log "Iniciando configuração dos serviços SQL Server..."
$services = @('MSSQLSERVER','SQLSERVERAGENT')
foreach ($svc in $services) {
    Write-Log "Configurando StartupType para ${svc}..." 'INFO'
    try {
        Set-Service -Name $svc -StartupType Automatic -ErrorAction Stop
        Write-Log "${svc} configurado para StartupType Automatic" 'INFO'
    } catch {
        Write-Log "Falha ao configurar StartupType para ${svc}: $($_.Exception.Message)" 'ERROR'
    }

    Write-Log "Iniciando serviço ${svc}..." 'INFO'
    try {
        Start-Service -Name $svc -ErrorAction Stop
        Write-Log "${svc} iniciado com sucesso" 'INFO'
    } catch {
        Write-Log "Falha ao iniciar ${svc}: $($_.Exception.Message)" 'ERROR'
    }
}

# 6. Verifica status dos serviços
Write-Log "Verificando status dos serviços SQL Server..." 'INFO'
foreach ($svc in $services) {
    try {
        $status = (Get-Service -Name $svc -ErrorAction Stop).Status
        Write-Log "${svc} status: $status" 'INFO'
    } catch {
        Write-Log "Falha ao obter status de ${svc}: $($_.Exception.Message)" 'ERROR'
    }
}

# 7. Conclusão
Write-Log "Initer_SQL concluído." 'INFO'
"====================================" | Out-File -FilePath $logFile -Append -Encoding UTF8
