<#
.SYNOPSIS
    Script para garantir que o serviço SQL Server esteja ativado e em execução.
.DESCRIPTION
    Eleva privilégios, configura o serviço como 'Automatic' e inicia o MSSQLSERVER e SQLSERVERAGENT.
#>

# Verifica se está em modo Administrador. Se não, reinicia o script elevando privilégios.
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "Elevando privilégios para Administrador..."
    Start-Process -FilePath PowerShell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

Write-Host "Privilégios Administrativos confirmados." -ForegroundColor Green

# Define serviços a configurar e iniciar
$services = @("MSSQLSERVER", "SQLSERVERAGENT")

foreach ($svc in $services) {
    try {
        Write-Host "Configurando $svc para iniciar automaticamente..." -NoNewline
        Set-Service -Name $svc -StartupType Automatic
        Write-Host " [OK]" -ForegroundColor Green

        Write-Host "Iniciando serviço $svc..." -NoNewline
        Start-Service -Name $svc -ErrorAction Stop
        Write-Host " [Iniciado]" -ForegroundColor Green
    } catch {
        Write-Host " [Falha]" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Yellow
    }
}

Write-Host "Verificando status dos serviços..." -ForegroundColor Cyan
Get-Service -Name $services[0], $services[1], "SQLBrowser" | Format-Table Name, Status, StartType

Write-Host "Script concluído." -ForegroundColor Green
