<#
.SYNOPSIS
    Configurações do VSCode para o projeto: aponta o Python interpreter para o venv.
.DESCRIPTION
    - Eleva privilégios para Administrador sem prompt extra.
    - Ajusta ExecutionPolicy para Bypass na sessão.
    - Desbloqueia o próprio script.
    - Cria a pasta .vscode, se não existir.
    - Gera o arquivo settings.json com python.pythonPath e python.venvPath.
    - Registra cada etapa em Initer_VSCode.log.
.EXAMPLE
    .\Initer_VSCode.ps1
#>

# 1. Autoelevação para Administrador
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    Write-Host 'Elevando privilégios para Administrador...' -ForegroundColor Yellow
    Start-Process -FilePath PowerShell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# 2. Ajusta ExecutionPolicy para esta sessão
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# 3. Desbloqueia o script atual
Unblock-File -Path $PSCommandPath

# 4. Configura log
$scriptDir = Split-Path -Parent $PSCommandPath
$logFile   = Join-Path $scriptDir 'Initer_VSCode.log'
"========== Initer_VSCode Log ==========" | Out-File -FilePath $logFile -Encoding UTF8
"Log iniciado em: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append -Encoding UTF8

function Write-Log {
    param(
        [string]$Msg,
        [ValidateSet('INFO','WARN','ERROR')]
        [string]$Level = 'INFO'
    )
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $entry = "[$timestamp] [$Level] $Msg"
    switch ($Level) {
        'ERROR' { Write-Host $entry -ForegroundColor Red }
        'WARN'  { Write-Host $entry -ForegroundColor Yellow }
        default { Write-Host $entry -ForegroundColor Gray }
    }
    $entry | Out-File -FilePath $logFile -Append -Encoding UTF8
}

# 5. Gerar pasta .vscode
$vscodeDir = Join-Path $scriptDir '.vscode'
if (-not (Test-Path $vscodeDir)) {
    Write-Host 'Criando pasta .vscode...' -ForegroundColor Gray
    New-Item -ItemType Directory -Path $vscodeDir | Out-Null
    Write-Log 'Pasta .vscode criada.' 'INFO'
} else {
    Write-Log 'Pasta .vscode já existe.' 'WARN'
}

# 6. Gera settings.json
$venvDir = Join-Path $scriptDir '.venv'
$pythonPath = Join-Path $venvDir 'Scripts\python.exe'
$settings = @{ 
    'python.pythonPath' = $pythonPath;
    'python.venvPath'   = $venvDir
}
$settingsFile = Join-Path $vscodeDir 'settings.json'
$settings | ConvertTo-Json -Depth 3 | Out-File -FilePath $settingsFile -Encoding UTF8
Write-Log "settings.json gerado em: $settingsFile" 'INFO'

# 7. Conclusão
Write-Log 'Initer_VSCode concluído.' 'INFO'
"====================================" | Out-File -FilePath $logFile -Append -Encoding UTF8
