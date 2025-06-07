<# 
.SYNOPSIS
    Inicia e configura o ambiente virtual Python com log detalhado.
.DESCRIPTION
    - Eleva privilégios para Administrador sem prompt extra.
    - Ajusta ExecutionPolicy para Bypass na sessão.
    - Desbloqueia o próprio script.
    - Verifica instalação do Python.
    - Cria o venv se não existir e ativa.
    - Instala dependências listadas em requirements.txt.
    - Registra cada etapa em Initer_Venv.log.
#>

# 1) Autoelevação para Administrador
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    Write-Host 'Elevando privilégios para Administrador...' -ForegroundColor Yellow
    Start-Process PowerShell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# 2) Bypass ExecutionPolicy na sessão
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# 3) Desbloqueia este script
Unblock-File -Path $PSCommandPath

# 4) Prepara o log
$root    = Split-Path -Parent $PSCommandPath
$logFile = Join-Path $root 'Initer_Venv.log'
"========== Initer_Venv Log ==========" | Out-File -FilePath $logFile -Encoding UTF8
"Log iniciado em: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $logFile -Append

function Write-Log {
    param(
        [string]$Msg,
        [ValidateSet('INFO','WARN','ERROR')][string]$Level = 'INFO'
    )
    $ts    = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $entry = "[$ts] [$Level] $Msg"
    switch ($Level) {
        'ERROR' { Write-Host $entry -ForegroundColor Red }
        'WARN'  { Write-Host $entry -ForegroundColor Yellow }
        default { Write-Host $entry -ForegroundColor Gray }
    }
    $entry | Out-File -FilePath $logFile -Append
}

# 5) Verificar Python
Write-Log 'Verificando instalação do Python...' 'INFO'
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Log 'Python não encontrado. Abortando.' 'ERROR'
    exit 1
}
$pythonPath = (Get-Command python).Source
Write-Log "Python encontrado em: $pythonPath" 'INFO'

# 6) Criar ou reutilizar o venv
$venvDir = Join-Path $root '.venv'
if (-not (Test-Path $venvDir)) {
    Write-Log "Criando venv em '$venvDir'..." 'INFO'
    python -m venv $venvDir
    Write-Log 'Venv criado com sucesso.' 'INFO'
} else {
    Write-Log 'Venv já existe.' 'WARN'
}

# 7) Ativar o venv
$activateScript = Join-Path $venvDir 'Scripts\Activate.ps1'
if (Test-Path $activateScript) {
    Write-Log 'Ativando venv...' 'INFO'
    & $activateScript
    Write-Log 'Venv ativado.' 'INFO'
} else {
    Write-Log "Activate.ps1 não encontrado em '$activateScript'." 'ERROR'
    exit 1
}

# 8) Instalar dependências
$reqFile = Join-Path $root 'requirements.txt'
if (Test-Path $reqFile) {
    Write-Log 'Instalando dependências de requirements.txt...' 'INFO'
    pip install -r $reqFile | ForEach-Object { Write-Host $_ }
    Write-Log 'Dependências instaladas com sucesso.' 'INFO'
} else {
    Write-Log 'requirements.txt não encontrado.' 'WARN'
}

# 9) Conclusão
Write-Log 'Initer_Venv concluído.' 'INFO'
"====================================" | Out-File -FilePath $logFile -Append
