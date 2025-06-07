<#
.SYNOPSIS
    Cria e ativa o ambiente virtual Python e instala dependências
.DESCRIPTION
    - Verifica Python instalado
    - Cria venv se não existir
    - Ativa o venv
    - Instala pacotes do requirements.txt
#>
Param(
    [string]$venvDir = ".venv",
    [string]$reqFile = "requirements.txt"
)

# Verifica se Python está disponível
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "Python não encontrado. Instale o Python 3.10+ antes de executar este script." -ForegroundColor Red
    exit 1
}

# Cria venv se não existir
if (-not (Test-Path $venvDir)) {
    Write-Host "Criando ambiente virtual em '$venvDir'..." -NoNewline
    python -m venv $venvDir
    Write-Host " [OK]" -ForegroundColor Green
} else {
    Write-Host "Ambiente virtual já existe em '$venvDir'." -ForegroundColor Yellow
}

# Ativa venv
$activateScript = Join-Path $venvDir "Scripts\Activate.ps1"
if (Test-Path $activateScript) {
    Write-Host "Ativando ambiente virtual..." -NoNewline
    & $activateScript
    Write-Host " [Ativado]" -ForegroundColor Green
} else {
    Write-Host "Script de ativação não encontrado em '$activateScript'." -ForegroundColor Red
    exit 1
}

# Instala dependências
if (Test-Path $reqFile) {
    Write-Host "Instalando dependências de '$reqFile'..." -NoNewline
    pip install -r $reqFile
    Write-Host " [Concluído]" -ForegroundColor Green
} else {
    Write-Host "Arquivo de requisitos '$reqFile' não encontrado." -ForegroundColor Yellow
}

Write-Host "Setup do ambiente Python concluído." -ForegroundColor Cyan
