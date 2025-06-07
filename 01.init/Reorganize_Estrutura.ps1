<#
.SYNOPSIS
    Reorganiza a estrutura do repositório: centraliza logs, limpa .vscode e cria src na raiz.
#>

# 1) Determina a raiz do projeto (uma pasta acima de 01.init)
$scriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

Push-Location $projectRoot

# 2) Garante que existam as pastas logs/ e src/ na raiz
if (-not (Test-Path .\logs)) { New-Item -ItemType Directory -Name logs | Out-Null }
if (-not (Test-Path .\src )) { New-Item -ItemType Directory -Name src  | Out-Null }

# 3) Move todos os .log de 01.init para logs/ na raiz
Get-ChildItem -Path ".\01.init" -Filter *.log -File | Move-Item -Destination ".\logs"

# 4) Limpa itens indesejados dentro de 01.init
Remove-Item -Recurse -Force ".\01.init\.vscode"  -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force ".\01.init\.venv"    -ErrorAction SilentlyContinue

Pop-Location

Write-Host "Reestruturação concluída em: $projectRoot" -ForegroundColor Green
