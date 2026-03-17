param(
    [string]$PythonExe = "python",
    [string]$VenvPath = ".venv",
    [string]$OllamaModel = "qwen2.5-coder:14b",
    [string]$OllamaEmbeddingModel = "bge-m3",
    [switch]$SkipVenv,
    [switch]$SkipOllama
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
    Write-Host "[AFM Setup] $Message" -ForegroundColor Cyan
}

function Write-Warn([string]$Message) {
    Write-Host "[AFM Setup] $Message" -ForegroundColor Yellow
}

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $projectRoot

Write-Step "Project root: $projectRoot"

$pythonCommand = Get-Command $PythonExe -ErrorAction SilentlyContinue
if (-not $pythonCommand) {
    throw "Python executable '$PythonExe' not found. Install Python 3.10+ or pass -PythonExe."
}

$pythonForPip = $PythonExe
if (-not $SkipVenv) {
    $venvPython = Join-Path $projectRoot "$VenvPath\Scripts\python.exe"

    if (-not (Test-Path $venvPython)) {
        Write-Step "Creating virtual environment at '$VenvPath'"
        & $PythonExe -m venv $VenvPath
    }
    else {
        Write-Step "Virtual environment already exists at '$VenvPath'"
    }

    if (-not (Test-Path $venvPython)) {
        throw "Virtual environment Python not found at '$venvPython'."
    }

    $pythonForPip = $venvPython
}
else {
    Write-Warn "Skipping virtual environment creation. Installing into current Python environment."
}

Write-Step "Upgrading pip"
& $pythonForPip -m pip install --upgrade pip

Write-Step "Installing dependencies from requirements.txt"
& $pythonForPip -m pip install -r requirements.txt

if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Copy-Item ".env.example" ".env"
        Write-Step "Created .env from .env.example"
    }
    else {
        Write-Warn ".env.example not found. Create .env manually."
    }
}
else {
    Write-Step ".env already exists. Keeping current values."
}

if (-not $SkipOllama) {
    $ollamaCommand = Get-Command "ollama" -ErrorAction SilentlyContinue
    if ($ollamaCommand) {
        Write-Step "Pulling Ollama model '$OllamaModel'"
        & ollama pull $OllamaModel

        Write-Step "Pulling Ollama embedding model '$OllamaEmbeddingModel'"
        & ollama pull $OllamaEmbeddingModel
    }
    else {
        Write-Warn "Ollama CLI not found. Install Ollama and run: ollama pull $OllamaModel"
        Write-Warn "Then pull embedding model: ollama pull $OllamaEmbeddingModel"
    }
}
else {
    Write-Warn "Skipping Ollama model pull."
}

Write-Host ""
Write-Host "Setup complete." -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Green
if (-not $SkipVenv) {
    Write-Host "1) Activate venv: .\$VenvPath\Scripts\Activate.ps1"
    Write-Host "2) Edit .env (set AFM_PG_DSN and model/provider settings)"
    Write-Host "3) Run ingestion: python scripts/ingest_cli.py --data data"
    Write-Host "4) Seed catalog: python scripts/seed_catalog.py"
    Write-Host "5) Query NL2SQL: python scripts/query_cli.py \"платежи по займам больше 5 млн за 2024\""
}
else {
    Write-Host "1) Edit .env (set AFM_PG_DSN and model/provider settings)"
    Write-Host "2) Run ingestion: $PythonExe scripts/ingest_cli.py --data data"
    Write-Host "3) Seed catalog: $PythonExe scripts/seed_catalog.py"
    Write-Host "4) Query NL2SQL: $PythonExe scripts/query_cli.py \"платежи по займам больше 5 млн за 2024\""
}
