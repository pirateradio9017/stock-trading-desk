param(
    [switch]$SkipPipeline,
    [switch]$NoLlm,
    [switch]$PublishKv,
    [int]$Port = 8787
)

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $PSScriptRoot
$PyExe = Join-Path $RepoDir ".venv\Scripts\python.exe"

if (-not (Test-Path $PyExe)) {
    throw "Missing virtualenv python: $PyExe"
}

Push-Location $RepoDir
try {
    $env:DESK_PORT = "$Port"
    if (-not $SkipPipeline) {
        $pipeArgs = @("-m", "src.pipeline")
        if ($NoLlm) { $pipeArgs += "--no-llm" }
        if ($PublishKv) { $pipeArgs += "--publish-kv" }
        Write-Host "[run_local] Running pipeline: $PyExe $($pipeArgs -join ' ')"
        & $PyExe @pipeArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Pipeline failed with exit code $LASTEXITCODE"
        }
    }
    Write-Host "[run_local] Starting FastAPI server on http://127.0.0.1:$Port"
    & $PyExe -m src.server
}
finally {
    Pop-Location
}
