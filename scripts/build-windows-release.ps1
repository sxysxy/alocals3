$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RootDir

$OutDir = if ($env:OUT_DIR) { $env:OUT_DIR } else { "dist" }
$Target = if ($env:TARGET) { $env:TARGET } else { "" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Python312 = if ($env:PYTHON) {
    $env:PYTHON
} else {
    (& py -3.12 -c "import sys; print(sys.executable)").Trim()
}

Write-Host "==> Building Windows server"
$OldPyo3NoPython = $env:PYO3_NO_PYTHON
$env:PYO3_NO_PYTHON = "1"
$CargoArgs = @(
    "build",
    "--release",
    "--locked",
    "--no-default-features",
    "--features",
    "server,server-binary",
    "--bin",
    "alocals3-server"
)
if ($Target) {
    $CargoArgs += @("--target", $Target)
}
try {
    & cargo @CargoArgs
} finally {
    $env:PYO3_NO_PYTHON = $OldPyo3NoPython
}

if ($Target) {
    $ServerSrc = Join-Path "target" (Join-Path $Target "release\alocals3-server.exe")
    $ServerDst = Join-Path $OutDir "alocals3-server-windows-$Target.exe"
} else {
    $ServerSrc = "target\release\alocals3-server.exe"
    $ServerDst = Join-Path $OutDir "alocals3-server-windows.exe"
}
Copy-Item -Force $ServerSrc $ServerDst

Write-Host "==> Building Windows cp312 abi3 wheel"
& $Python312 -m pip install --upgrade "maturin>=1.7,<2"
& $Python312 -m maturin build `
    --release `
    --locked `
    --features extension-module `
    --interpreter $Python312 `
    --out $OutDir

Write-Host "==> Artifacts"
Get-ChildItem $OutDir -Filter "alocals3-server-windows*.exe"
Get-ChildItem $OutDir -Filter "*.whl"
