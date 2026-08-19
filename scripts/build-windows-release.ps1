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
    "alocals3-server",
    "--bin",
    "alocals3-migrate2pg"
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
} else {
    $ServerSrc = "target\release\alocals3-server.exe"
}
$ServerDst = Join-Path $OutDir "alocals3-server.exe"
$MigrateSrc = if ($Target) { Join-Path "target" (Join-Path $Target "release\alocals3-migrate2pg.exe") } else { "target\release\alocals3-migrate2pg.exe" }
$MigrateDst = Join-Path $OutDir "alocals3-migrate2pg.exe"
Copy-Item -Force $ServerSrc $ServerDst
Copy-Item -Force $MigrateSrc $MigrateDst
$PackageBinDir = Join-Path $RootDir "alocals3\bin"
if (Test-Path $PackageBinDir) {
    Remove-Item -Recurse -Force $PackageBinDir
}
New-Item -ItemType Directory -Force -Path $PackageBinDir | Out-Null
Copy-Item -Force $ServerSrc (Join-Path $PackageBinDir "alocals3-server.exe")
Copy-Item -Force $MigrateSrc (Join-Path $PackageBinDir "alocals3-migrate2pg.exe")

Write-Host "==> Building Windows cp312 abi3 wheel"
& $Python312 -m pip install --upgrade "maturin>=1.7,<2"
& $Python312 -m maturin build `
    --release `
    --locked `
    --features extension-module `
    --interpreter $Python312 `
    --out $OutDir

Write-Host "==> Artifacts"
Get-ChildItem $OutDir -Filter "alocals3-server.exe"
Get-ChildItem $OutDir -Filter "alocals3-migrate2pg.exe"
Get-ChildItem $OutDir -Filter "*.whl"
