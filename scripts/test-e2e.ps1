[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$postgresRoot = if ($env:E2E_POSTGRES_ROOT) { $env:E2E_POSTGRES_ROOT } else { Join-Path (Get-Location) 'build/e2e/postgres' }
$backend = if ($env:E2E_BACKEND) { $env:E2E_BACKEND } else { 'all' }
$requestedVersions = if ($env:E2E_POSTGRES_VERSION) { $env:E2E_POSTGRES_VERSION } else { '17.5.0 18.3.0' }
$e2ePackages = if ($env:E2E_PACKAGES) { $env:E2E_PACKAGES } else { './internal/daemon' }
$packageArguments = @($e2ePackages -split '\s+' | Where-Object { $_ })
$operatingSystem = (& go env GOOS).Trim()
$goArchitecture = (& go env GOARCH).Trim()

function Resolve-PostgresVersion {
    param([Parameter(Mandatory)][string]$Version)

    switch ($Version) {
        { $_ -in @('17', '17.5', '17.5.0') } { return '17.5.0' }
        { $_ -in @('18', '18.3', '18.3.0') } { return '18.3.0' }
        default { throw "Unsupported E2E PostgreSQL version: $Version (supported: 17, 18)" }
    }
}

function Invoke-E2ETests {
    & go test -tags=e2e @packageArguments -count=1 -timeout=10m
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

function Invoke-SQLiteE2E {
    Write-Host 'Running product-contract E2E tests with SQLite'
    $env:E2E_BACKEND = 'sqlite'
    Remove-Item Env:E2E_POSTGRES_VERSION -ErrorAction SilentlyContinue
    Remove-Item Env:E2E_POSTGRES_BINARIES -ErrorAction SilentlyContinue
    Invoke-E2ETests
}

function Invoke-PostgresE2E {
    param([Parameter(Mandatory)][string]$Version)

    $binaries = Join-Path $postgresRoot "$Version/$operatingSystem-$goArchitecture"
    $pgCtl = Join-Path $binaries 'bin/pg_ctl'
    if (-not (Test-Path -LiteralPath $pgCtl -PathType Leaf)) {
        throw "PostgreSQL $Version binaries are not prepared at $binaries. Run: make prepare-e2e E2E_POSTGRES_VERSION=$Version"
    }

    Write-Host "Running product-contract E2E tests with PostgreSQL $Version"
    $env:E2E_BACKEND = 'postgres'
    $env:E2E_POSTGRES_VERSION = $Version
    $env:E2E_POSTGRES_BINARIES = (Resolve-Path -LiteralPath $binaries).Path
    Invoke-E2ETests
}

switch ($backend) {
    'sqlite' { Invoke-SQLiteE2E }
    'postgres' {
        foreach ($requestedVersion in ($requestedVersions -split '[,\s]+' | Where-Object { $_ })) {
            Invoke-PostgresE2E (Resolve-PostgresVersion $requestedVersion)
        }
    }
    'all' {
        Invoke-SQLiteE2E
        foreach ($requestedVersion in ($requestedVersions -split '[,\s]+' | Where-Object { $_ })) {
            Invoke-PostgresE2E (Resolve-PostgresVersion $requestedVersion)
        }
    }
    default { throw "Unsupported E2E_BACKEND: $backend (supported: all, sqlite, postgres)" }
}
