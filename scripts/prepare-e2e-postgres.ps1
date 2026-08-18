[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$postgresRoot = [System.IO.Path]::GetFullPath($(if ($env:E2E_POSTGRES_ROOT) { $env:E2E_POSTGRES_ROOT } else { Join-Path (Get-Location) 'build/e2e/postgres' }))
$requestedVersions = if ($env:E2E_POSTGRES_VERSION) { $env:E2E_POSTGRES_VERSION } else { '17.5.0 18.3.0' }
$repositoryUrl = 'https://repo1.maven.org/maven2'

function Resolve-PostgresVersion {
    param([Parameter(Mandatory)][string]$Version)

    switch ($Version) {
        { $_ -in @('17', '17.5', '17.5.0') } { return '17.5.0' }
        { $_ -in @('18', '18.3', '18.3.0') } { return '18.3.0' }
        default { throw "Unsupported E2E PostgreSQL version: $Version (supported: 17, 18)" }
    }
}

function Resolve-ArtifactArchitecture {
    param(
        [Parameter(Mandatory)][string]$OperatingSystem,
        [Parameter(Mandatory)][string]$Architecture,
        [Parameter(Mandatory)][string]$Version
    )

    switch ("$OperatingSystem/$Architecture") {
        'linux/amd64' { return 'amd64' }
        'linux/arm64' { return 'arm64v8' }
        'darwin/amd64' { return 'amd64' }
        'darwin/arm64' {
            if ($Version.StartsWith('17.')) { return 'amd64' }
            return 'arm64v8'
        }
        'windows/amd64' { return 'amd64' }
        default { throw "Unsupported E2E PostgreSQL platform: $OperatingSystem/$Architecture" }
    }
}

function Resolve-ArtifactChecksum {
    param(
        [Parameter(Mandatory)][string]$Version,
        [Parameter(Mandatory)][string]$OperatingSystem,
        [Parameter(Mandatory)][string]$ArtifactArchitecture
    )

    switch ("$Version|$OperatingSystem|$ArtifactArchitecture") {
        '17.5.0|linux|amd64' { return '646f3269075d1a23904127374e3cdbcef96beb32979fedee25d835877400fcbe' }
        '17.5.0|linux|arm64v8' { return 'e02f27ff50ff2d90b6541d54f3f108505aacd84ea53c2890e976a684bfea2752' }
        '17.5.0|linux|amd64-alpine' { return '7122386c7f7a089568f05720ada63448093d4065abbc0a89e6dc660851a53e3c' }
        '17.5.0|linux|arm64v8-alpine' { return 'f9cf1d22dd98bfdb63738846c330f2b9f18adc39c927b3a082d0ccbe51a0efa1' }
        '17.5.0|darwin|amd64' { return 'fb70f783928e77c3d6ef5ca8ed644f1ac502f740fcfc032a375c8347820f2bdd' }
        '17.5.0|windows|amd64' { return '06ced3b3267c569ffb3f85544aac0075926dd81bf78bd5247abb1aa9fe994d6b' }
        '18.3.0|linux|amd64' { return '1974c3a59c3ae010e9818e81637e425aad94eaac26e4ef533b42e081fe7ca345' }
        '18.3.0|linux|arm64v8' { return '1fb555a6602c13a113ab1f0cc9224d599948e85e51ac5f001393605505eaf78e' }
        '18.3.0|linux|amd64-alpine' { return '510fd314da39f1eeccef774a92428474168fedf55403342548e2adeaed66b483' }
        '18.3.0|linux|arm64v8-alpine' { return 'e8204cd252eced653bacf680f7f0f3db2d71fb87186ffeaa97dddb4c9b9697d6' }
        '18.3.0|darwin|amd64' { return 'cb68c57232945f5e74db946e6077d70cec56bfed35959b21acc750cc80ca0977' }
        '18.3.0|darwin|arm64v8' { return 'a18ec9ec6a905e04f5a08a8f7870bddadd635feb54dee332a3daf80fada3cb1a' }
        '18.3.0|windows|amd64' { return 'e1e2bb484f775c0b70fbde39c69451a7a4925629a85d622f557fe6f60de37ba1' }
        default { throw "No pinned checksum for PostgreSQL artifact $Version/$OperatingSystem/$ArtifactArchitecture" }
    }
}

function Invoke-BoundedDownload {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$Destination
    )

    & curl.exe --fail --location --silent --show-error --connect-timeout 15 --max-time 300 --output $Destination $Uri
    if ($LASTEXITCODE -ne 0) { throw "Unable to download $Uri" }
}

function Assert-ChildPath {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Root
    )

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($Root).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    ) + [System.IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to modify path outside $Root`: $Path"
    }
    return $fullPath
}

function Remove-ValidatedDirectory {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Root
    )

    if (-not (Test-Path -LiteralPath $Path)) { return }
    $fullPath = Assert-ChildPath -Path $Path -Root $Root
    Remove-Item -LiteralPath $fullPath -Recurse -Force
}

function Test-PostgresInstallation {
    param(
        [Parameter(Mandatory)][string]$OperatingSystem,
        [Parameter(Mandatory)][string]$Destination
    )

    $suffix = if ($OperatingSystem -eq 'windows') { '.exe' } else { '' }
    $requiredPaths = @(
        "bin/pg_ctl$suffix",
        "bin/initdb$suffix",
        "bin/postgres$suffix",
        'share/postgres.bki',
        'share/postgresql.conf.sample'
    )
    if ($OperatingSystem -eq 'windows') { $requiredPaths += 'bin/pg_ctl' }
    foreach ($requiredPath in $requiredPaths) {
        if (-not (Test-Path -LiteralPath (Join-Path $Destination $requiredPath) -PathType Leaf)) { return $false }
    }
    return $true
}

function Expand-PostgresArchive {
    param(
        [Parameter(Mandatory)][string]$JarPath,
        [Parameter(Mandatory)][string]$ArchivePath,
        [Parameter(Mandatory)][string]$Destination
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($JarPath)
    try {
        $entry = $zip.Entries | Where-Object { $_.FullName.EndsWith('.txz') } | Select-Object -First 1
        if (-not $entry) { throw "PostgreSQL binary jar does not contain a .txz archive" }
        [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $ArchivePath, $true)
    }
    finally {
        $zip.Dispose()
    }

    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    & tar -xJf $ArchivePath -C $Destination
    if ($LASTEXITCODE -ne 0) { throw "Unable to extract PostgreSQL binary archive" }
}

$operatingSystem = (& go env GOOS).Trim()
$goArchitecture = (& go env GOARCH).Trim()
foreach ($requestedVersion in ($requestedVersions -split '[,\s]+' | Where-Object { $_ })) {
    $version = Resolve-PostgresVersion $requestedVersion
    $artifactArchitecture = Resolve-ArtifactArchitecture $operatingSystem $goArchitecture $version
    $destination = Join-Path $postgresRoot "$version/$operatingSystem-$goArchitecture"
    $executable = if ($operatingSystem -eq 'windows') { 'pg_ctl.exe' } else { 'pg_ctl' }
    $pgCtl = Join-Path $destination "bin/$executable"
    $extensionlessPgCtl = Join-Path $destination 'bin/pg_ctl'
    if ($operatingSystem -eq 'windows' -and (Test-Path -LiteralPath $pgCtl -PathType Leaf) -and
        -not (Test-Path -LiteralPath $extensionlessPgCtl -PathType Leaf)) {
        Copy-Item -LiteralPath $pgCtl -Destination $extensionlessPgCtl
    }
    if (Test-PostgresInstallation -OperatingSystem $operatingSystem -Destination $destination) {
        Write-Host "PostgreSQL $version binaries already prepared at $destination"
        continue
    }

    $artifact = "embedded-postgres-binaries-$operatingSystem-$artifactArchitecture-$version"
    $downloadUrl = "$repositoryUrl/io/zonky/test/postgres/embedded-postgres-binaries-$operatingSystem-$artifactArchitecture/$version/$artifact.jar"
    $temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("runnerfoundry-e2e-" + [guid]::NewGuid())
    $destinationParent = Split-Path -Parent $destination
    New-Item -ItemType Directory -Force -Path $destinationParent | Out-Null
    $stagingDirectory = Join-Path $destinationParent ("$(Split-Path -Leaf $destination).prepare." + [guid]::NewGuid())
    $replacedDirectory = $null
    New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
    New-Item -ItemType Directory -Path $stagingDirectory | Out-Null
    try {
        $jarPath = Join-Path $temporaryDirectory 'postgres.jar'
        $archivePath = Join-Path $temporaryDirectory 'postgres.txz'
        Write-Host "Preparing PostgreSQL $version binaries for $operatingSystem/$goArchitecture"
        Invoke-BoundedDownload -Uri $downloadUrl -Destination $jarPath

        $expectedChecksum = Resolve-ArtifactChecksum -Version $version -OperatingSystem $operatingSystem -ArtifactArchitecture $artifactArchitecture
        $actualChecksum = (Get-FileHash -Algorithm SHA256 -LiteralPath $jarPath).Hash.ToLowerInvariant()
        if ($actualChecksum -ne $expectedChecksum) { throw "Checksum mismatch for PostgreSQL $version binary archive" }

        Expand-PostgresArchive -JarPath $jarPath -ArchivePath $archivePath -Destination $stagingDirectory
        $stagedPgCtl = Join-Path $stagingDirectory "bin/$executable"
        $stagedExtensionlessPgCtl = Join-Path $stagingDirectory 'bin/pg_ctl'
        if ($operatingSystem -eq 'windows' -and -not (Test-Path -LiteralPath $stagedExtensionlessPgCtl -PathType Leaf)) {
            Copy-Item -LiteralPath $stagedPgCtl -Destination $stagedExtensionlessPgCtl
        }
        if (-not (Test-PostgresInstallation -OperatingSystem $operatingSystem -Destination $stagingDirectory)) {
            throw "Prepared PostgreSQL $version archive is incomplete"
        }

        if (Test-Path -LiteralPath $destination) {
            $replacedDirectory = "$destination.invalid.$([guid]::NewGuid())"
            Assert-ChildPath -Path $destination -Root $postgresRoot | Out-Null
            Assert-ChildPath -Path $replacedDirectory -Root $postgresRoot | Out-Null
            Move-Item -LiteralPath $destination -Destination $replacedDirectory
        }
        Assert-ChildPath -Path $stagingDirectory -Root $postgresRoot | Out-Null
        Assert-ChildPath -Path $destination -Root $postgresRoot | Out-Null
        Move-Item -LiteralPath $stagingDirectory -Destination $destination
        $stagingDirectory = $null
        if ($replacedDirectory) {
            Remove-ValidatedDirectory -Path $replacedDirectory -Root $postgresRoot
            $replacedDirectory = $null
        }
        Write-Host "Prepared PostgreSQL $version binaries at $destination"
    }
    catch {
        if ($replacedDirectory -and -not (Test-Path -LiteralPath $destination)) {
            Move-Item -LiteralPath $replacedDirectory -Destination $destination
            $replacedDirectory = $null
        }
        throw
    }
    finally {
        Remove-ValidatedDirectory -Path $temporaryDirectory -Root ([System.IO.Path]::GetTempPath())
        if ($stagingDirectory) { Remove-ValidatedDirectory -Path $stagingDirectory -Root $postgresRoot }
    }
}
