$ErrorActionPreference = 'Stop'
$cacheRoot = Join-Path $env:RUNNER_TEMP 'go-cache'
$cacheDirectory = 'Z:\cache'
$cacheHash = $env:GO_CACHE_HASH
$archivePath = "$cacheDirectory\$cacheHash.zip"
Remove-Item -LiteralPath $cacheRoot -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $cacheRoot -Force | Out-Null
try {
  if (-not (Test-Path -LiteralPath $cacheDirectory -PathType Container)) {
    Write-Warning "Shared Go cache directory is unavailable; using an empty cache."
  }
  elseif (Test-Path -LiteralPath $archivePath -PathType Leaf) {
    [System.IO.Compression.ZipFile]::ExtractToDirectory($archivePath, $cacheRoot)
  }
}
catch {
  Write-Warning "Shared Go cache restore failed; using an empty cache: $($_.Exception.Message)"
  Remove-Item -LiteralPath $cacheRoot -Recurse -Force -ErrorAction SilentlyContinue
  New-Item -ItemType Directory -Path $cacheRoot -Force | Out-Null
}
"GOCACHE=$(Join-Path $cacheRoot 'go-build')" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
"GOMODCACHE=$(Join-Path $cacheRoot 'go-mod')" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
