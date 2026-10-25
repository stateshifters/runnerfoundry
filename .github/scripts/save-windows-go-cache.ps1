$ErrorActionPreference = 'Stop'
$cacheDirectory = 'Z:\cache'
$cacheHash = $env:GO_CACHE_HASH
$archivePath = "$cacheDirectory\$cacheHash.zip"
$cacheRoot = Join-Path $env:RUNNER_TEMP 'go-cache'
$localArchive = $null
$sharedTemporary = $null
try {
  New-Item -ItemType Directory -Path $cacheDirectory -Force | Out-Null
  if (Test-Path -LiteralPath $archivePath -PathType Leaf) {
    Write-Host "Shared Go cache already exists for $cacheHash; skipping save."
    return
  }
  $identifier = [System.Guid]::NewGuid().ToString('N')
  $localArchive = Join-Path $env:RUNNER_TEMP "$cacheHash-$identifier.zip"
  $sharedTemporary = Join-Path $cacheDirectory ".$cacheHash-$identifier.tmp"
  [System.IO.Compression.ZipFile]::CreateFromDirectory($cacheRoot, $localArchive)
  Copy-Item -LiteralPath $localArchive -Destination $sharedTemporary
  try {
    [System.IO.File]::Move($sharedTemporary, $archivePath)
    Write-Host "Saved shared Go cache for $cacheHash."
  }
  catch [System.IO.IOException] {
    if (Test-Path -LiteralPath $archivePath -PathType Leaf) {
      Write-Host "Another runner saved the shared Go cache for $cacheHash."
    }
    else {
      throw
    }
  }
}
catch {
  Write-Warning "Shared Go cache save failed; the successful job result is unchanged: $($_.Exception.Message)"
}
finally {
  if ($null -ne $localArchive) {
    Remove-Item -LiteralPath $localArchive -Force -ErrorAction SilentlyContinue
  }
  if ($null -ne $sharedTemporary) {
    Remove-Item -LiteralPath $sharedTemporary -Force -ErrorAction SilentlyContinue
  }
}
