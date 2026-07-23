$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$installRoot = 'C:\RunnerFoundry'
$templateDirectory = Join-Path $installRoot 'template'
$launcherPath = Join-Path $installRoot 'runnerfoundry-jit-launcher.ps1'
$archivePath = Join-Path $env:TEMP 'actions-runner.zip'
$runnerUrl = "https://github.com/actions/runner/releases/download/v$env:GITHUB_RUNNER_VERSION/actions-runner-win-x64-$env:GITHUB_RUNNER_VERSION.zip"

New-Item -ItemType Directory -Path $installRoot -Force | Out-Null
Copy-Item -LiteralPath 'C:\Windows\Temp\runnerfoundry-jit-launcher.ps1' -Destination $launcherPath -Force

try {
    Invoke-WebRequest -UseBasicParsing -Uri $runnerUrl -OutFile $archivePath
    $actualHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actualHash -ne $env:GITHUB_RUNNER_SHA256) { throw 'The GitHub Actions runner checksum did not match.' }

    Remove-Item -LiteralPath $templateDirectory -Recurse -Force -ErrorAction SilentlyContinue
    Expand-Archive -LiteralPath $archivePath -DestinationPath $templateDirectory
}
finally {
    Remove-Item -LiteralPath $archivePath -Force -ErrorAction SilentlyContinue
}

. $launcherPath
if ((Get-RunnerFoundryOemStrings) -notcontains 'runnerfoundry.packer=raw-smbios-reader-ok') {
    throw 'The raw SMBIOS reader did not find the Packer validation value.'
}

$registrationFiles = @('.runner', '.credentials', '.credentials_rsaparams')
if (-not (Test-Path -LiteralPath (Join-Path $templateDirectory 'run.cmd'))) { throw 'The runner template is incomplete.' }
foreach ($name in $registrationFiles) {
    if (Test-Path -LiteralPath (Join-Path $templateDirectory $name)) { throw 'The runner template is registered.' }
}

$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$launcherPath`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName 'RunnerFoundry JIT runner' -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null

$task = Get-ScheduledTask -TaskName 'RunnerFoundry JIT runner'
if ($task.Principal.UserId -notin @('SYSTEM', 'NT AUTHORITY\SYSTEM')) { throw 'The startup task does not run as LocalSystem.' }
if ($task.State -eq 'Disabled') { throw 'The startup task is disabled.' }
