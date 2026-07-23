$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$unattendedFiles = @(
    'C:\Autounattend.xml',
    'C:\Windows\Panther\Autounattend.xml',
    'C:\Windows\Panther\Unattend.xml',
    'C:\Windows\Panther\Unattend\Unattend.xml',
    'C:\Windows\System32\Sysprep\unattend.xml'
)

foreach ($path in $unattendedFiles) { Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue }
Remove-Item -LiteralPath 'C:\Windows\Temp\runnerfoundry-jit-launcher.ps1' -Force -ErrorAction SilentlyContinue
Remove-Item -Path 'C:\Windows\Temp\packer-*', 'C:\Windows\Temp\script-*.ps1' -Recurse -Force -ErrorAction SilentlyContinue

Get-ChildItem -Path WSMan:\localhost\Listener -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
Get-NetFirewallRule -DisplayName 'Packer WinRM HTTPS' -ErrorAction SilentlyContinue | Remove-NetFirewallRule
Set-Service -Name WinRM -StartupType Disabled

& net.exe user packer /delete | Out-Null
Disable-LocalUser -Name Administrator

if (Get-LocalUser -Name packer -ErrorAction SilentlyContinue) { throw 'The Packer account still exists.' }
if ((Get-Service -Name WinRM).StartType -ne 'Disabled') { throw 'WinRM is still enabled.' }
foreach ($path in $unattendedFiles) {
    if (Test-Path -LiteralPath $path) { throw 'Cached unattended setup data remains.' }
}

$task = Get-ScheduledTask -TaskName 'RunnerFoundry JIT runner'
if ($task.Principal.UserId -notin @('SYSTEM', 'NT AUTHORITY\SYSTEM') -or $task.State -eq 'Disabled') {
    throw 'The RunnerFoundry startup task is not enabled under LocalSystem.'
}
$templateDirectory = 'C:\RunnerFoundry\template'
if (-not (Test-Path -LiteralPath (Join-Path $templateDirectory 'run.cmd'))) { throw 'The runner template is missing.' }
foreach ($name in @('.runner', '.credentials', '.credentials_rsaparams')) {
    if (Test-Path -LiteralPath (Join-Path $templateDirectory $name)) { throw 'The runner template is registered.' }
}

Remove-Item -LiteralPath $MyInvocation.MyCommand.Path -Force
& shutdown.exe /s /t 0 /f
