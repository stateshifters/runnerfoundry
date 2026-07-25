$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

Start-Sleep -Seconds 5

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

& schtasks.exe /Delete /TN RunnerFoundrySeal /F | Out-Null
Remove-Item -LiteralPath $MyInvocation.MyCommand.Path -Force
& shutdown.exe /s /t 10 /f
