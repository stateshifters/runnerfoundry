$ErrorActionPreference = 'Stop'

winrm.exe quickconfig -quiet | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "winrm quickconfig failed with exit code $LASTEXITCODE"
}

Set-Service -Name WinRM -StartupType Automatic
Start-Service -Name WinRM

Get-ChildItem WSMan:\LocalHost\Listener -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force

$certificate = New-SelfSignedCertificate -DnsName $env:COMPUTERNAME -CertStoreLocation Cert:\LocalMachine\My
New-Item `
    -Path WSMan:\LocalHost\Listener `
    -Transport HTTPS `
    -Address * `
    -CertificateThumbprint $certificate.Thumbprint `
    -Force | Out-Null
Set-Item -Path WSMan:\localhost\Service\Auth\Basic -Value $true
New-NetFirewallRule -DisplayName 'Packer WinRM HTTPS' -Direction Inbound -Protocol TCP -LocalPort 5986 -Action Allow | Out-Null
Restart-Service -Name WinRM -Force
