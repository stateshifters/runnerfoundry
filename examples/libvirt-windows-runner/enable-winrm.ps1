$ErrorActionPreference = 'Stop'

winrm.exe quickconfig -q | Out-Null
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
Set-Item -Path WSMan:\localhost\Client\Auth\Basic -Value $true

netsh.exe advfirewall firewall add rule name="Packer WinRM HTTPS" dir=in protocol=TCP localport=5986 action=allow | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "opening the WinRM HTTPS port failed with exit code $LASTEXITCODE"
}

netsh.exe advfirewall firewall set rule group="windows remote management" new enable=yes | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "enabling the WinRM firewall group failed with exit code $LASTEXITCODE"
}

Restart-Service -Name WinRM -Force
