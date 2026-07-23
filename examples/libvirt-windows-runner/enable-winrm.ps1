$ErrorActionPreference = 'Stop'

Set-Service -Name WinRM -StartupType Automatic
Start-Service -Name WinRM

Get-WSManInstance -ResourceURI 'winrm/config/listener' -Enumerate | ForEach-Object {
    Remove-WSManInstance -ResourceURI 'winrm/config/listener' -SelectorSet @{
        Transport = $_.Transport
        Address = $_.Address
    }
}

$certificate = New-SelfSignedCertificate -DnsName $env:COMPUTERNAME -CertStoreLocation Cert:\LocalMachine\My
New-WSManInstance -ResourceURI 'winrm/config/listener' -SelectorSet @{
    Transport = 'HTTPS'
    Address = '*'
} -ValueSet @{
    Hostname = $env:COMPUTERNAME
    CertificateThumbprint = $certificate.Thumbprint
} | Out-Null
Set-WSManInstance -ResourceURI 'winrm/config/service/auth' -ValueSet @{ Basic = 'true' } | Out-Null
Set-WSManInstance -ResourceURI 'winrm/config/service' -ValueSet @{ AllowUnencrypted = 'false' } | Out-Null
New-NetFirewallRule -DisplayName 'Packer WinRM HTTPS' -Direction Inbound -Protocol TCP -LocalPort 5986 -Action Allow | Out-Null
Restart-Service -Name WinRM -Force

$listener = Get-WSManInstance -ResourceURI 'winrm/config/listener' -SelectorSet @{
    Transport = 'HTTPS'
    Address = '*'
}
if ($listener.Transport -ne 'HTTPS' -or [int]$listener.Port -ne 5986) {
    throw 'The Packer WinRM HTTPS listener is not available on port 5986.'
}
