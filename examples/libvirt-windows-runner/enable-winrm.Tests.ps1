$scriptPath = Join-Path $PSScriptRoot 'enable-winrm.ps1'

Describe 'Windows Packer WinRM bootstrap' {
    BeforeEach {
        Mock Set-Service {}
        Mock Start-Service {}
        Mock Get-WSManInstance {
            if ($Enumerate) {
                return @()
            }

            return [pscustomobject]@{
                Transport = 'HTTPS'
                Port = 5986
            }
        }
        Mock Remove-WSManInstance {}
        Mock New-SelfSignedCertificate {
            return [pscustomobject]@{ Thumbprint = 'test-thumbprint' }
        }
        Mock New-WSManInstance {}
        Mock Set-WSManInstance {}
        Mock New-NetFirewallRule {}
        Mock Restart-Service {}
    }

    It 'creates and verifies the HTTPS listener before Packer connects' {
        & $scriptPath

        Assert-MockCalled Set-Service -Times 1 -Exactly -ParameterFilter {
            $Name -eq 'WinRM' -and $StartupType -eq 'Automatic'
        }
        Assert-MockCalled Start-Service -Times 1 -Exactly -ParameterFilter {
            $Name -eq 'WinRM'
        }
        Assert-MockCalled New-WSManInstance -Times 1 -Exactly -ParameterFilter {
            $ResourceURI -eq 'winrm/config/listener' -and
            $SelectorSet.Transport -eq 'HTTPS' -and
            $SelectorSet.Address -eq '*' -and
            $ValueSet.CertificateThumbprint -eq 'test-thumbprint'
        }
        Assert-MockCalled Set-WSManInstance -Times 1 -Exactly -ParameterFilter {
            $ResourceURI -eq 'winrm/config/service/auth' -and $ValueSet.Basic -eq 'true'
        }
        Assert-MockCalled New-NetFirewallRule -Times 1 -Exactly -ParameterFilter {
            $LocalPort -eq 5986 -and $Action -eq 'Allow'
        }
        Assert-MockCalled Restart-Service -Times 1 -Exactly -ParameterFilter {
            $Name -eq 'WinRM' -and $Force
        }
        Assert-MockCalled Get-WSManInstance -Times 1 -Exactly -ParameterFilter {
            $ResourceURI -eq 'winrm/config/listener' -and -not $Enumerate
        }
    }

    It 'fails when the HTTPS listener is not exposed on port 5986' {
        Mock Get-WSManInstance {
            if ($Enumerate) {
                return @()
            }

            return [pscustomobject]@{
                Transport = 'HTTPS'
                Port = 5985
            }
        }

        { & $scriptPath } | Should Throw 'The Packer WinRM HTTPS listener is not available on port 5986.'
    }
}
