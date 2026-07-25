$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

if (-not ('RunnerFoundryFirmware' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class RunnerFoundryFirmware
{
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint GetSystemFirmwareTable(
        uint providerSignature,
        uint tableId,
        byte[] buffer,
        uint bufferSize);

    public static byte[] ReadRawSmbios()
    {
        const uint rsmb = 0x52534D42;
        uint size = GetSystemFirmwareTable(rsmb, 0, null, 0);
        if (size == 0) throw new Win32Exception(Marshal.GetLastWin32Error());

        byte[] buffer = new byte[size];
        uint written = GetSystemFirmwareTable(rsmb, 0, buffer, size);
        if (written != size) throw new Win32Exception(Marshal.GetLastWin32Error());
        return buffer;
    }
}
'@
}

function ConvertFrom-RunnerFoundryRawSmbios {
    [CmdletBinding()]
    param([Parameter(Mandatory)][byte[]]$RawData)

    if ($RawData.Length -lt 8) { return @() }
    $declaredLength = [BitConverter]::ToUInt32($RawData, 4)
    if ($declaredLength -gt ($RawData.Length - 8)) { return @() }

    $values = [System.Collections.Generic.List[string]]::new()
    $offset = 8
    $end = 8 + [int]$declaredLength
    while ($offset -lt $end) {
        if (($end - $offset) -lt 4) { return @() }
        $type = $RawData[$offset]
        $length = [int]$RawData[$offset + 1]
        if ($length -lt 4 -or ($offset + $length) -gt $end) { return @() }

        $stringOffset = $offset + $length
        $strings = [System.Collections.Generic.List[string]]::new()
        if ($stringOffset -ge $end) { return @() }
        if ($stringOffset -lt ($end - 1) -and $RawData[$stringOffset] -eq 0 -and $RawData[$stringOffset + 1] -eq 0) {
            $stringOffset += 2
        }
        else {
            while ($stringOffset -lt $end) {
                $terminator = [Array]::IndexOf($RawData, [byte]0, $stringOffset, $end - $stringOffset)
                if ($terminator -lt 0) { return @() }
                $strings.Add([Text.Encoding]::ASCII.GetString($RawData, $stringOffset, $terminator - $stringOffset))
                $stringOffset = $terminator + 1
                if ($stringOffset -ge $end) { return @() }
                if ($RawData[$stringOffset] -eq 0) {
                    $stringOffset++
                    break
                }
            }
        }

        if ($type -eq 11) {
            if ($length -lt 5 -or $RawData[$offset + 4] -gt $strings.Count) { return @() }
            for ($index = 0; $index -lt $RawData[$offset + 4]; $index++) { $values.Add($strings[$index]) }
        }
        if ($type -eq 127) { break }
        $offset = $stringOffset
    }
    return $values.ToArray()
}

function Get-RunnerFoundryOemStrings {
    ConvertFrom-RunnerFoundryRawSmbios -RawData ([RunnerFoundryFirmware]::ReadRawSmbios())
}

function Find-RunnerFoundryJit {
    [CmdletBinding()]
    param([Parameter(Mandatory)][AllowNull()][AllowEmptyCollection()][string[]]$OemStrings)

    foreach ($value in $OemStrings) {
        if ($value.StartsWith('runnerfoundry.jit=', [StringComparison]::Ordinal)) {
            $jit = $value.Substring('runnerfoundry.jit='.Length)
            if ($jit.Length -gt 0) { return $jit }
        }
    }
    return $null
}

function Invoke-RunnerFoundryProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$RuntimeDirectory,
        [Parameter(Mandatory)][string]$JitConfiguration,
        [Parameter(Mandatory)][TimeSpan]$Timeout,
        [scriptblock]$StartProcess = {
            param($Directory, $Jit)
            Start-Process -FilePath (Join-Path $Directory 'run.cmd') -ArgumentList @('--jitconfig', $Jit) `
                -WorkingDirectory $Directory -WindowStyle Hidden -PassThru
        },
        [scriptblock]$KillProcessTree = {
            param($ProcessId)
            & taskkill.exe /PID $ProcessId /T /F | Out-Null
            if ($LASTEXITCODE -ne 0) { throw 'Runner process-tree termination failed.' }
        }
    )

    $process = & $StartProcess $RuntimeDirectory $JitConfiguration
    if (-not $process.WaitForExit([int]$Timeout.TotalMilliseconds)) {
        & $KillProcessTree $process.Id
        if (-not $process.WaitForExit(30000)) { throw 'The runner process tree did not exit.' }
        throw 'The runner job exceeded its time limit.'
    }
    if ($process.ExitCode -ne 0) { throw 'The runner job failed.' }
}

function Invoke-RunnerFoundryPowerOff {
    [CmdletBinding()]
    param(
        [scriptblock]$StopComputer = { Stop-Computer -Force },
        [scriptblock]$Shutdown = { & shutdown.exe /s /t 0 /f }
    )

    try { & $StopComputer }
    catch { & $Shutdown }
}

function Remove-RunnerFoundryRuntime {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$RuntimeDirectory)

    for ($attempt = 0; $attempt -lt 10; $attempt++) {
        Remove-Item -LiteralPath $RuntimeDirectory -Recurse -Force -ErrorAction SilentlyContinue
        if (-not (Test-Path -LiteralPath $RuntimeDirectory)) { return }
        Start-Sleep -Seconds 1
    }
    throw 'Runner runtime cleanup failed.'
}

function Invoke-RunnerFoundryJitLauncher {
    [CmdletBinding()]
    param(
        [string]$TemplateDirectory = 'C:\RunnerFoundry\template',
        [string]$RuntimeDirectory = 'C:\RunnerFoundry\runtime',
        [TimeSpan]$JobTimeout = ([TimeSpan]::FromHours(6)),
        [scriptblock]$ReadOemStrings = { Get-RunnerFoundryOemStrings },
        [scriptblock]$RunRunner = {
            param($Directory, $Jit, $Timeout)
            Invoke-RunnerFoundryProcess -RuntimeDirectory $Directory -JitConfiguration $Jit -Timeout $Timeout
        },
        [scriptblock]$PowerOff = { Invoke-RunnerFoundryPowerOff }
    )

    try {
        $jit = Find-RunnerFoundryJit -OemStrings @(& $ReadOemStrings)
        if ([string]::IsNullOrEmpty($jit)) { throw 'Runner JIT configuration is unavailable.' }
        if (-not (Test-Path -LiteralPath (Join-Path $TemplateDirectory 'run.cmd'))) {
            throw 'The clean runner template is unavailable.'
        }

        Remove-RunnerFoundryRuntime -RuntimeDirectory $RuntimeDirectory
        Copy-Item -LiteralPath $TemplateDirectory -Destination $RuntimeDirectory -Recurse -Force
        & $RunRunner $RuntimeDirectory $jit $JobTimeout
    }
    finally {
        try { Remove-RunnerFoundryRuntime -RuntimeDirectory $RuntimeDirectory }
        finally { & $PowerOff }
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    Invoke-RunnerFoundryJitLauncher
}
