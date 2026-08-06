$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

. (Join-Path $PSScriptRoot 'runnerfoundry-jit-launcher.ps1')

function Assert-RunnerFoundryEqual {
    param($Expected, $Actual, [string]$Message)

    if ($Expected -ne $Actual) {
        throw "$Message Expected '$Expected', received '$Actual'."
    }
}

$process = [pscustomobject]@{ Id = 42; ExitCode = 0 }
$process | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value { param($Milliseconds) return $true }

$previousJit = 'parent-jit-value'
$env:ACTIONS_RUNNER_INPUT_JITCONFIG = $previousJit
$env:RUNNERFOUNDRY_TEST_MARKER = 'inherited-value'
$capturedJit = $null
$capturedMarker = $null
$capturedExtraArguments = $null
$startProcess = {
    param($Directory)
    $script:capturedJit = $env:ACTIONS_RUNNER_INPUT_JITCONFIG
    $script:capturedMarker = $env:RUNNERFOUNDRY_TEST_MARKER
    $script:capturedExtraArguments = $args.Count
    return $process
}

Invoke-RunnerFoundryProcess -RuntimeDirectory 'C:\runtime' -JitConfiguration 'test-jit-secret' `
    -Timeout ([TimeSpan]::FromMinutes(1)) -StartProcess $startProcess

Assert-RunnerFoundryEqual 'test-jit-secret' $capturedJit 'The runner did not inherit the JIT environment variable.'
Assert-RunnerFoundryEqual 'inherited-value' $capturedMarker 'The runner did not inherit the parent environment.'
Assert-RunnerFoundryEqual 0 $capturedExtraArguments 'The launcher passed the JIT configuration as a process argument.'
Assert-RunnerFoundryEqual $previousJit $env:ACTIONS_RUNNER_INPUT_JITCONFIG 'The launcher did not restore the parent JIT environment variable.'

$startFailure = $null
try {
    Invoke-RunnerFoundryProcess -RuntimeDirectory 'C:\runtime' -JitConfiguration 'other-test-jit-secret' `
        -Timeout ([TimeSpan]::FromMinutes(1)) -StartProcess { param($Directory) throw 'start failed' }
}
catch {
    $startFailure = $_.Exception.Message
}
Assert-RunnerFoundryEqual 'start failed' $startFailure 'The launcher did not preserve the process-start failure.'
Assert-RunnerFoundryEqual $previousJit $env:ACTIONS_RUNNER_INPUT_JITCONFIG 'The launcher did not restore the parent environment after process-start failure.'

$events = [System.Collections.Generic.List[string]]::new()
$readOemStrings = { return @('runnerfoundry.jit=test-jit-secret') }
$templateAvailable = { param($Directory) return $true }
$prepareRuntime = { param($Template, $Runtime) $events.Add('prepare') }
$runRunner = { param($Directory, $Jit, $Timeout) $events.Add('run'); throw 'runner failed' }
$cleanupRuntime = { param($Directory) $events.Add('cleanup') }
$powerOff = { $events.Add('poweroff') }

$failure = $null
try {
    Invoke-RunnerFoundryJitLauncher -ReadOemStrings $readOemStrings -TemplateAvailable $templateAvailable `
        -PrepareRuntime $prepareRuntime -RunRunner $runRunner -CleanupRuntime $cleanupRuntime -PowerOff $powerOff
}
catch {
    $failure = $_.Exception.Message
}

Assert-RunnerFoundryEqual 'runner failed' $failure 'The launcher did not preserve the runner failure.'
Assert-RunnerFoundryEqual 'prepare,run,cleanup,poweroff' ($events -join ',') 'The launcher did not clean up and power off after failure.'

Write-Output 'Windows JIT launcher tests passed.'
