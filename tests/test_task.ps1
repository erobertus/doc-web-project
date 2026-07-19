# Unit tests for agent_task.ps1's decision logic.
#
# Runs UNELEVATED and touches nothing: it dot-sources the script
# with -LoadOnly and drives Repair-Settings / Test-HasRepeat with
# real in-memory objects from New-ScheduledTaskSettingsSet and
# New-ScheduledTaskTrigger - the same types Get-ScheduledTask
# returns, so the comparisons are the real ones.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File tests\test_task.ps1
#
# NOT covered here: Get/Set/Register-ScheduledTask against the live
# task store, which needs admin. Verify -Repair on ONE machine
# before pushing it to the fleet.

$ErrorActionPreference = 'Stop'
$script:fail = 0
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path (Split-Path -Parent $here) 'agent_task.ps1') -LoadOnly

function Check($cond, $what) {
    if ($cond) { "  PASS  $what" }
    else { $script:fail++; "  FAIL  $what" }
}

'=== Task Scheduler defaults are what break a long-running agent ==='
$d = New-ScheduledTaskSettingsSet
"  defaults: limit=$($d.ExecutionTimeLimit) noBattStart=$($d.DisallowStartIfOnBatteries) stopBatt=$($d.StopIfGoingOnBatteries) avail=$($d.StartWhenAvailable)"
Check ($d.ExecutionTimeLimit -ne 'PT0S') 'default execution time limit is finite (this is the bug)'

'=== Repair-Settings on default (broken) settings ==='
$s = New-ScheduledTaskSettingsSet
$changes = @(Repair-Settings -s $s)
"  changes: $($changes -join '; ')"
Check ($changes.Count -ge 1)                  'reports at least one change'
Check ($s.ExecutionTimeLimit -eq 'PT0S')      'ExecutionTimeLimit -> PT0S (no limit)'
Check ($s.MultipleInstances -eq 'IgnoreNew')  'MultipleInstances is IgnoreNew'
Check (-not $s.DisallowStartIfOnBatteries)    'DisallowStartIfOnBatteries cleared'
Check (-not $s.StopIfGoingOnBatteries)        'StopIfGoingOnBatteries cleared'
Check ($s.StartWhenAvailable)                 'StartWhenAvailable set'
Check (($changes -join ' ') -match 'ExecutionTimeLimit') 'names the time limit in its report'

'=== idempotent: a second pass changes nothing ==='
$again = @(Repair-Settings -s $s)
"  changes: $($again.Count)"
Check ($again.Count -eq 0) 'no changes reported on an already-correct task'

'=== an already-correct settings object is left alone ==='
$good = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -DontStopOnIdleEnd `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
Check (@(Repair-Settings -s $good).Count -eq 0) 'wanted settings need no repair'
Check ($good.ExecutionTimeLimit -eq 'PT0S')     'and are still PT0S afterwards'

'=== any non-IgnoreNew policy is corrected ==='
# a repeat launch under Parallel would start a SECOND agent on the
# machine; under StopExisting (settable via XML) it would kill the
# running one. Only IgnoreNew is safe with a repeating trigger.
$par = New-ScheduledTaskSettingsSet -MultipleInstances Parallel `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -StartWhenAvailable
$c = @(Repair-Settings -s $par)
Check ($par.MultipleInstances -eq 'IgnoreNew')    'Parallel -> IgnoreNew'
Check (($c -join ' ') -match 'MultipleInstances') 'and says so'
Check ($c.Count -eq 1)                            'nothing else touched'

'=== Test-HasRepeat ==='
$boot = New-ScheduledTaskTrigger -AtStartup
Check (-not (Test-HasRepeat $boot))       'a bare boot trigger does not repeat'
Check (-not (Test-HasRepeat @($boot)))    'nor as a single-element array'
Check (-not (Test-HasRepeat $null))       'null trigger list is handled'
Check (-not (Test-HasRepeat @()))         'empty trigger list is handled'

$rep = New-ScheduledTaskTrigger -Once -At (Get-Date).Date
$rep.Repetition = New-CimInstance -ClassName MSFT_TaskRepetitionPattern `
    -Namespace Root/Microsoft/Windows/TaskScheduler -ClientOnly `
    -Property @{ Interval = 'PT10M'; StopAtDurationEnd = $false }
Check (Test-HasRepeat $rep)               'a repeating trigger is detected'
Check (Test-HasRepeat @($boot, $rep))     'detected when mixed with a boot trigger'
Check ($rep.Repetition.Interval -eq 'PT10M') 'repetition interval is PT10M'

'=== Set-ScheduledTask: -Settings and -Trigger must stay apart ==='
# Passing both to ONE call throws "Type mismatch" against a live
# SYSTEM task on Windows 11, while the same two changes applied one
# at a time both succeed. Caught in production on 2026-07-19.
$src = Get-Content (Join-Path (Split-Path -Parent $here) 'agent_task.ps1') -Raw
# drop comment lines first (the comment explaining this very rule
# names all three tokens on one line), then join backtick
# continuations so a call split across lines is still seen whole
$code = ($src -split '\r?\n' |
         Where-Object { $_.TrimStart() -notmatch '^#' }) -join "`n"
$flat = $code -replace '`\r?\n\s*', ' '
$combined = @($flat -split '\r?\n' | Where-Object {
    $_ -match 'Set-ScheduledTask' -and
    $_ -match '-Settings' -and $_ -match '-Trigger' })
Check ($combined.Count -eq 0) 'no Set-ScheduledTask passes -Settings and -Trigger together'
Check ($flat -match 'Set-ScheduledTask[^\r\n]*-Settings') 'settings are applied'
Check ($flat -match 'Set-ScheduledTask[^\r\n]*-Trigger')  'triggers are applied'

'=== triggers: no appending to the existing typed array ==='
# $task.Triggers is MSFT_TaskBootTrigger[] on a boot-only task and
# @() does not retype it, so appending a Time trigger throws
# "Type mismatch". Seen in production 2026-07-19.
Check ($flat -notmatch '\@\(\$task\.Triggers\)\s*\+') 'does not append to $task.Triggers'
Check ($flat -match '\$existing\[0\]\.Repetition') 'attaches repetition to the existing trigger'

'=== the repair verifies by re-reading, not by absence of an error ==='
Check ($src -match "verified ExecutionTimeLimit") 'success line reports a verified state'
Check ($src -match "did not stick")               'mismatch after writing is reported as WARN'

''
if ($script:fail) { "RESULT: $script:fail CHECK(S) FAILED"; exit 1 }
'RESULT: all checks passed'
exit 0
