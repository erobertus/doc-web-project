# Scheduled-task definition for the CPSO scrape agent.
#
# The agent is a long-running poller, but `schtasks /create` applies
# Task Scheduler's DEFAULTS - and the default ExecutionTimeLimit is
# 3 days. When it expires Task Scheduler gracefully ENDS the task,
# which delivers a console CTRL+C to the whole process tree; the
# batch wrapper dies with it, leaving a bare "^C" at the end of
# agent.log. With only an at-boot trigger, nothing ever restarts it
# and the machine sits there idle until someone reboots it. That is
# what silently killed most of the fleet in July 2026.
#
#   -Register -Command <run_agent.bat>   create/replace the task
#   -Repair                              fix an existing task
#
# -Repair is what a running agent calls on startup. It only ever
# uses Set-ScheduledTask, which updates the definition WITHOUT
# stopping the running instance - never Register -Force, which
# would kill the very agent doing the repair.
#
# Prints one line per change, or nothing when already correct.
# Exit 0 = fine (including "nothing to do"), 1 = could not do it.

[CmdletBinding(DefaultParameterSetName = 'Repair')]
param(
    [Parameter(ParameterSetName = 'Register', Mandatory = $true)]
    [switch]$Register,

    [Parameter(ParameterSetName = 'Register', Mandatory = $true)]
    [string]$Command,

    [Parameter(ParameterSetName = 'Repair')]
    [switch]$Repair,

    [string]$TaskName = 'CPSO scrape agent',

    # how often the task is re-launched as a liveness net; with
    # IgnoreNew this is a no-op while the agent is alive
    [int]$RepeatMinutes = 10,

    # define the functions and return without touching anything,
    # so tests/test_task.ps1 can exercise the decision logic
    [switch]$LoadOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$REPEAT = "PT${RepeatMinutes}M"
$NS = 'Root/Microsoft/Windows/TaskScheduler'


function New-Repetition {
    # Built as a CIM instance: New-ScheduledTaskTrigger's
    # -RepetitionDuration is inconsistent across Windows builds,
    # this shape is not.
    New-CimInstance -ClassName MSFT_TaskRepetitionPattern `
        -Namespace $NS -ClientOnly -Property @{
            Interval          = $REPEAT
            StopAtDurationEnd = $false
        }
}


function Get-WantedSettings {
    # PT0S = run indefinitely. IgnoreNew = a repeat launch while the
    # agent is already running is discarded rather than starting a
    # second copy or killing the first.
    New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -DontStopOnIdleEnd `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -MultipleInstances IgnoreNew `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -StartWhenAvailable
}


function Invoke-Register {
    if (-not (Test-Path -LiteralPath $Command)) {
        Write-Error "command not found: $Command"
    }
    $work = Split-Path -Parent $Command

    $action = New-ScheduledTaskAction -Execute $Command `
        -WorkingDirectory $work

    # at boot, plus a repeating liveness trigger so the agent comes
    # back within $RepeatMinutes no matter what stopped it
    $atBoot = New-ScheduledTaskTrigger -AtStartup
    $repeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date
    $repeat.Repetition = New-Repetition

    Register-ScheduledTask -TaskName $TaskName -Force `
        -Action $action -Trigger @($atBoot, $repeat) `
        -Settings (Get-WantedSettings) `
        -User 'SYSTEM' -RunLevel Highest | Out-Null

    Write-Output "registered task '$TaskName' (no time limit, repeat $REPEAT)"
}


# Bring a settings object in line with what a long-running poller
# needs. MUTATES $s; returns one string per change, empty when it
# was already correct. Pure decision logic - no task store access,
# so it can be unit tested.
function Repair-Settings {
    param([Parameter(Mandatory = $true)]$s)

    $changes = @()

    # --- the killer: a finite execution time limit (default P3D/
    # PT72H). PT0S means "do not stop this task". ---
    if ($s.ExecutionTimeLimit -ne 'PT0S') {
        $changes += "ExecutionTimeLimit $($s.ExecutionTimeLimit)->PT0S"
        $s.ExecutionTimeLimit = 'PT0S'
    }
    # a repeat launch must never stop the instance already running
    if ($s.MultipleInstances -ne 'IgnoreNew') {
        $changes += "MultipleInstances $($s.MultipleInstances)->IgnoreNew"
        $s.MultipleInstances = 'IgnoreNew'
    }
    # battery rules kill agents on any laptop in the fleet
    if ($s.DisallowStartIfOnBatteries) {
        $changes += 'DisallowStartIfOnBatteries->False'
        $s.DisallowStartIfOnBatteries = $false
    }
    if ($s.StopIfGoingOnBatteries) {
        $changes += 'StopIfGoingOnBatteries->False'
        $s.StopIfGoingOnBatteries = $false
    }
    # so a missed boot trigger still starts the agent later
    if (-not $s.StartWhenAvailable) {
        $changes += 'StartWhenAvailable->True'
        $s.StartWhenAvailable = $true
    }

    return $changes
}


# True when any trigger already repeats. Written defensively:
# Repetition is absent on some trigger types under StrictMode.
function Test-HasRepeat {
    param($triggers)

    foreach ($t in @($triggers)) {
        if ($null -eq $t) { continue }
        $rep = $t.PSObject.Properties['Repetition']
        if ($rep -and $rep.Value -and $rep.Value.Interval) {
            return $true
        }
    }
    return $false
}


function Invoke-Repair {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        # not installed as a task (e.g. run by hand) - nothing to do
        return
    }

    $s = $task.Settings
    $changes = @(Repair-Settings -s $s)

    # --- liveness net: a repeating trigger ---
    $triggers = $null
    if (-not (Test-HasRepeat $task.Triggers)) {
        try {
            $repeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date
            $repeat.Repetition = New-Repetition
            $triggers = @($task.Triggers) + $repeat
            $changes += "added repeat trigger $REPEAT"
        } catch {
            # not fatal - the time-limit fix above is the real one
            Write-Output "WARN could not add repeat trigger: $($_.Exception.Message)"
        }
    }

    if ($changes.Count -eq 0) {
        return                      # already correct, stay silent
    }

    if ($null -ne $triggers) {
        Set-ScheduledTask -TaskName $TaskName -Settings $s `
            -Trigger $triggers | Out-Null
    } else {
        Set-ScheduledTask -TaskName $TaskName -Settings $s | Out-Null
    }

    Write-Output ("task settings repaired: " + ($changes -join '; '))
}


if ($LoadOnly) { return }        # tests dot-source us; do nothing

try {
    if ($Register) { Invoke-Register } else { Invoke-Repair }
    exit 0
} catch {
    Write-Output "ERROR $($_.Exception.Message)"
    exit 1
}
