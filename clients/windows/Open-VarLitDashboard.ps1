[CmdletBinding()]
param(
    [string]$SshHost = "var-lit",
    [ValidateRange(1024, 65535)]
    [int]$LocalPort = 18780,
    [ValidateRange(5, 120)]
    [int]$TimeoutSeconds = 30
)

$ErrorActionPreference = "Stop"
$ssh = Get-Command ssh.exe -ErrorAction SilentlyContinue
if (-not $ssh) {
    throw "OpenSSH Client is not installed. See docs/WINDOWS_DASHBOARD.md."
}

function Test-LocalPort([int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $result = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        return $result.AsyncWaitHandle.WaitOne(250) -and $client.Connected
    }
    finally {
        $client.Dispose()
    }
}

if (Test-LocalPort $LocalPort) {
    throw "Local port $LocalPort is already in use. Choose another -LocalPort."
}

$forward = "${LocalPort}:127.0.0.1:8780"
$arguments = @(
    "-N", "-T",
    "-o", "ExitOnForwardFailure=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=6",
    "-L", $forward,
    $SshHost
)

$process = Start-Process -FilePath $ssh.Source -ArgumentList $arguments -PassThru
try {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while (-not (Test-LocalPort $LocalPort)) {
        if ($process.HasExited) {
            throw "SSH tunnel exited with code $($process.ExitCode). Check SSH and firewall settings."
        }
        if ([DateTime]::UtcNow -ge $deadline) {
            throw "Timed out waiting for local port $LocalPort."
        }
        Start-Sleep -Milliseconds 250
    }

    $url = "http://127.0.0.1:$LocalPort"
    Start-Process $url
    Write-Host "Dashboard opened at $url"
    Read-Host "Press Enter to close this SSH tunnel"
}
finally {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id
        $process.WaitForExit()
    }
}
