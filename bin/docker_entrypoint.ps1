# Stop the script on any errors
$ErrorActionPreference = 'Stop'

# Set environment variables
$env:IS_PERSISTENT = 1
$env:CHROMA_SERVER_NOFILE = 65535
$args = $args

# Function to execute the command
Function Start-Server {
    param (
        [string]$commandLine
    )
    Write-Host "Starting server with args: $commandLine"
    & cmd /c $commandLine
}

# Check if arguments contain 'uvicorn'
if ($args -join ' ' -match '^uvicorn.*') {
    Write-Host "WARNING: Please remove 'uvicorn chromadb.app:app' from your command line arguments. This is now handled by the entrypoint script." -ForegroundColor Red
    Start-Server -commandLine ($args -join ' ')
} else {
    $newArgs = 'uvicorn chromadb.app:app ' + ($args -join ' ')
    Write-Host "Starting 'uvicorn chromadb.app:app' with args: $newArgs"
    Start-Server -commandLine $newArgs
}
