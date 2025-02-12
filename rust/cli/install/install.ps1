# Chroma CLI Installer Script for Windows (PowerShell)
# Usage:
#   iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1'))

$ErrorActionPreference = 'Stop'

$repo    = "chroma-core/chroma"
$release = "cli-0.1.0"
$asset = "chroma-windows.exe"

$downloadUrl = "https://github.com/$repo/releases/download/$release/$asset"
Write-Host "Downloading $asset from $downloadUrl ..."

$tempFile = Join-Path $env:TEMP "chroma.exe"
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if ($isAdmin) {
    $installDir = Join-Path $env:ProgramFiles "Chroma"
} else {
    $installDir = Join-Path $env:USERPROFILE "bin"
}

if (-not (Test-Path $installDir)) {
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
}

$installPath = Join-Path $installDir "chroma.exe"
Move-Item -Path $tempFile -Destination $installPath -Force

Write-Host "Chroma has been installed to: $installPath"

$pathDirs = $env:PATH -split ';'
if ($pathDirs -notcontains $installDir) {
    Write-Warning "WARNING ⚠️: The directory '$installDir' is not in your PATH."
    Write-Host "To add it for the current session, run:"
    Write-Host "    `\$env:PATH = '$installDir;' + \$env:PATH"
    Write-Host "To add it permanently, update your system environment variables."
}
