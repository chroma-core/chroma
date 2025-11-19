# Chroma CLI Installer Script for Windows (PowerShell)
# Usage:
#   iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1'))

$ErrorActionPreference = 'Stop'

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if ($isAdmin) {
    $expectedInstallDir = Join-Path $env:ProgramFiles "Chroma"
} else {
    $expectedInstallDir = Join-Path $env:USERPROFILE "bin"
}
$expectedInstallPath = Join-Path $expectedInstallDir "chroma.exe"

$repo    = "chroma-core/chroma"
$release = "cli-1.2.2"
$asset   = "chroma-windows.exe"

$downloadUrl = "https://github.com/$repo/releases/download/$release/$asset"
Write-Host "Downloading $asset from $downloadUrl ..."

$tempFile = Join-Path $env:TEMP "chroma.exe"
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile

if (-not (Test-Path $expectedInstallDir)) {
    New-Item -ItemType Directory -Path $expectedInstallDir -Force | Out-Null
}

Move-Item -Path $tempFile -Destination $expectedInstallPath -Force

Write-Host "Chroma has been installed to: $expectedInstallPath"

$pathDirs = $env:PATH -split ';'
if ($pathDirs -notcontains $expectedInstallDir) {
    Write-Warning "WARNING ⚠️: The directory '$expectedInstallDir' is not in your PATH."
    Write-Host "To add it for the current session, run:"
    Write-Host "    `$env:PATH = '$expectedInstallDir;' + `$env:PATH"
    Write-Host "To add it permanently, update your system environment variables."
}
