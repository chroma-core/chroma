#!/usr/bin/env node
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';
import process from 'process';

function findChromaCliBinary(): string | null {
    let defaultPaths: string[] = [];

    if (process.platform === 'win32') {
        defaultPaths = [
            path.join(process.env['ProgramFiles'] || 'C:\\Program Files', 'Chroma', 'chroma.exe'),
            path.join(process.env['USERPROFILE'] || '', 'bin', 'chroma.exe')
        ];
    } else {
        defaultPaths = [
            '/usr/local/bin/chroma',
            path.join(os.homedir(), '.local/bin/chroma')
        ];
    }

    for (const p of defaultPaths) {
        try {
            if (fs.existsSync(p)) {
                fs.accessSync(p, fs.constants.X_OK);
                return p;
            }
        } catch (e) {
            // Ignore errors and continue checking other paths.
        }
    }
    return null;
}

function installChromaCliUnix(): void {
    const installScriptUrl = 'https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh';
    console.log('Chroma CLI not found. Installing using the official installer script…');
    try {
        const curlResult = spawnSync('curl', ['-sSL', installScriptUrl], { encoding: 'utf-8', stdio: ['ignore', 'pipe', 'inherit'] });
        if (curlResult.status !== 0) {
            console.error('Failed to download installer script.');
            process.exit(1);
        }
        const bashResult = spawnSync('bash', { input: curlResult.stdout, stdio: 'inherit' });
        if (bashResult.status !== 0) {
            console.error('Failed to install chroma CLI tool on Unix.');
            process.exit(1);
        }
    } catch (e) {
        console.error('Failed to install chroma CLI tool on Unix.');
        process.exit(1);
    }
}

function installChromaCliWindows(): void {
    const installScriptUrl = 'https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1';
    console.log('Chroma CLI not found. Installing using the official PowerShell installer script…');
    try {
        const psResult = spawnSync('powershell', [
            '-NoProfile',
            '-ExecutionPolicy', 'Bypass',
            '-Command', `iex (New-Object Net.WebClient).DownloadString('${installScriptUrl}')`
        ], { stdio: 'inherit' });
        if (psResult.status !== 0) {
            console.error('Failed to install chroma CLI tool on Windows.');
            process.exit(1);
        }
    } catch (e) {
        console.error('Failed to install chroma CLI tool on Windows.');
        process.exit(1);
    }
}

function ensureChromaCliInstalled(): string {
    let chromaPath = findChromaCliBinary();
    if (chromaPath) {
        return chromaPath;
    }

    if (process.platform === 'win32') {
        installChromaCliWindows();
    } else {
        installChromaCliUnix();
    }

    chromaPath = findChromaCliBinary();
    if (!chromaPath) {
        console.error("Installation failed: 'chroma' not found after installation.");
        process.exit(1);
    }
    return chromaPath;
}

function main(): void {
    const args: string[] = process.argv.slice(2);
    const chromaPath: string = ensureChromaCliInstalled();
    try {
        const result = spawnSync(chromaPath, args, { stdio: 'inherit' });
        if (result.signal === 'SIGINT') {
            process.exit(130);
        }
        process.exit(result.status ?? 0);
    } catch (e: any) {
        console.error(`Error executing chroma CLI: ${e}`);
        process.exit(1);
    }
}

main();
