export function waitForConnection(url: string, exit: boolean, period: number = 1000): Promise<boolean> {
    return new Promise(async (resolve, reject) => {
        while (!exit) {
            try {
              await fetch(url);
              resolve(true);
              break;
            } catch (error) {
              await new Promise(resolve => setTimeout(resolve, period));
            }
        }
        resolve(false);
    })
}

export interface CodeChunk {
    source_code: string;
    repo: string;
    file_path: string;
    func_name: string;
    language: string;
    start_line: number;
    url?: string;
}
