# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import argparse
import contextlib
import http.server
import os
import queue
import shutil
import subprocess
import sys
import time
import threading

from pathlib import Path
from io import BytesIO

from selenium import webdriver


class TemplateOverrider(http.server.SimpleHTTPRequestHandler):
    def log_request(self, code="-", size="-"):
        # don't log successful requests
        return

    def do_GET(self) -> bytes | None:
        if self.path.endswith(PYARROW_WHEEL_PATH.name):
            self.send_response(200)
            self.send_header("Content-type", "application/x-zip")
            self.end_headers()
            with PYARROW_WHEEL_PATH.open(mode="rb") as wheel:
                self.copyfile(wheel, self.wfile)
        if self.path.endswith("/test.html"):
            body = b"""
                <!doctype html>
                <html>
                <head>
                    <script>
                        window.python_done_callback = undefined;
                        window.python_logs = [];
                        function capturelogs(evt) {
                            if ('results' in evt.data) {
                                if (window.python_done_callback) {
                                    let callback = window.python_done_callback;
                                    window.python_done_callback = undefined;
                                    callback({result:evt.data.results});
                                }
                            }
                            if ('print' in evt.data) {
                                evt.data.print.forEach((x)=>{window.python_logs.push(x)});
                            }
                        }
                        window.pyworker = new Worker("worker.js");
                        window.pyworker.onmessage = capturelogs;
                    </script>
                </head>
                <body></body>
                </html>
                """
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.send_header("Content-length", len(body))
            self.end_headers()
            self.copyfile(BytesIO(body), self.wfile)
        elif self.path.endswith("/worker.js"):
            body = b"""
                importScripts("./pyodide.js");
                onmessage = async function (e) {
                    const data = e.data;
                    if (!self.pyodide) {
                        self.pyodide = await loadPyodide();
                    }
                    function do_print(arg) {
                        let databytes = Array.from(arg);
                        self.postMessage({print:databytes});
                        return databytes.length;
                    }
                    self.pyodide.setStdout({write:do_print,isatty:data.isatty});
                    self.pyodide.setStderr({write:do_print,isatty:data.isatty});

                    await self.pyodide.loadPackagesFromImports(data.python);
                    let results = await self.pyodide.runPythonAsync(data.python);
                    self.postMessage({results});
                }
                """
            self.send_response(200)
            self.send_header("Content-type", "application/javascript")
            self.send_header("Content-length", len(body))
            self.end_headers()
            self.copyfile(BytesIO(body), self.wfile)

        else:
            return super().do_GET()

    def end_headers(self):
        # Enable Cross-Origin Resource Sharing (CORS)
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()


def run_server_thread(dist_dir, q):
    global _SERVER_ADDRESS
    os.chdir(dist_dir)
    server = http.server.HTTPServer(("", 0), TemplateOverrider)
    q.put(server.server_address)
    print(f"Starting server for {dist_dir} at: {server.server_address}")
    server.serve_forever()


@contextlib.contextmanager
def launch_server(dist_dir):
    q = queue.Queue()
    p = threading.Thread(target=run_server_thread, args=[dist_dir, q], daemon=True)
    p.start()
    address = q.get(timeout=50)
    time.sleep(0.1)  # wait to make sure server is started
    yield address
    p.terminate()


class NodeDriver:
    import subprocess

    def __init__(self, hostname, port):
        self.process = subprocess.Popen(
            [shutil.which("script"), "-c", shutil.which("node")],
            stdin=subprocess.PIPE,
            shell=False,
            bufsize=0,
        )
        print(self.process)
        time.sleep(0.1)  # wait for node to start
        self.hostname = hostname
        self.port = port
        self.last_ret_code = None

    def load_pyodide(self, dist_dir):
        self.execute_js(
            f"""
        const {{ loadPyodide }} = require('{dist_dir}/pyodide.js');
        let pyodide = await loadPyodide();
        """
        )

    def clear_logs(self):
        pass  # we don't handle logs for node

    def write_stdin(self, buffer):
        # because we use unbuffered IO for
        # stdout, stdin.write is also unbuffered
        # so might under-run on writes
        while len(buffer) > 0 and self.process.poll() is None:
            written = self.process.stdin.write(buffer)
            if written == len(buffer):
                break
            elif written == 0:
                # full buffer - wait
                time.sleep(0.01)
            else:
                buffer = buffer[written:]

    def execute_js(self, code, wait_for_terminate=True):
        self.write_stdin((code + "\n").encode("utf-8"))

    def load_arrow(self):
        self.execute_js(f"await pyodide.loadPackage('{PYARROW_WHEEL_PATH}');")

    def execute_python(self, code, wait_for_terminate=True):
        js_code = f"""
            python = `{code}`;
            await pyodide.loadPackagesFromImports(python);
            python_output = await pyodide.runPythonAsync(python);
        """
        self.last_ret_code = self.execute_js(js_code, wait_for_terminate)
        return self.last_ret_code

    def wait_for_done(self):
        # in node we just let it run above
        # then send EOF and join process
        self.write_stdin(b"process.exit(python_output)\n")
        return self.process.wait()


class BrowserDriver:
    def __init__(self, hostname, port, driver):
        self.driver = driver
        self.driver.get(f"http://{hostname}:{port}/test.html")
        self.driver.set_script_timeout(100)

    def load_pyodide(self, dist_dir):
        pass

    def load_arrow(self):
        self.execute_python(
            f"import pyodide_js as pjs\n"
            f"await pjs.loadPackage('{PYARROW_WHEEL_PATH.name}')\n"
        )

    def execute_python(self, code, wait_for_terminate=True):
        if wait_for_terminate:
            self.driver.execute_async_script(
                f"""
                let callback = arguments[arguments.length-1];
                python = `{code}`;
                window.python_done_callback = callback;
                window.pyworker.postMessage(
                    {{python, isatty: {'true' if sys.stdout.isatty() else 'false'}}});
                """
            )
        else:
            self.driver.execute_script(
                f"""
                let python = `{code}`;
                window.python_done_callback= (x) => {{window.python_script_done=x;}};
                window.pyworker.postMessage(
                    {{python,isatty:{'true' if sys.stdout.isatty() else 'false'}}});
                """
            )

    def clear_logs(self):
        self.driver.execute_script("window.python_logs = [];")

    def wait_for_done(self):
        while True:
            # poll for console.log messages from our webworker
            # which are the output of pytest
            lines = self.driver.execute_script(
                "let temp = window.python_logs;window.python_logs=[];return temp;"
            )
            if len(lines) > 0:
                sys.stdout.buffer.write(bytes(lines))
            done = self.driver.execute_script("return window.python_script_done;")
            if done is not None:
                value = done["result"]
                self.driver.execute_script("delete window.python_script_done;")
                return value
            time.sleep(0.1)


class ChromeDriver(BrowserDriver):
    def __init__(self, hostname, port):
        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        super().__init__(hostname, port, webdriver.Chrome(options=options))


class FirefoxDriver(BrowserDriver):
    def __init__(self, hostname, port):
        from selenium.webdriver.firefox.options import Options

        options = Options()
        options.add_argument("--headless")

        super().__init__(hostname, port, webdriver.Firefox(options=options))


def _load_pyarrow_in_runner(driver, wheel_name):
    driver.load_arrow()
    driver.execute_python(
        """import sys
import micropip
if "pyarrow" not in sys.modules:
    await micropip.install("hypothesis")
    import pyodide_js as pjs
    await pjs.loadPackage("numpy")
    await pjs.loadPackage("pandas")
    import pytest
    import pandas # import pandas after pyarrow package load for pandas/pyarrow
                  # functions to work
import pyarrow
    """,
        wait_for_terminate=True,
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "-d",
    "--dist-dir",
    type=str,
    help="Pyodide distribution directory",
    default="./pyodide",
)
parser.add_argument("wheel", type=str, help="Wheel to run tests from")
parser.add_argument(
    "-t", "--test-submodule", help="Submodule that tests live in", default="test"
)
parser.add_argument(
    "-r",
    "--runtime",
    type=str,
    choices=["chrome", "node", "firefox"],
    help="Runtime to run tests in",
    default="chrome",
)
args = parser.parse_args()

PYARROW_WHEEL_PATH = Path(args.wheel).resolve()

dist_dir = Path(os.getcwd(), args.dist_dir).resolve()
print(f"dist dir={dist_dir}")
with launch_server(dist_dir) as (hostname, port):
    if args.runtime == "chrome":
        driver = ChromeDriver(hostname, port)
    elif args.runtime == "node":
        driver = NodeDriver(hostname, port)
    elif args.runtime == "firefox":
        driver = FirefoxDriver(hostname, port)

    print("Load pyodide in browser")
    driver.load_pyodide(dist_dir)
    print("Load pyarrow in browser")
    _load_pyarrow_in_runner(driver, Path(args.wheel).name)
    driver.clear_logs()
    print("Run pytest in browser")
    driver.execute_python(
        """
import pyarrow,pathlib
pyarrow_dir = pathlib.Path(pyarrow.__file__).parent
pytest.main([pyarrow_dir, '-v'])
""",
        wait_for_terminate=False,
    )
    print("Wait for done")
    os._exit(driver.wait_for_done())
