"""Run the physical correctness executable as two native local processes first."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time


def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--binary',required=True,type=Path)
    p.add_argument('--reader',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    sockets=[]
    try:
        for _ in range(4):
            s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s)
        addresses=[f'127.0.0.1:{s.getsockname()[1]}' for s in sockets]
    finally:
        for s in sockets:s.close()
    token=a.output/'token';token.write_text(secrets.token_hex(32));token.chmod(0o600)
    deadline=int(time.time())+120
    identity=secrets.randbits(120)
    common=['--run',str(identity),'--input-file',str(a.output/'fixture.f32'),'--token-file',str(token),'--shutdown-at-unix',str(deadline)]
    commands=[]
    for owner in (1,2):
        local=(owner-1)*2;remote=(2-owner)*2
        command=[str(a.binary),'--mode','run' if owner==1 else 'serve','--owner',str(owner),'--output',str(a.output/f'owner{owner}'),'--data-bind',addresses[local],'--control-bind',addresses[local+1],'--peer-data',addresses[remote],'--peer-control',addresses[remote+1]]+common
        if owner==1:command+=['--reader-binary',str(a.reader)]
        commands.append(command)
    write(a.output/'admission.json',{'kind':'two native processes on one Mac, not physical two-host evidence','deadline':deadline,'binary':str(a.binary),'binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest(),'reader':str(a.reader),'reader_sha256':hashlib.sha256(a.reader.read_bytes()).hexdigest(),'commands':commands})
    children=[]; streams=[]; outcome={'status':'failed'}
    try:
        for owner in (2,1):
            stdout=(a.output/f'owner{owner}.stdout').open('wb');stderr=(a.output/f'owner{owner}.stderr').open('wb');streams.extend([stdout,stderr])
            child=subprocess.Popen(commands[owner-1],stdout=stdout,stderr=stderr,start_new_session=True);children.append(child)
            if owner==2:
                ready=False
                while time.time()<deadline-10 and child.poll() is None:
                    event=a.output/'owner2/events.jsonl'
                    if event.exists() and 'server_ready' in event.read_text():ready=True;break
                    time.sleep(.05)
                if not ready:raise RuntimeError('Peer failed to become ready')
            else:
                code=child.wait(timeout=max(1,deadline-time.time()))
                if code:raise RuntimeError(f'Coordinator exited {code}')
                outcome={'status':'passed','coordinator_exit':code,'result_path':str(a.output/'owner1/result.json')}
    except BaseException as error:
        outcome['error']=str(error)
        raise
    finally:
        for child in children:
            try:os.killpg(child.pid,signal.SIGTERM)
            except ProcessLookupError:pass
        time.sleep(.1)
        for child in children:
            if child.poll() is None:
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                except PermissionError:
                    if child.poll() is None:raise
            child.wait()
        for stream in streams:stream.close()
        outcome['process_exit_codes']=[child.returncode for child in children]
        write(a.output/'process.json',outcome)
    print(json.dumps(outcome))


if __name__=='__main__':main()
