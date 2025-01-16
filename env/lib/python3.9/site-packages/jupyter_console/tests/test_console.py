"""Tests for two-process terminal frontend"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import os
import shutil
import sys
import tempfile
from subprocess import check_output

from flaky import flaky
import pytest

from traitlets.tests.utils import check_help_all_output


should_skip = sys.platform == "win32" or sys.version_info < (3,8) or sys.version_info[:2] == (3, 10)  # noqa


@flaky
@pytest.mark.skipif(should_skip, reason="not supported")
def test_console_starts():
    """test that `jupyter console` starts a terminal"""
    p, pexpect, t = start_console()
    p.sendline("5")
    p.expect([r"Out\[\d+\]: 5", pexpect.EOF], timeout=t)
    p.expect([r"In \[\d+\]", pexpect.EOF], timeout=t)
    stop_console(p, pexpect, t)

def test_help_output():
    """jupyter console --help-all works"""
    check_help_all_output('jupyter_console')


@flaky
@pytest.mark.skipif(should_skip, reason="not supported")
def test_display_text():
    "Ensure display protocol plain/text key is supported"
    # equivalent of:
    #
    #   x = %lsmagic
    #   from IPython.display import display; display(x);
    p, pexpect, t = start_console()
    p.sendline('x = %lsmagic')
    p.expect(r'In \[\d+\]', timeout=t)
    p.sendline('from IPython.display import display; display(x);')
    p.expect(r'Available line magics:', timeout=t)
    p.expect(r'In \[\d+\]', timeout=t)
    stop_console(p, pexpect, t)

def stop_console(p, pexpect, t):
    "Stop a running `jupyter console` running via pexpect"
    # send ctrl-D;ctrl-D to exit
    p.sendeof()
    p.sendeof()
    p.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=t)
    if p.isalive():
        p.terminate()


def start_console():
    "Start `jupyter console` using pexpect"
    import pexpect
    
    args = ['-m', 'jupyter_console', '--colors=NoColor']
    cmd = sys.executable
    env = os.environ.copy()
    env["JUPYTER_CONSOLE_TEST"] = "1"
    env["PROMPT_TOOLKIT_NO_CPR"] = "1"

    try:
        p = pexpect.spawn(cmd, args=args, env=env)
    except IOError:
        pytest.skip("Couldn't find command %s" % cmd)
    
    # timeout after two minutes
    t = 120
    p.expect(r"In \[\d+\]", timeout=t)
    return p, pexpect, t


def test_multiprocessing():
    p, pexpect, t = start_console()
    p.sendline('')


def test_generate_config():
    """jupyter console --generate-config works"""
    td = tempfile.mkdtemp()
    try:
        check_output([sys.executable, '-m', 'jupyter_console', '--generate-config'],
            env={'JUPYTER_CONFIG_DIR': td},
        )
        assert os.path.isfile(os.path.join(td, 'jupyter_console_config.py'))
    finally:
        shutil.rmtree(td)
