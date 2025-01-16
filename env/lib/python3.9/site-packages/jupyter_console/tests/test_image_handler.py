# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import base64
import os
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import pytest

from jupyter_console.ptshell import ZMQTerminalInteractiveShell


SCRIPT_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'writetofile.py')

class NonCommunicatingShell(ZMQTerminalInteractiveShell):
    """A testing shell class that doesn't attempt to communicate with the kernel"""
    def init_kernel_info(self):
        pass


class ZMQTerminalInteractiveShellTestCase(unittest.TestCase):

    def setUp(self):
        self.shell = NonCommunicatingShell()
        self.raw = b'dummy data'
        self.mime = 'image/png'
        self.data = {self.mime: base64.encodebytes(self.raw).decode('ascii')}

    def test_call_pil_by_default(self):
        pil_called_with = []

        def pil_called(data, mime):
            pil_called_with.append(data)

        def raise_if_called(*args, **kwds):
            assert False

        shell = self.shell
        shell.handle_image_PIL = pil_called
        shell.handle_image_stream = raise_if_called
        shell.handle_image_tempfile = raise_if_called
        shell.handle_image_callable = raise_if_called

        shell.handle_image(None, None)  # arguments are dummy
        assert len(pil_called_with) == 1

    def test_handle_image_PIL(self):
        pytest.importorskip('PIL')
        from PIL import Image, ImageShow

        open_called_with = []
        show_called_with = []

        def fake_open(arg):
            open_called_with.append(arg)

        def fake_show(img):
            show_called_with.append(img)

        with patch.object(Image, 'open', fake_open), \
             patch.object(ImageShow, 'show', fake_show):
            self.shell.handle_image_PIL(self.data, self.mime)

        self.assertEqual(len(open_called_with), 1)
        self.assertEqual(len(show_called_with), 1)
        self.assertEqual(open_called_with[0].getvalue(), self.raw)

    def check_handler_with_file(self, inpath, handler):
        shell = self.shell
        configname = '{0}_image_handler'.format(handler)
        funcname = 'handle_image_{0}'.format(handler)

        assert hasattr(shell, configname)
        assert hasattr(shell, funcname)

        with TemporaryDirectory() as tmpdir:
            outpath = os.path.join(tmpdir, 'data')
            cmd = [sys.executable, SCRIPT_PATH, inpath, outpath]
            setattr(shell, configname, cmd)
            getattr(shell, funcname)(self.data, self.mime)
            # cmd is called and file is closed.  So it's safe to open now.
            with open(outpath, 'rb') as file:
                transferred = file.read()

        self.assertEqual(transferred, self.raw)

    def test_handle_image_stream(self):
        self.check_handler_with_file('-', 'stream')

    def test_handle_image_tempfile(self):
        self.check_handler_with_file('{file}', 'tempfile')

    def test_handle_image_callable(self):
        called_with = []
        self.shell.callable_image_handler = called_with.append
        self.shell.handle_image_callable(self.data, self.mime)
        self.assertEqual(len(called_with), 1)
        assert called_with[0] is self.data
