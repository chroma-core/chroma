# -*- coding: utf-8 -*-
"""Adapt readline completer interface to make ZMQ request."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from traitlets.config import Configurable
from traitlets import Float

from jupyter_console.utils import run_sync


class ZMQCompleter(Configurable):
    """Client-side completion machinery.

    How it works: self.complete will be called multiple times, with
    state=0,1,2,... When state=0 it should compute ALL the completion matches,
    and then return them for each value of state."""

    timeout = Float(5.0, config=True, help='timeout before completion abort')
    
    def __init__(self, shell, client, config=None):
        super(ZMQCompleter,self).__init__(config=config)

        self.shell = shell
        self.client =  client
        self.matches = []
    
    def complete_request(self, code, cursor_pos):
        # send completion request to kernel
        # Give the kernel up to 5s to respond
        msg_id = self.client.complete(
            code=code,
            cursor_pos=cursor_pos,
        )
    
        msg = run_sync(self.client.shell_channel.get_msg)(timeout=self.timeout)
        if msg['parent_header']['msg_id'] == msg_id:
            return msg['content']

        return {'matches': [], 'cursor_start': 0, 'cursor_end': 0,
                'metadata': {}, 'status': 'ok'}

