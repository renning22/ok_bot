"""
Make sure all underlying I/O modules that need to patched by eventlet are here
and they must precede any other module imports.

Do not import_patched directly, do this instead:

  from .patched_io_modules import websocket

See bug: https://github.com/renning22/ok_bot/issues/45
"""

import sys

import eventlet

# Double-check these modules have not been imported yet.
assert 'websocket' not in sys.modules
assert 'requests' not in sys.modules

websocket = eventlet.import_patched('websocket')
requests = eventlet.import_patched('requests')

# Double-check they have been patched.
assert websocket.recv.__globals__['socket'] is eventlet.green.socket
assert websocket._socket.recv.__globals__['socket'] is eventlet.green.socket
