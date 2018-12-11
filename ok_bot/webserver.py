import asyncio
import sys

import socketio
from aiohttp import web

routes = web.RouteTableDef()
app = web.Application()
sio = socketio.AsyncServer()
sio.attach(app)


@routes.get('/')
async def index(request):
    return web.FileResponse('static/index.html')

app.add_routes(routes)


async def emit_add_line(line):
    await sio.emit('add line', line)


@sio.on('connect')
def connect(sid, environ):
    print("connect: {}".format(sid))


@sio.on('disconnect')
def disconnect(sid):
    print('disconnect: {}'.format(sid))


async def watch_log(filename):
    proc = await asyncio.create_subprocess_exec(
        'tail', '-f', filename,
        stdout=asyncio.subprocess.PIPE)

    # Read one line of output.
    while True:
        data = await proc.stdout.readline()
        line = data.decode('ascii').rstrip()
        await emit_add_line(line)


async def start_background_tasks(app):
    app['log_watcher'] = app.loop.create_task(watch_log('1.log'))


async def cleanup_background_tasks(app):
    print('cleanup background tasks...')
    app['log_watcher'].cancel()
    await app['log_watcher']


if __name__ == '__main__':
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    web.run_app(app)
