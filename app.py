import os

import aiohttp_jinja2
import jinja2
import utils
from aiohttp import web

from app_routes import setup_routes

PROJ_ROOT = utils.get_proj_dir()
TEMPLATES_DIR = os.path.join(PROJ_ROOT, 'templates')
app = web.Application()

aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(TEMPLATES_DIR))
app.router.add_static('/static/', path=os.path.join(PROJ_ROOT, 'static'), name='static')
setup_routes(app)

if __name__ == '__main__':
    web.run_app(app, port=8881)
