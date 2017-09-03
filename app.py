import aiohttp_jinja2
import jinja2
import os
from aiohttp import web

from app_routes import setup_routes
project_root = os.path.abspath(os.path.curdir)
app = web.Application()

# load config from yaml file in current dir
aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('/home/padalko/dev/csv_diff/templates'))
app.router.add_static('/static/', path=os.path.join(project_root, 'static'), name='static')
setup_routes(app)

if __name__ == '__main__':
    web.run_app(app, port=8881)
