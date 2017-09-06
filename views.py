import aiohttp_jinja2 as aiohttp_jinja2
from aiohttp.web import View

from run import compare


class Diff(View):
    @aiohttp_jinja2.template('diff_table.jinja2')
    async def get(self):
        return compare()


class Upload(View):
    @aiohttp_jinja2.template('diff_table.jinja2')
    async def get(self):
        return compare()
