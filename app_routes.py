from views import Diff


def setup_routes(app):
    app.router.add_get('/', Diff)

