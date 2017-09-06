from views import Diff, Upload


def setup_routes(app):
    app.router.add_get('/', Upload)
    app.router.add_get('/diff', Diff)

