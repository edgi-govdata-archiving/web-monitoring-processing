from web_monitoring_diff.server.server import make_app, start_app, cli
# Suppress Pyflakes Warnings
assert make_app
assert start_app


if __name__ == '__main__':
    cli()
