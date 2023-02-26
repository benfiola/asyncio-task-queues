import asyncio

import flask

from asyncio_task_queues.app import App


class Blueprint(flask.Blueprint):
    app: App

    def __init__(self, name: str, app: App):
        super().__init__(name, __name__)
        self.app = app

        routes = [("/ping", self.ping, ["GET"])]
        for rule, view_func, methods in routes:
            self.add_url_rule(rule=rule, view_func=view_func, methods=methods)

    def ping(self) -> str:
        return asyncio.run(self.app.ping())
