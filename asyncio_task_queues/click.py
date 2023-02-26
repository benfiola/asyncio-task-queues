import asyncio

import click
import click.core

from asyncio_task_queues.app import App


class Group(click.core.Group):
    app: App

    def __init__(self, app: App):
        super().__init__()
        self.app = app

        commands = [("ping", self.ping)]
        for name, callback in commands:
            self.add_command(click.Command(name=name, callback=callback))

    def ping(self):
        click.echo(asyncio.run(self.app.ping()))
