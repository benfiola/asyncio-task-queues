import fastapi
import fastapi.routing

from asyncio_task_queues.app import App


class APIRouter(fastapi.routing.APIRouter):
    app: App

    def __init__(self, app: App):
        super().__init__()
        self.app = app

        routes = [("/ping", self.ping, ["GET"])]
        for path, endpoint, methods in routes:
            self.add_api_route(path=path, endpoint=endpoint, methods=methods)

    async def ping(self) -> str:
        return await self.app.ping()
