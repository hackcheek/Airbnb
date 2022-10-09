import asyncio
from typing import List
from typing import Dict
from typing import Tuple
from typing import Union
from typing import Optional
from aiohttp import ClientSession


class AsyncRequest:

    def __init__(
        self, 
        urls: List[str], 
        params: Optional[Dict] = None, 
        semaphore: int = 1000
    ):
        self.urls = (urls, ) if isinstance(urls, str) else urls
        self.params = params if params else dict()
        self.semaphore = semaphore
        self.error = list()


    async def __fetch(
        self, url: str, session: ClientSession
    ) -> Union[bytes, None]:
        try:
            async with session.get(url, params=self.params) as response:
                date = response.headers.get("DATE")
                print("{}:{}".format(date, response.url))
                return await response.read()

        except asyncio.TimeoutError:
            self.error.append(f"timeout error on {url}")
            return None

        except Exception as msg:
            self.error.append(f"{msg} on {url}")


    async def __bound_fetch(
        self, sem: asyncio.Semaphore, url: str, session: ClientSession
    ) -> Union[bytes, None]:
        async with sem:
            return await self.__fetch(url, session)


    async def requester(self) -> List[bytes]:
        sem = asyncio.Semaphore(self.semaphore)

        async with ClientSession() as session:
            tasks = [
                asyncio.ensure_future(self.__bound_fetch(sem, i, session))
                for i in self.urls
            ]
            responses = asyncio.gather(*tasks)
            return await responses


    def run(self) -> Tuple[List[str], List[bytes]]:
        return self.error, asyncio.run(self.requester())
