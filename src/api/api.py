import aiohttp
import asyncio


async def requester(json=False, *args, **kwargs):
    print(args)
    async with aiohttp.ClientSession() as session:
        async with session.get(*args, **kwargs) as response:
            if json:
                content = await response.json()
            else:
                content = await response.text()
            status = response.status
            return status, content


def GET(*args, **kwargs):
    return asyncio.run(requester(*args, **kwargs))
