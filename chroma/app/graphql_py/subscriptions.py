import asyncio
from typing import AsyncGenerator

import strawberry

# this is just an example, but a useful one, so leaving it here for now
# note that app.py need some modifications to make this work
# https://strawberry.rocks/docs/general/subscriptions
# @strawberry.type
# class Subscription:
#     @strawberry.subscription
#     async def count(self, target: int = 100) -> AsyncGenerator[int, None]:
#         for i in range(target):
#             yield i
#             await asyncio.sleep(0.5)