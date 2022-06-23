import models
import inspect
import asyncio

async def seed_the_database():
    async with models.get_session() as s:
        project = models.Project()
        s.add(project)
        await s.flush()
        print("adding project, here is the id: " + str(project.id))


if __name__ == "__main__":
    print("Seeding chroma.db with test data")
    
    if inspect.iscoroutinefunction(seed_the_database):
        task = seed_the_database()
        res = asyncio.get_event_loop().run_until_complete(task)