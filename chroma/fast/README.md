This is a [Next.js](https://nextjs.org/) project bootstrapped with [`create-next-app`](https://github.com/vercel/next.js/tree/canary/packages/create-next-app). It demonstrates how to use Strawberry, FastAPI, SQLAlchemy, and NextJS together. It makes use of `graphql-codegen` to automatically generate `urql` hooks based on GraphQL API that are ready to use in your React/NextJS code.


## Getting Started

First, install the Python dependencies:

```
$ pip install -r requirements.txt
```

Next, install the npm based dependencies:

```
$ npm install
```

Create the db:

```
$ python models.py
```

Now, run the `uvicorn` server:

```
$ uvicorn app:app --reload --host '::'
```

Finally, run the NextJS development server:

```bash
npm run dev
# or
yarn dev
```

Now you can go to `http://127.0.0.1:3000/graphql` to explore the interactive GraphiQL app and start developing your NextJS app.


This tut is at https://blog.logrocket.com/using-graphql-strawberry-fastapi-next-js/

Alembic tut
https://kimsereylam.com/sqlalchemy/2019/10/18/get-started-with-alembic.html


# adding a new object
1. alembic migration
2. add to models.py for sqlalchemy
3. add to grapqhl_py/types
4. add query
5. add mutation
6. add graphql_js/operations.grapqhl
7. run codegen
8. use urql hook


### Mutiple apps
uvicorn data_manager:app --reload --host '::' --port 5001


```
{
  embeddings(first: 5, after:"MQ==") {
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    edges {
      node {
        name
        id
      }
      cursor
    }
  }
}
```

https://medium.com/thelorry-product-tech-data/celery-asynchronous-task-queue-with-fastapi-flower-monitoring-tool-e7135bd0479f

http://www.prschmid.com/2013/04/using-sqlalchemy-with-celery-tasks.html