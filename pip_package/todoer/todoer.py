from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

import gorilla
import pprint

# Select your transport with a defined url endpoint
transport = AIOHTTPTransport(url="http://127.0.0.1:5000/graphql")

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# Provide a GraphQL query
get_all_todos = gql(
    """
   query fetchAllTodos {
    todos {
      success
      errors
      todos {
        description
        completed
        id
      }
    }
  }
"""
)

create_todo = gql(
  """
  mutation newTodo ($description: String!) {
        createTodo(description: $description, dueDate:"24-10-2020") {
          success
          errors
          todo {
            id
            completed
            description
          }
        }
      }
  """
)

def patched_pprint(self): 
    print("hello monkeys!")
    pass

class Todoer:

    def __init__(self):
        print("Todoer inititated, FYI: monkey patching pprint")
        settings = gorilla.Settings(allow_hit=True, store_hit=True)
        patch_pprint = gorilla.Patch(pprint, "pprint", patched_pprint, settings)
        gorilla.apply(patch_pprint)

    def get_todos(self):
      result = client.execute(get_all_todos)
      print(str(result))
    
    def create_todo(self, description):
      params = {"description": description}
      result = client.execute(create_todo, variable_values=params)
      print(str(result))