import React, { useState, useEffect } from "react";

function createTodo(description, cb) {
  fetch(`/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `mutation newTodo {
        createTodo(description: "`+ description + `", dueDate:"24-10-2020") {
          success
          errors
          todo {
            id
            completed
            description
          }
        }
      }`,
    }),
  })
    .then(res => res.json())
    .then(res => cb(res.data))
    .catch(console.error)
}

function getTodos(cb) {
  fetch(`/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `query fetchAllTodos {
        todos {
          success
          errors
          todos {
            description
            completed
            id
          }
        }
      }`,
    }),
  })
    .then(res => res.json())
    .then(res => cb(res.data.todos))
    .catch(console.error)
}

function App() {
  const [myTodos, setMyTodos] = useState([])
  const [todo, setTodo] = useState({
    description: "test",
  })

  function onSubmitOrderForm(e) {
    e.preventDefault()

    // Let's create this API call shortly
    createTodo(todo.description, ({ createTodo }) => {
      setMyTodos([...myTodos, createTodo.todo])
    })
  }

  useEffect(() => {
    getTodos(data => setMyTodos(data.todos))
  }, [])


  return (
    <div className="App">
      <h1>Todos</h1>
      <h3>Create Todo</h3>
      <header className="App-header">
        <form onSubmit={onSubmitOrderForm}>
          <label>
            description:{" "}
            <input
              type="text"
              onChange={({ target }) =>
                setTodo({ ...todo, description: target.value })
              }
            />
          </label>
          <input type="submit" value="New Todo" />
        </form>
        <h3>My Todos</h3>
        <ul>
          {myTodos.map(item => (
            <li key={item.id}>
              {item.description} 
            </li>
          ))}
        </ul>
      </header>
    </div>
  )
}

export default App;