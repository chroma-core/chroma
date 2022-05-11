import pprint
from todoer import todoer

todoer = todoer.Todoer()
todoer.get_todos()
todoer.create_todo("this is from pip!")
pprint.pprint('this should trigger an additional print')