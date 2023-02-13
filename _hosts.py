from rich import print
from src.mysql.hosts import MySQLHosts
from src.server import Server, State

mh = MySQLHosts()
mh.update(
    nodes=[
        Server(
            host="127.0.0.1",
            port=3306,
            state=State.Run,
        )
    ]
)

a = mh.get_host("hello")
print(a)
