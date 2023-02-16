from rich import print

from mmy.mysql.hosts import MySQLHosts
from mmy.server import Server, State

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
