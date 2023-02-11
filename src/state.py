import asyncio

from rich import print

from .etcd import MySQLEtcdClient, MySQLEtcdData
from .pool import ServerPool
from .server import Server, State


async def _state_main():
    cli = MySQLEtcdClient()
    data: MySQLEtcdData = await cli.get()

    if len(data.nodes) == 0:
        print("[bold]No MySQL nodes...[/bold]")
        return

    def delimiter_print(func):
        DELIMITER = "========================="

        def _w(*args, **kwargs):
            print(DELIMITER)
            func(*args, **kwargs)
            print(DELIMITER)

        return _w

    @delimiter_print
    def print_nodes():
        def _state_str(state: State) -> str:
            match state:
                case State.Run:
                    return f"[bold turquoise2]{state.value}[/bold turquoise2]"
                case State.Move:
                    return f"[bold chartreuse3]{state.value}[/bold chartreuse3]"
                case State.Broken:
                    return f"[bold red]{state.value}[/bold red]"
                case State.Unknown:
                    return f"[bold white]{state.value}[/bold white]"

        sp = ServerPool()
        # Show state per node
        for index, node in enumerate(data.nodes):
            sp.add(node)
            print("[bold]Node%d[/bold]: %s:%d" % (index, node.host, node.port))
            print("\tstate: %s" % (_state_str(node.state)))

        def _address_from_server(ss: list[Server]):
            return ["[bold]%s:%d[/bold]" % (s.host, s.port) for s in ss]

        def _print_state_map(sp: ServerPool, state: State):
            # Summarize
            _state = getattr(sp, state.value)
            return "%s" % (
                ", ".join(_address_from_server(_state))
                if len(_state) > 0
                else "[bold]No host[/bold]",
            )

        print("\n")
        from rich.table import Table

        table = Table(
            title="Summarize",
            caption="All node with state",
            expand=True,
            show_lines=False,
        )
        table.add_column("State", justify="center")
        table.add_column("Nodes", justify="center")
        table.add_row(_state_str(State.Run), _print_state_map(sp, State.Run))
        table.add_row(_state_str(State.Move), _print_state_map(sp, State.Move))
        table.add_row(_state_str(State.Broken), _print_state_map(sp, State.Broken))
        table.add_row(_state_str(State.Unknown), _print_state_map(sp, State.Unknown))
        print(table)

    print_nodes()
    pass


def state_main():
    asyncio.run(_state_main())
