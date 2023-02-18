import asyncio

from rich import print

from .etcd import EtcdConnectError, MmyEtcdInfo, MySQLEtcdClient, MySQLEtcdData
from .pool import ServerPool
from .server import Server, State, state_rich_str


async def _state_main(
    etcd_info: MmyEtcdInfo,
):
    for etcd in etcd_info.nodes:
        try:
            cli = MySQLEtcdClient(
                host=etcd.host,
                port=etcd.port,
            )
            data: MySQLEtcdData = await cli.get()
            break
        except EtcdConnectError:
            continue
        except Exception as e:
            print(type(e))
    else:
        # No alive etcd node
        raise RuntimeError("No alive etcd nodes")

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
        sp = ServerPool()
        # Show state per node
        for index, node in enumerate(data.nodes):
            sp.add(node)
            print("[bold]Node%d[/bold]: %s:%d" % (index, node.host, node.port))
            print("\tstate: %s" % (state_rich_str(node.state)))

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
        table.add_row(state_rich_str(State.Run), _print_state_map(sp, State.Run))
        table.add_row(state_rich_str(State.Move), _print_state_map(sp, State.Move))
        table.add_row(state_rich_str(State.Broken), _print_state_map(sp, State.Broken))
        table.add_row(
            state_rich_str(State.Unknown), _print_state_map(sp, State.Unknown)
        )
        print(table)

    print_nodes()
    pass


def state_main(
    etcd_info: MmyEtcdInfo,
):
    asyncio.run(_state_main(etcd_info))
