import rpyc
from rpyc.utils.server import ThreadedServer


class master_service(rpyc.Service):
    def on_connect(self, conn):
        print('Connected to the master server!')

    def on_disconnect(self, conn):
        print('Disconnected with the master server.')

    def exposed_test(self):
        return 11

    def exposed_test1(self):
        return 12


if __name__ == "__main__":
    t = ThreadedServer(master_service, port=18861)
    t.start()
