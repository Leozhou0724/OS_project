import rpyc
from rpyc.utils.server import ThreadedServer

path = './files/chunk/'


class chunk_service(rpyc.Service):
    def on_connect(self, conn):
        print('Connected to the chunk server!')

    def on_disconnect(self, conn):
        print('Disconnected with the chunk server.')

    def exposed_put(self, data):
        with open(path+'data.txt', 'w') as f:
            f.write(data)

    def exposed_test1(self):
        return 12


if __name__ == "__main__":
    t = ThreadedServer(chunk_service, port=8888)
    t.start()
