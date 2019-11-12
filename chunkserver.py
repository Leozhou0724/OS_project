import rpyc
from rpyc.utils.server import ThreadedServer
import os
path = './files/chunk/'


class chunk_service(rpyc.Service):
    

    file_table = {}

    def on_connect(self, conn):
        print('Connected to the chunk server!')

    def on_disconnect(self, conn):
        print('Disconnected with the chunk server.')

    ############################################################
    #199 start here
    ############################################################

    #--------------------
    #initialize 
    #--------------------
    chunkloc = 0
    chunktable = {}
    local_filesystem_root = "/tmp/gfs/chunks/" + repr(chunkloc)
    if not os.access(local_filesystem_root, os.W_OK):
        os.makedirs(local_filesystem_root)

    # def __init__(self, chunkloc):
    #     self.chunkloc = chunkloc
    #     self.chunktable = {}

    #     self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(chunkloc)
    #     if not os.access(self.local_filesystem_root, os.W_OK):
    #         os.makedirs(self.local_filesystem_root)

    def exposed_write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "wb") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def exposed_read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "rb") as f:
            data = f.read()
        return data

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" \
            + str(chunkuuid) + '.gfs'
        return local_filename


if __name__ == "__main__":
    t = ThreadedServer(chunk_service, port=8888)
    t.start()
#& C:/Users/51934/Anaconda3/envs/tensorflow/python.exe c:/Users/51934/Desktop/Python/OST/chunkserver.py