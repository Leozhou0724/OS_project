import rpyc
from rpyc.utils.server import ThreadedServer
import time
import uuid


class master_service(rpyc.Service):
    def on_connect(self, conn):
        print('Connected to the master server!')

    def on_disconnect(self, conn):
        print('Disconnected with the master server.')
        
    ######################################
    # 199 start here
    ######################################
    #
    num_chunkservers = 1
    max_chunkservers = 1
    max_chunksperfile = 1
    #
    chunksize = 100*1024*1024
    chunkrobin = 0
    filetable={}
    chunktable={}
    #For now only one chunk server
    chunkservers={}     
    #-----------------------------------------
    #initilize the master server,set some parameters
    # def __init__(self):
    #     #
    #     self.num_chunkservers = 1
    #     self.max_chunkservers = 1
    #     self.max_chunksperfile = 1
    #     #
    #     self.chunksize = 100*1024*1024
    #     self.chunkrobin = 0
    #     self.filetable={}
    #     self.chunktable={}
    #     #For now only one chunk server
    #     self.chunkservers={}
    #     self.init_chunkserver()
    #---------------------------------------------
    con_chunk = rpyc.connect('localhost', port=8888)
    chunkservers[0] = con_chunk.root
    #------------------------------------------
    # def init_chunkserver(self):
    #     # for i in range(self.num_chunkservers):
    #     #     ##这里我觉得需要连接一下这个chunkserver
    #     #     # chunkserver = chunk_service(i)
    #     #     # self.chunkservers[i] = chunkserver

    #     #     ##这里我不知道该怎么弄了

    #     #     self.chunkservers = chunk_service
    #     con_chunk = rpyc.connect('localhost', port=8888)
    #     self.chunkservers[0] = con_chunk.root
    #------------------------------------------------
    def exposed_get_chunkservers(self):
        return self.chunkservers

    #Allocate a new file with filename and number of chunks
    def exposed_alloc_file(self, fname, num_chunks): # return ordered chunkuuid list
        chunkuuids = self.alloc_chunks(num_chunks)
        self.filetable[fname] = chunkuuids
        return chunkuuids


    #allocate chunks
    def alloc_chunks(self, num_chunks):
        chunkuuids = []
        for i in range(0, num_chunks):
            chunkuuid = uuid.uuid1()
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = chunkloc
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return chunkuuids

    #append some chunks to a file
    def exposed_append(self, fname, num_append_chunks): # append chunks
        chunkuuids = self.filetable[fname]
        append_chunkuuids = self.alloc_chunks(num_append_chunks)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def exposed_get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def exposed_get_chunkuuids(self, fname):
        return self.filetable[fname]

    def exposed_exists(self, fname):
        return fname in self.filetable
    #delete the file and rename it for garbge collection
    def exposed_delete(self, fname): 
        chunkuuids = self.filetable[fname]
        del self.filetable[fname]
        # #garbge collection
        # timestamp = repr(time.time())
        # deleted_filename = "/hidden/deleted/" + timestamp + fname
        # self.filetable[deleted_filename] = chunkuuids
        # print ("deleted file: " + fname + " renamed to " + \
        #      deleted_filename + " ready for gc")

    # def dump_metadata(self):
    #     print ("Filetable:")
    #     for filename, chunkuuids in self.filetable.items():
    #         print (filename, "with", len(chunkuuids),"chunks")
    #     print ("Chunkservers: ", len(self.chunkservers))
    #     print ("Chunkserver Data:")
    #     for chunkuuid, chunkloc in sorted(self.chunktable.iteritems(), key=operator.itemgetter(1)):
    #         chunk = self.chunkservers[chunkloc].read(chunkuuid)
    #         print (chunkloc, chunkuuid, chunk)

if __name__ == "__main__":
    t = ThreadedServer(master_service, port=18861)
    t.start()

#C:/Users/51934/Anaconda3/envs/tensorflow/python.exe c:/Users/51934/Desktop/Python/OST/master.py