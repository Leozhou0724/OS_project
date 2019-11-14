import rpyc
import sys
import os
path = './files/client/'

# Three Basic Functions:
# Download
# Upload
# Delete


class client:
    def __init__(self):
        # connect to the master
        con_master = rpyc.connect('localhost', port=18861)
        master_sever = con_master.root
        self.master = master_sever

    def write(self, filename, data):
        if self.exists(filename):
            self.delete(filename)
        num_chunks = self.num_chunks(len(data))
        # ---
        print("num_chunks", num_chunks)
        # ---
        chunkuuids = self.master.alloc_file(filename, num_chunks)
        self.write_chunks(chunkuuids, data)
        print("test")

    def write_chunks(self, chunkuuids, data):
        # ----------------------------
        # 这里要根据chunksize 来把文件划分成很多个chunk，但是我在测试的时候发现不能直接
        # 调用master里的chunksize 这个参数，感觉后面可能要改
        # chunks = [ data[x:x+self.master.chunksize] \
        #     for x in range(0, len(data), self.master.chunksize) ]
        # ------------------------------
        chunks = {}
        chunks[0] = data
        con_chunk = rpyc.connect('localhost', port=8888)
        chunkserver = con_chunk.root
        chunkserver = self.master.get_chunkservers()
        print("num_chunkuids", len(chunkuuids))
        for i in range(0, len(chunkuuids)):  # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunkserver.write(chunkuuid, chunks[i])

    def num_chunks(self, size):
        # return (size // self.master.chunksize) \
        #     + (1 if size % self.master.chunksize > 0 else 0)
        # 这里我们只有一个chunk，直接return1
        return 1

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: "
                            + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename,
                                                     num_append_chunks)
        self.write_chunks(append_chunkuuids, data)

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename):  # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: "
                            + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        # 他这个代码需要多个chunk server，我们暂时只有一个
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)
        # 我们这里暂时只有一个chunk所以就可以直接用
        data = chunk
        # data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data

    def delete(self, filename):
        self.master.delete(filename)


client1 = client()
client1.write("file1", "1.txt")
