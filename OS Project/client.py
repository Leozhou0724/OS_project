import rpyc
import sys
import os
path = './files/client/'

# Three Basic Functions:
# Download
# Upload
# Delete
def download(master, fname):
    file_list = master.get_file_list(fname)
    if fname in file_list:
        data = None
        ######################################
        # 这里是不是也要连接chunkserver
        # local_fname = self.chunk_filename
        with open(path + str(fname), "rb") as f:
            data = f.read()
            return data
    else:
        print('File does not exist.')
        return


def upload(chunk, fname):
    con = rpyc.connect('localhost', port=8888)

    chunk_sever = con.root##这些地方之后要改成连chunk server
    
    with open(path + str(fname), 'rb') as f:
        # print(f)
        data = f.read()
        chunk_sever.put(data,fname)

def delete(fname):

    return 0
########################################################
#199
########################################################
def __init__(self, master):
        self.master = master

def write(self, filename, data): # filename is full namespace path
        if self.exists(filename): # if already exists, overwrite
            self.delete(filename)
        num_chunks = self.num_chunks(len(data))
        chunkuuids = self.master.alloc(filename, num_chunks)
        self.write_chunks(chunkuuids, data)

def write_chunks(self, chunkuuids, data):
        chunks = [ data[x:x+self.master.chunksize] \
            for x in range(0, len(data), self.master.chunksize) ]
        chunkservers = self.master.get_chunkservers()
        for i in range(0, len(chunkuuids)): # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

def num_chunks(self, size):
        # return (size // self.master.chunksize) \
        #     + (1 if size % self.master.chunksize > 0 else 0)
        ##这里我们只有一个chunk，直接return1
        return 1

def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " \
                 + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename, \
            num_append_chunks)
        self.write_chunks(append_chunkuuids, data)

def exists(self, filename):
        return self.master.exists(filename)

def read(self, filename): # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " \
                + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        #他这个代码需要多个chunk server，我们暂时只有一个
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)
        ##我们这里暂时只有一个chunk所以就可以直接用
        data = chunk
        #data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data

def delete(self, filename):
        self.master.delete(filename)
upload(0, 'data.txt')
