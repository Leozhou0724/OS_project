import rpyc
import sys
import os
path = './client/'

# Three Basic Functions:
# Download
# Upload
# Delete

class client :
    client_root = os.path.expanduser("~")
    client_root += "./gfs_root/client/"
    if  not os.access(client_root, os.W_OK):
        os.makedirs(client_root)
        print("create client root")
    con_master = rpyc.connect('localhost', port=18861)
    master_sever = con_master.root
    master = master_sever

    chunkservers = {}
    con_chunk = rpyc.connect('localhost', port=8888)
    chunkservers[0] = con_chunk.root
    
    def __init__(self):
        #connect to the master
        return 

    def write(self, filename, data): 
        if self.exists(filename): 
            self.delete(filename)
            print('file already exists')
        else:
            print('this is a new data')
        with open(path + str(data), 'rb') as f:
            data = f.read()
            # num_chunks = self.num_chunks(len(data))
            num_chunks =1
            #---
            print("num_chunks",num_chunks)
            #---
            chunkuuids = self.master.alloc_file(filename, num_chunks)
            print("chunk_id ",chunkuuids)
            
            
            self.write_chunks(chunkuuids, data)
        
        

    def write_chunks(self, chunkuuids, data):

        chunks={}
        chunks[0]=data
        chunkservers = self.chunkservers
        print("num_chunkuids",len(chunkuuids))
        for i in range(0, len(chunkuuids)): # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            print("chunk_location",chunkloc)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

    

    def num_chunks(self, size):

        return 1

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename): # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " \
                + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)

        chunkservers = self.chunkservers
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)

        data = chunk

        with open('./' + str(filename), 'wb') as f:
            f.write(data)
    def delete(self, filename):
        self.master.delete(filename)
    def show_file(self):
        filelist = self.master.filelist
        


client1 = client()
print("client root is ",client1.client_root)
print("download root is local root")
print("chunk root is C:/Users/51934/gfs_root/chunks/0")

client1.write('pic.jpg','pic.jpg')
client1.master.dump_metadata()
client1.read('pic.jpg')