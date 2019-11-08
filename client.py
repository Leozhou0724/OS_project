import rpyc
import sys
import os

path = './files/client/'


def download(master, file_name):
    #file_list = master.get_file_list(file_name)
    if file_name == None:
        print('File does not exist.')
        return


def upload(chunk, data):
    con = rpyc.connect('localhost', port=8888)
    chunk_sever = con.root
    with open(path + str(data), 'r') as f:
        # print(f)
        data = f.read()
        chunk_sever.put(data)


upload(0, 'data.txt')
