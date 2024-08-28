import wbsoc
import Save
import gen_token
import multiprocessing
import time
 
def get_stonks(file):
    stonks_list = []
    with open(file,'r') as f:
        line = f.readline()
        while line != 'stonks:\n':
            line = f.readline()
            pass
        line = f.readline()
        while line != '':
            stonks_list.extend(line.split(',')[:2])
            #print(line.split(',')[:2])
            line=f.readline()
    return stonks_list

def accesstoken(file_loc,data_type):
    return gen_token.AutoLogin(file_loc=file_loc,data_type=data_type).get_access_token()

# runs Depth and Symbol Data websockets simultaneously via multiprocessing for a set amount of time. 
def connect(access_token,stonks,wait_time,save_format=Save.csv,dir=r'/Users/gurusai/programming/STONKS/data_retreival_v2/data'):
    

    dep = multiprocessing.Process(target = wbsoc.collect, args=(wbsoc.Depth,access_token,stonks,wait_time,save_format,dir))
    sym = multiprocessing.Process(target = wbsoc.collect, args=(wbsoc.Symbol,access_token,stonks,wait_time,save_format,dir))
    print('processes initiated!')
    print('\n\n\n\n\n\n')
    dep.start()
    sym.start()
    time.sleep(wait_time+5)
    dep.terminate()
    sym.terminate()
    print('processes closed!')
    print('\n\n\n\n\n\n')
    dep.join()
    sym.join()

# an all encompassing function that deals with token collection, websocket initiation and saving data
def collect(file_loc:str,app_name:str,wait_time:int,save_format=Save.csv,dir=r'/Users/gurusai/programming/STONKS/data_retreival_v2/data'):
    access_token = accesstoken(file_loc,app_name)
    stonks = get_stonks('secrets.txt')
    connect(access_token,stonks,wait_time,save_format=save_format)