import wbsoc
import Save
import gen_token
import multiprocessing
import time

def accesstoken(file_loc,data_type):
    return gen_token.AutoLogin(file_loc=file_loc,data_type=data_type).get_access_token()

def connect(access_token,stonks,wait_time,save_format=Save.csv):
    

    dep = multiprocessing.Process(target = wbsoc.collect, args=(wbsoc.Depth,access_token,stonks,wait_time,save_format))
    sym = multiprocessing.Process(target = wbsoc.collect, args=(wbsoc.Symbol,access_token,stonks,wait_time,save_format))
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