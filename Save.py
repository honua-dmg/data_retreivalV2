import datetime as dt
import os


# each format class MUST have 2 overarching functions : initialise and save_files

class csv():
    def __init__(self,stonks,keys,directory) -> None:
        self.stonks = stonks
        self.directory = directory
        self.keys = keys

    def _initcols(self,file_path):

        with open(file_path,'a+') as f:
            if os.path.getsize(file_path) != 0:  #if file is already made, no need to initialse it again
                return
           
            for key in self.keys:
                f.write(key+',')
            f.write('time')
            f.write('\n')

    # *********************************************************************************************

    def initialise(self):
        for stonk in self.stonks:
            #check if directories exist
            file_symbol = ''.join(['-' if x == ":" else x for x in stonk])
            if not os.path.exists(f'{self.dir}/{file_symbol}'): #checking to see if file path exists
                try:                                            # we had to include this try block again due to issues in multiprocessing
                    os.mkdir(f'{self.dir}/{file_symbol}')           #we will make the file path if it doesnt :)
                except Exception:                   
                    print('file already exists')
            #check if file with type and datestamp is initialised
            file_path = f'{self.dir}/{file_symbol}/{self.data_type[:4]}-{self.india_date}.csv'
            self._initcols(file_path=file_path)


    def save_files(self,message):
        india_epoch = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)).timestamp()
        if 'symbol' not in message.keys(): # a message without data
            return
       
        del message['type'] # we don't need it
        file_symbol = ''.join(['-' if x == ":" else x for x in message.pop('symbol')])
        file_path = f'{self.dir}/{file_symbol}/{self.data_type[:4]}-{self.india_date}.csv'


        with open(file_path,'a+') as f:
            for key in message:
                f.write(str(message[key])+',')
            f.write(str(india_epoch))
            f.write('\n')