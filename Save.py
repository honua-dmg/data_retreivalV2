import datetime as dt
import os
import h5py as h


# each format class MUST have 2 overarching functions : initialise and save_files

class Format():
    #abstract class
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        self.stonks = stonks
        self.dir = directory
        self.keys = keys
        self.data_type=data_type
        self.india_date=date
    def initialise(self):
        #abstract 
        pass

    def save_files(self):
        #abstract
        pass


class csv(Format):
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        super().__init__(stonks=stonks,keys=keys,directory=directory,data_type=data_type,date=date)

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


class mdf5(Format):
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        super().__init__(stonks=stonks,keys=keys,directory=directory,data_type=data_type,date=date)

    def append(data,dataset):
        # ensure the data is properly formatted
        dataset.reshape((dataset.shape[0]+1,dataset.shape[1]))
        try:
            dataset[-1] = data
        except TypeError:
            return f"data doesn't match datset shape:{dataset.shape}"
        
    def initialise(self):
        file_name = self.directory + '-'.join(self.india_date.split('-')[:2])
        
        
        with h.File(file_name+'.h5','a') as f:
            #initialsing master file
            try:
                self.append(f['master'],self.india_date.split('-')[2])
            except Exception: # datset not made yet.
                f.create_dataset('master',shape=(0,1),maxshape=(None,1))
                self.append(f['master'],self.india_date.split('-')[2])

            #making stock files:
            for stonk in self.stonks:
                exchange,name= stonk.split(':')
                ticker = name.split('-')[0]
                try:
                    f[f'{ticker}/{f["master"].shape[0]}/{exchange}/{self.data_type}']
                    #f[f'{ticker}/{f["master"].shape[0]}/{exchange}/Symbol']
                except Exception:
                    f.create_dataset(f'{ticker}/{f["master"].shape[0]}/{exchange}/Depth',shape=(0,len(self.keys)+1),maxshape=(None,len(self.keys)+1))
                    #f.create_dataset(f'{ticker}/{f["master"].shape[0]}/{exchange}/Symbol',shape=(0,20),maxshape=(None,20))

    def save_files(self,message):
        india_epoch = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)).timestamp()
        file_name = self.directory + '-'.join(self.india_date.split('-')[:2])
        if 'symbol' not in message.keys(): # a message without data
            return
        del message['type'] # we don't need it
        exchange,name= message.pop('symbol').split(':')
        ticker = name.split('-')[0]
        with h.File(file_name+'.h5','a') as f:
            data = [message[key] for key in message].append(india_epoch)
            self.append(data,f[f'{ticker}/{f["master"].shape[0]}/{exchange}/{self.data_type}'])
