import datetime as dt
import os
import h5py as h
import numpy as np

# each format class MUST have 2 overarching functions : initialise and save_files

class Format():
    #abstract class
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        # all of these come from the Depth or Symbol classes in wbsoc
        self.stonks = stonks
        self.dir = directory
        self.keys = keys # all the data types we get from the websocket
        self.data_type=data_type # Depth or Symbol
        self.india_date=date
        self.f = None
    
    def initialise(self):
        #abstract 
        # creates files if not already made 
        pass

    def open_file(self):
        pass

    def close_file(self):
        pass

    def save_files(self):
        #abstract
        # a method to save incoming data to our files
        pass


class csv(Format):
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        super().__init__(stonks=stonks,keys=keys,directory=directory,data_type=data_type,date=date)

    def _initcols(self,file_path):
        """
        args:
            file_path: location of csv file
        initialises columns within newly made csv files 
        
        """
        with open(file_path,'a+') as f:
            if os.path.getsize(file_path) != 0:  #if file is already made, no need to initialse it again
                return
           #creates columns in csv files
            for key in self.keys:
                f.write(key+',')
            f.write('time')
            f.write('\n')

    # *********************************************************************************************

    def initialise(self):
        """
        create individual csv files for each stock for said day
        """
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
        """
        args:
            message: data received from fyers
        
        parses message data and saves it to the right csv file
        """
        india_epoch = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)).timestamp()
        if 'symbol' not in message.keys(): # a message without data
            return
       
        del message['type'] # we don't need it
        file_symbol = ''.join(['-' if x == ":" else x for x in message.pop('symbol')])
        file_path = f'{self.dir}/{file_symbol}/{self.data_type[:4]}-{self.india_date}.csv'

        # appending data to our file as a line
        with open(file_path,'a+') as f:
            for key in message:
                f.write(str(message[key])+',')
            f.write(str(india_epoch))
            f.write('\n') # to go to a new line to save the next record 


class hdf5(Format):
    def __init__(self,stonks,keys,directory,data_type,date) -> None:
        super().__init__(stonks=stonks,keys=keys,directory=directory,data_type=data_type,date=date)
        self.file_name =  "{directory}/{date}-{datatype}.h5".format(directory = self.dir,
                                                                    date = '-'.join(self.india_date.split('-')[:2]),
                                                                    datatype=self.data_type.split('U')[0])
        
        
    def append(self,data,dataset):
        """
        args:
            data: parsed message data received from websocket
            dataset: location of dataset within the hdf5 file to save the data
        returns:
            error message in case of a type error
        appends time and message data receieved from fyers to the correct hdf5 dataset 
        within the month file.
        """
        # ensure the data is properly formatted
        dataset.resize((dataset.shape[0]+1,dataset.shape[1]))
        try:
            dataset[-1] = data
        except TypeError:
            return f"data doesn't match datset shape:{dataset.shape}"


    def open_file(self):
        """
        set to open file to enable data writing and reading simultaneously (swmr mode)
        """
        self.f = h.File(self.file_name,'a',libver='latest')
        try:
            self.f.swmr_mode=True
        except Exception:
            pass

    def close_file(self):
        """
        set to close the hdf5 file
        """
        self.f.close()
    

    def initialise(self):
        """

        creates 2 files if not already made for the given month.
        Depth and Symbol
        the dataset location inside said file is TICKER/Day-id/MARKET
        
        """
        # file format ex: YYYY-MM-Depth.h5
        # dataset locations:
        # SBIN/1/NSE
        # the 1 refers to the master dataset's corrilated date (assumed it'd make it easier to sort through (lcoated in f['master']) 

        files_made = False
        with h.File(self.file_name,'a',libver='latest') as f:
            #initialsing master file
            try:
                master = f['master']
            except KeyError:
                master = f.create_dataset('master',shape=(0,1),maxshape=(None,1))
                files_made=True
                #f.swmr_mode=True

            if master.shape[0] == 0:
                self.append(self.india_date.split('-')[2],f['master'])
            
            if  f['master'][-1] != float(self.india_date.split('-')[2]):
                self.append(self.india_date.split('-')[2],f['master'])

            #making stock files:
            for stonk in self.stonks:
                exchange,name= stonk.split(':')
                ticker = name.split('-')[0]
                dataset_loc = f'{ticker}/{f["master"].shape[0]}/{exchange}'
                try:
                    f[dataset_loc]
                    #f[f'{ticker}/{f["master"].shape[0]}/{exchange}/Symbol']
                except Exception:
                    files_made=True
                    dataset = f.create_dataset(dataset_loc,
                                               shape=(0,len(self.keys)+1),
                                               maxshape=(None,len(self.keys)+1),
                                               compression='lzf',# to make the files as small as possible 
                                               )#dtype=np.float64) 
                        #    SBIN   /    Day-id            / NSE      / Depth
                    
                    print(dataset_loc)
                    dataset.attrs['columns'] = self.keys + ['time']
                    #f.create_dataset(f'{ticker}/{f["master"].shape[0]}/{exchange}/Symbol',shape=(0,20),maxshape=(None,20))
            if files_made:
                f.swmr_mode=True
    
    def save_files(self,message):
        """
        args:
            message: the data received from fyers.

        returns:
            none

        Description

        uses append function to parse fyers message and 
        save said data to the respective database via the append function.
        """
        india_epoch = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)).timestamp()
        
        if 'symbol' not in message.keys(): # a message without data
            return
        del message['type'] # we don't need it
        exchange,name= message.pop('symbol').split(':')
        ticker = name.split('-')[0]
        data =[message[key] for key in message]+ [india_epoch]
        #print(data)
        self.append(data,self.f[f'{ticker}/{self.f["master"].shape[0]}/{exchange}'])
        self.f[f'{ticker}/{self.f["master"].shape[0]}/{exchange}'].flush()
        print('saved to:'+f'{ticker}/{self.f["master"].shape[0]}/{exchange}')
