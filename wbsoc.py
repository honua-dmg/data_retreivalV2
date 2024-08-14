from fyers_apiv3.FyersWebsocket import data_ws
import time 
import datetime as dt

import Save


class _Data():
    def __init__(self,access_token:str,stonks:list,save_mode=Save.csv,directory = None,):
        self.access_token = access_token
        self.stonks = stonks # list of stonks in "NSE:SBIN-EQ" this format
        self._initialised = False
        self._connected = False
        self._litemode = False
        self.data_type = None # defined in subclasses
        self.keys = None
        if directory == None:
            self.dir = r'/Users/gurusai/data'
        else:
            self.dir = directory

        #datetime in YYYY-MM-DD format
        self.india_date = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        




    def onmessage(self,message):
        print("Response:", message)
        if self._connected:
            self.save_mode.save_files(message)

    def onerror(self,message):
        print("Error:", message)
       
    def onclose(self,message):
        print("Connection closed:", message)

    def onopen(self):
        print('connection opened')
   
    def connect(self):
        self.fyers  = data_ws.FyersDataSocket(
        access_token=self.access_token,       # Access token in the format "appid:accesstoken"
        log_path='',                     # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=self._litemode,                  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,              # Save response in a log file instead of printing it.
        reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=self.onopen,               # Callback function to subscribe to data upon connection.
        on_close=self.onclose,                # Callback function to handle WebSocket connection close events.
        on_error=self.onerror,                # Callback function to handle WebSocket errors.
        on_message=self.onmessage,            # Callback function to handle incoming messages from the WebSocket.
        reconnect_retry=10               # Number of times reconnection will be attepmted in case
        )
        self.fyers.connect()
        self._initialised = True

    def subscribe(self):
        #data type: DepthUpdate, SymbolUpdate
        if self._initialised:
            self.fyers.subscribe(symbols=self.stonks,data_type=self.data_type)
            self.fyers.keep_running()
            self._connected = True
        else:
            print(f'initialise websocket via .connect()')

    def unsubscribe(self):
        if self._connected:
            self.fyers.unsubscribe(symbols=self.stonks, data_type=self.data_type)
            self._connected=False # not having this seems to cause some bugs (i.e it wont unsubscribe)
    

class Depth(_Data):
    # a class to collect depth data from fyers 
    def __init__(self,access_token,stonks,directory=None,save_mode = Save.csv):
        super().__init__(access_token=access_token,stonks=stonks,directory=directory,save_mode=save_mode)
        self.keys = ['bid_price1','bid_price2','bid_price3','bid_price4','bid_price5',
                    'bid_size1','bid_size2','bid_size3','bid_size4','bid_size5',
                    'bid_order1','bid_order2','bid_order3','bid_order4','bid_order5',
                    'ask_price1','ask_price2','ask_price3','ask_price4','ask_price5',
                    'ask_size1','ask_size2','ask_size3','ask_size4','ask_size5',
                    'ask_order1','ask_order2','ask_order3','ask_order4','ask_order5']
        self.data_type = 'DepthUpdate'
        self.save_mode = save_mode(stonks=self.stonks,keys=self.keys,directory=self.dir,data_type=self.data_type,date=self.india_date)
        self.save_mode.initialise()
        

class Symbol(_Data):
    # a class to collect symbol data from fyers
    def __init__(self,access_token,stonks,directory=None,litemode=False,save_mode=Save.csv):
        super().__init__(access_token=access_token,stonks=stonks,directory=directory,save_mode=Save.csv)
        self.keys =   ['ltp', 'vol_traded_today', 'last_traded_time', 'exch_feed_time', 'bid_size', 'ask_size',
                        'bid_price', 'ask_price', 'last_traded_qty', 'tot_buy_qty', 'tot_sell_qty', 'avg_trade_price',
                        'low_price','high_price', 'lower_ckt', 'upper_ckt', 'open_price', 'prev_close_price', 'ch', 'chp']
        self.data_type = 'SymbolUpdate'
        self._litemode=litemode
        self.save_mode = save_mode(stonks=self.stonks,keys=self.keys,directory=self.dir,data_type=self.data_type,date=self.india_date)
        self.save_mode.initialise()

# used while multiprocessing to run both depth and symbol websockets simultaneously
def collect(Data_class,auth:str,stonks:list,wait_time:int,save_mode=Save.csv,dir=r'/Users/gurusai/data'):
    symbol = Data_class(auth,stonks,save_mode=save_mode,directory=dir)
    symbol.connect()
    symbol.subscribe()
    time.sleep(wait_time)
    symbol.unsubscribe()