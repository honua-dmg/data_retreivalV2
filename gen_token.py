"""
1. get creds
2. get auth_code response url
3. login and get auth code from url
4. generate access token from auth_code
"""
from fyers_apiv3 import fyersModel
import seleniumbase as sb
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pyotp

def get_creds(file_loc,data_type):
    """ 
    returns credentials stored in the specified file_loc
    args:
        file_loc: location of file
        data_type: (specified in secrets.txt)
    """
    creds = {}
    with open(file_loc,'r') as f:
        txt = f.readlines()
        for i in range(len(txt)):
            if data_type in txt[i]:
                creds['TOTPseckey'] = txt[i+1].split('=')[1].strip("\n,")
                creds['key'] = txt[i+2].split('=')[1].strip("\n,")
                creds['phoneno'] = txt[i+3].split('=')[1].strip("\n,")
                creds['client_id'] = txt[i+4].split('=')[1].strip("\n,")
                creds['secret_key'] = txt[i+5].split('=')[1].strip("\n,")
                creds['redirect_uri'] = txt[i+6].split('=')[1].strip("\n,")  
    return creds

# question to ponder: is making a separate class for this really necessary?
class Login():
    """
    for manual intervention, you can use the _generate_response_url and get the authcode manually,set the variable to it and run get_access_token
    """

    def __init__(self,client_id,secret_key, redirect_uri,key=None,phoneno=None,TOTPseckey=None):
        self.client_id = client_id
        self.secret_key = secret_key
        self.redirect_uri = redirect_uri
        self.four_digit_key = key
        self.phoneno = phoneno
        self.TOTPseckey = TOTPseckey
        self.response_type = "code"  
        self.state = "sample_state"
        self.grant_type = "authorization_code"
        self.auth_code = None
        self.responseurl = None
        self.access_token = ''

    def _generate_response_url(self):
        """ gets auth_code"""
        self.session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type=self.response_type,
            grant_type=self.grant_type
        )
       
        # Generate the auth code using the session model
        self.responseurl = self.session.generate_authcode()
        return self.responseurl
    
    def _login_and_get_auth(self,response,driver_mode=0):
        """automatically gets auth_code from response url"""


        if self.TOTPseckey == None:
            return KeyError('TOTPseckey not provided')
        if self.four_digit_key == None:
            return KeyError('four digit key not provided')
        if self.phoneno == None:
            return KeyError('phoneno not provided')
        try:
            if driver_mode == 0:
                drive = sb.Driver(uc=True)
            elif driver_mode == 1:
                drive = sb.Driver(undetectable=True)
            drive.get(response)
            time.sleep(2)
            #clicking on phone number box
            phno = drive.find_element(By.XPATH,'/html/body/section[1]/div[3]/div[3]/form/div[1]/div/input')
            phno.click()
            #sending phone number details
            phno.send_keys(self.phoneno)

            #clicking on continue
            drive.find_element(By.XPATH,'/html/body/section[1]/div[3]/div[3]/form/button').click()
            time.sleep(3)
            #sending TOTP 
            otp = pyotp.TOTP(self.TOTPseckey).now()
            for i in range(1,7):
                drive.find_element(By.XPATH,f'/html/body/section[6]/div[3]/div[3]/form/div[3]/input[{i}]').send_keys(otp[i-1])

            #pressing continue
            drive.find_element(By.XPATH,'/html/body/section[6]/div[3]/div[3]/form/button').click()
            time.sleep(3)
            #sending id
            for i in range(1,5):
                drive.find_element(By.XPATH,f'/html/body/section[8]/div[3]/div[3]/form/div[2]/input[{i}]').send_keys(self.four_digit_key[i-1])
            drive.find_element(By.XPATH,'/html/body/section[8]/div[3]/div[3]/form/button').click()
            try:
                # terms and conditions validation 
                drive.find_element(By.XPATH,'/html/body/div/div/div/div/div/div[3]/div/div[3]/label').click()
                drive.find_element(By.XPATH,'/html/body/div/div/div/div/div/div[4]/div/a[2]/span').click()
            except Exception as e:
                print('no neeed to validate :)')
            time.sleep(2)
            url = drive.current_url
            return url.split('&')[2].split('=')[1]
        finally:
            drive.quit()

    def get_access_token(self):
        # generating response url if not done manually
        if not self.responseurl:
            response = self._generate_response_url()
        
        #getting auth_code if we did not do it manually
        if not self.auth_code: 
            try:
                self.auth_code= self._login_and_get_auth(response)               # trying with uc mode in seleniumbase
                print('auth_code obtained!')
            except Exception:
                print('shucks login issue occured, we prolly got detected')
                try:
                    self.auth_code= self._login_and_get_auth(response,driver_mode=1)     # trying with undected mode in seleniumbase
                    print('auth_code obtained!')
                except Exception:
                    print('auth code failed to be received')
                    return NotImplementedError('bro SeleniumBase might be failing you look for alternatives')

        # getting access token
        """outputs access token """
        # Set the authorization code in the session object
        self.session.set_token(self.auth_code)
        # Generate the access token using the authorization code
        response = self.session.generate_token()
        print(response['code'])
        if response['s']=='ok':
            self.access_token = response['access_token']
            print('*'*80)
            print('success')
            return self.access_token
        else:
            return NotImplementedError('bro you fucked up somewhere'+str(response['code']))


# autologin by saving data in secrets.txt file
class AutoLogin(Login):
    def __init__(self,file_loc,data_type):
        creds = get_creds(file_loc=file_loc,data_type=data_type)
        super().__init__(client_id=creds['client_id'],
                         secret_key=creds['secret_key'],
                         redirect_uri=creds['redirect_uri'],
                         key=creds['key'],
                         phoneno=creds['phoneno'],
                         TOTPseckey=creds['TOTPseckey'])

