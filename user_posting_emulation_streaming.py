import requests
import json
import random
import sqlalchemy
from sqlalchemy import *
from time import sleep

random.seed(100)

class AWSDBConnector:
    '''
    Creates a connection with the AWS RDS database from which data will be obtained
    
    Attributes
    ----------
    HOST: str
        AWS RDS host that will be accessed
    USER: str
        Username that the database is under
    PASSWORD: str
        Password required to access the database
    DATABASE: str
        Name of the database
    PORT: int
        Default port number for the database engine used
    
    Methods
    -------
    create_db_connector(): 
        creates a database engine that MySQL will use to create, read, update and delete (CRUD) date from the database
    '''
    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
    
    def create_db_connector(self):
        '''
        Parameters
        ----------
        None
        
        Returns
        -------
        engine: sqlalchemy engine
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
    
class UserPostingEmulation():
    '''
    A class that represents a user posting emulation

    Attributes
    ----------
    pin_result: json
        pin data from a random row in the pinterest_data table
    geo_result: json
        geolocation data from a random row in the pinterest_data table
    user_result: json
        user data from a random row in the pinterest_data table
    
    Methods
    -------
    post_pinterest_data(connection, random_row, record_type):
        gets data from a random row in the pinterest_data table corresponding to pinterest, geolocation and user data
    '''
    def __init__(self, connection, random_row):
        '''
        Constructs the attributes for the UserPostingEmulation method
        
        Parameters
        ----------
        connection:
            connects the pinterest data table to the API server the user posting emulation is running on
        random_row: int
        a number from 0 to 11000, corresponding to a row in the pinterest data table
        '''
        self.pin_result = self.post_pinterest_data(connection, random_row, "pinterest")
        self.geo_result = self.post_pinterest_data(connection, random_row, "geolocation")
        self.user_result = self.post_pinterest_data(connection, random_row, "user")
    
    def post_pinterest_data(self, connection, random_row, record_type):
        '''
        Gets data from a random row in the pinterest data table, corresponding to pinterest, geolocation and user data
        
        Parameters
        ----------
        record_type: str

        Returns
        -------
        data_result: dict
            data from a random row in the pinterest data table
        '''
        data_string = text(f"SELECT * FROM {record_type}_data LIMIT {random_row}, 1")
        data_selected_row = connection.execute(data_string)

        for row in data_selected_row:
            data_result = dict(row._mapping)
        return data_result

class PostData():
    '''
    A class that posts data to the AWS Kinesis streams
    
    Attributes
    ----------
    headers: dict
        The HTTP headers necessary for the API to process the streaming data request
    
    Methods
    -------
    __create_streaming_payload(data_structure, record_type):
        creates the payload for the streaming data to be sent to AWS Kinesis
    
    send_stream_request(data_structure, record_type):
        sends a PUT request to the API for the data to be sent to the Kinesis stream
    
    post_streaming_data(pin_data_structure, geo_data_structure, user_data_structure):
        sends streaming data requests to the API in order to be sent to the corresponding Kinesis stream
    '''  
    def __init__(self):

        self.headers = {'Content-Type': 'application/json'}
    
    def __create_streaming_payload(self, data_structure, record_type):
        '''
        Parameters
        ----------
        data_structure: dict
            the structure of the data result given by the user posting emulation
            
        Returns
        -------
        payload: json
            configures the data into the json format, so that it's ready to send to the Kinesis stream
        '''    
        payload = json.dumps({
            "StreamName": f"streaming-0e4c2ab6fb3b-{record_type}",
            "Data": data_structure,
                "PartitionKey": f"{record_type}-partition"
                })
        return payload
    
    def send_stream_request(self, data_structure, record_type):
        '''
        Parameters
        ----------
        data_structure: dict
            the structure of the data result given by the user posting emulation 
        
        record_type: str
            the type of record being added to the dataframe: "pin", "geo", "user"
        '''
        invoke_url = f"https://w65ht1cpna.execute-api.us-east-1.amazonaws.com/REST/streams/streaming-0e4c2ab6fb3b-{record_type}/record"
        payload = self.__create_streaming_payload(data_structure, record_type)
        response = requests.request("PUT", invoke_url, headers = self.headers, data = payload)
        print(f"{record_type}:{response.status_code}")
    
    def post_streaming_data(self, pin_data_structure, geo_data_structure, user_data_structure):
        '''
        Sends stream data requests to the API, which then sends them to their respective Kinesis streams
        
        Parameters
        ----------
        pin_data_structure: dict
            structure of the pin_result provided by the user posting emulation
        
        geo_data_structure: dict
            structure of the geo_result provided by the user posting emulation
        
        geo_data_structure: dict
            structure of the geo_result provided by the user posting emulation
        '''
        self.send_stream_request(pin_data_structure, "pin")
        self.send_stream_request(geo_data_structure, "geo")
        self.send_stream_request(user_data_structure, "user")

def run_infinite_post_data_loop():
    '''
    Runs a loop which posts data retrieved from the pinterest data table
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None
    '''
    new_connector = AWSDBConnector() 
    pd = PostData()
    while True:
        sleep(random.randrange(0, 2))
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            random_row = random.randint(0, 11000)
            upe = UserPostingEmulation(connection, random_row)

            pin_data_structure = {
                        #Data should be sent as pairs of column_name:value, with different columns separated by commas      
                        "index": upe.pin_result["index"], "unique_id": upe.pin_result["unique_id"], "title": upe.pin_result["title"], 
                        "description": upe.pin_result["description"], "poster_name": upe.pin_result["poster_name"], 
                        "follower_count": upe.pin_result["follower_count"], "tag_list": upe.pin_result["tag_list"], 
                        "is_image_or_video": upe.pin_result["is_image_or_video"], "image_src": upe.pin_result["image_src"], 
                        "downloaded": upe.pin_result["downloaded"], "save_location": upe.pin_result["save_location"], 
                        "category": upe.pin_result["category"]} 
            
            geo_data_structure = {
                        #Data should be sent as pairs of column_name:value, with different columns separated by commas      
                        "ind": upe.geo_result["ind"], "timestamp": upe.geo_result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"), 
                        "latitude": upe.geo_result["latitude"], "longitude":upe.geo_result["longitude"], "country": upe.geo_result["country"]}
            
            user_data_structure = {
                        #Data should be sent as pairs of column_name:value, with different columns separated by commas      
                        "ind": upe.user_result["ind"], "first_name": upe.user_result["first_name"], "last_name": upe.user_result["last_name"], 
                        "age": upe.user_result["age"], "date_joined": upe.user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")
                        }

            pd.post_streaming_data(pin_data_structure, geo_data_structure, user_data_structure)
            
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print("Working")
