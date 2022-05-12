 #ExampleOPC Client
 #Connect to UAGateway on remote computer
 #Get data from OPC-DA
#based on https://github.com/FreeOpcUa/python-opcua/blob/master/examples/client-minimal.py

import sys
import time
import datetime
import json
import requests
sys.path.insert(0, "..")

from opcua import Client
from functools import wraps

def json_send(data,time_array,unit,meas):
    #Create JSON with correct information
    
    if unit == "Rod mill":
        if meas == "Weight":
            MeasurementUnit = "kg"            
        if meas == "Power":
            MeasurementUnit = "kW"
        if meas == "Water feed":
            MeasurementUnit = "ml/min"
        if meas == "Feed rate":
            MeasurementUnit = "kg/h"
    if unit == "Ball mill":
        if meas == "Weight":
            MeasurementUnit = "kg"
        if meas == "Power":
            MeasurementUnit = "kW"
        if meas == "Water feed":
            MeasurementUnit = "ml/min"
    if unit == "Silo":
        MeasurementUnit = "kg"
    if unit == "Screen":
        MeasurementUnit = "kW"
    
    #JSON message to be sent
    msg = {
        "Organization": "...",
        "Location": "...",
        "Unit": unit,
        "Measurement": meas,
        "MeasurementUnit": MeasurementUnit,
        "MeasurementData": data,
        "Timestamp": time_array
        }   
    
    #HTTP post
    Azure_Address = ''
    headers = {'Content-type': 'application/json','Accept': 'text/plain'}
    #headers = {'Content-type': 'text/plain'}
    response = requests.post(Azure_Address, data = json.dumps(msg), headers = headers)
    print("Sending: {} {}".format(unit, meas))
    print("Status code:", response.status_code)
    print("Post request")    
    print(response.json())  
 
def data_read():
    #Connect to UAGateway on scada computer    
    if __name__ == "__main__":
        client = Client("opc.tcp://192.168.xxx.xxx:48050")
        client.set_security_string("Basic256Sha256,SignAndEncrypt,cert.der,private-key.pem")
        client.secure_channel_timeout = 300000
        client.session_timeout = 10000
        
        try:
            client.connect()

            # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects           
            root = client.get_root_node()
            print("Objects node is: ", root)

            # Node objects have methods to read and write node attributes as well as browse or populate address space
            print("Children of root are: ", root.get_children())
                 
            #get data from process according to predetermined list
            RM_Weight_path = "ns=x;s="tag path"
            BM_Weight_path = "ns=x;s="tag path"
            Silo_Weight_path = "ns=x;s="tag path"
            RM_Power_path = "ns=x;s="tag path"
            BM_Power_path = "ns=x;s="tag path"
            Screen_Power_path = "ns=x;s="tag path"
            RM_FeedRate_path = "ns=x;s="tag path"
            RM_WaterFeed_path = "ns=x;s="tag path"
            BM_WaterFeed_path = "ns=x;s="tag path"

            #Additional information on measurements
            WeightUnit = "kg"
            WaterFlowUnit = "ml/min"
            OreFlowUnit = "kg/h"

            # Now getting a variable nodes using browse paths               
            RM_Weight_var = client.get_node(RM_Weight_path)
            BM_Weight_var = client.get_node(BM_Weight_path)
            Silo_Weight_var = client.get_node(Silo_Weight_path)
            RM_Power_var = client.get_node(RM_Power_path)
            BM_Power_var =client.get_node(BM_Power_path)
            Screen_Power_var = client.get_node(Screen_Power_path)
            RM_FeedRate_var = client.get_node(RM_FeedRate_path)
            RM_WaterFeed_var = client.get_node(RM_WaterFeed_path)
            BM_WaterFeed_var = client.get_node(BM_WaterFeed_path)      
               
           #Define arrays for data values
            RM_Weight = []
            BM_Weight = []
            Silo_Weight = []
            RM_Power = []
            BM_Power = []
            Screen_Power = []
            RM_FeedRate = []
            RM_WaterFeed = []
            BM_WaterFeed = []
            time_array = []
            
            #How often do we get data from process in seconds (note that this uses time.sleep() and execution time varies)
            FetchCycle = 10

            #How many values we collect before sending data to Azure cloud
            SendCycle = 6
            
            #Collect values according to SendCycle
            for x in range(SendCycle):
                            
                #Get datavalues for selected nodes and append to arrays
                RM_Weight.append(RM_Weight_var.get_value())
                BM_Weight.append(BM_Weight_var.get_value())
                Silo_Weight.append(Silo_Weight_var.get_value())
                RM_Power.append(RM_Power_var.get_value())
                BM_Power.append(BM_Power_var.get_value())
                Screen_Power.append(Screen_Power_var.get_value())
                RM_FeedRate.append(RM_FeedRate_var.get_value())
                RM_WaterFeed.append(RM_WaterFeed_var.get_value())
                BM_WaterFeed.append(BM_WaterFeed_var.get_value())
                
                #timestamp in string
                date_time = datetime.datetime.now() #current time in datetime object
                dt_string = date_time.strftime("%Y-%m-%dT%H:%M:%S.%f") #datetime object to string
                time_array.append(dt_string)               
                time.sleep(FetchCycle)
            
            #Create JSON from measurement data and metadata
            
            json_send(RM_Weight,time_array,"Rod mill","Weight")
            json_send(BM_Weight,time_array,"Ball mill","Weight")
            json_send(Silo_Weight,time_array,"Silo","Weight")
            json_send(RM_Power,time_array,"Rod mill","Power")
            json_send(BM_Power,time_array,"Ball mill","Power")
            json_send(Screen_Power,time_array,"Screen","Power")
            json_send(RM_FeedRate,time_array,"Rod mill","Feed rate")
            json_send(RM_WaterFeed,time_array,"Rod mill","Water feed")
            json_send(BM_WaterFeed,time_array,"Ball mill","Water feed")
            
        finally:
            client.disconnect()
            print("Disconnected")
            time.sleep(FetchCycle)

#How many times we want to try again if code fails
retries = 120 

#For loop for retries
for n in range(retries):
    
    try:
        print('Trying ', n+1)
        while True:
            #Main client function for reading data and posting it
            data_read()
            n=1; #Back to first if data_read() successful
    except:
        #wait 10seconds before trying again in first 5 failures
        if n <= 4:
            print('Something went wrong - retrying after 10 seconds')
            time.sleep(10)
        if n >= 5 and n <=9:
            print('Something went wrong - retrying after 1 minute')
            time.sleep(60)
        if n >= 10 and n <=19:
            print('Something went wrong - retrying after 5 minutes')
            time.sleep(300)
        if n >= 20 and n <=119:
            print('Something went wrong - retrying after 20 minutes')
            time.sleep(1200)   
    else:
        break
else:
    print('Stopping execution')
