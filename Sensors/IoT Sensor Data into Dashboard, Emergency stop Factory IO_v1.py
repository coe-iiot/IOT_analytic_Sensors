#!/usr/bin/env python3

import paho.mqtt.client as mqtt

import time
import json
import pyodbc
import datetime

server = 'kecollege.database.windows.net'
database = 'IoT_Analytics' 
username = 'CloudSA811fa360'
password = 'Admin123$' 
driver= '{ODBC Driver 17 for SQL Server}'
status1=''
topic="kongu" #Data Publish topic
#Driver={ODBC Driver 13 for SQL Server};Server=tcp:kecollege.database.windows.net,1433;Database=IoT_Analytics;Uid=CloudSA811fa360;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
connection_string='DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
print(connection_string)
cnxn = pyodbc.connect(connection_string) 
cursor = cnxn.cursor()


# Function to extract the data from Photoelectric sensor
status1=''
def PES(msg,devaddr):
  qry="update pes_1 set status='"
  s=''
  if(msg[4:6]=="00"):
    s="smoke alaram:ON,"
    status1="off"  # If smoke alaram is on status send signal
                  #to mttp then switch off the machine
  else:
    s="smoke alaram:OFF,"
  x1=msg[6:8]
  res = bin(int(x1, 16)).zfill(8)
  res=str(res[res.index('b')+1:])
  if(len(res)!=8):
    res='0'* (8-len(res)) +res
  if(res[0:2]=='00'):
     s+="Sensor Health:NORMAL,"
     qry+="Running',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',battery_status="
  elif(res[0:2]=='01'):
    s+="Sensor Health:BAD,"
    qry+="Stopped',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',battery_status="
    
  if(res[2:4]=='01'):
     s+="LOW BATTERY ALERT:ON,"
     qry+="'Week',Demolination_status='"
  elif(res[2:4]=='00'):
    s+="LOW BATTERY ALERT:OFF,"
    qry+="'Normal',Demolination_status='"
  if(res[4:6]=='00'):
     s+="DISASSEMBLED STATE':OFF"
     qry+="Normal' where sensor_id='"+devaddr+"'"
  elif(res[4:6]=='01'):
    s+="DISASSEMBLED STATE:ON"
    qry+="Bad' where sensor_id='"+devaddr+"'"
  print(qry)
  cursor.execute(qry)
  cursor.commit()
  publish(client,status1)
  
  return(s)

#function to extract the status of PIR Motion detection sensor

def PIR(msg,devaddr):
  qry="update pir_1 set health_status='"
  x=msg[2:4]
  y=msg[4:6]
  s=''
  if(y=="00"):
    s="alaram status:ON,"
  else:
    s="alaram status:OFF,"

  z=msg[6:8]

  res = bin(int(z, 16)).zfill(9)  
  res=str(res[2:])
  if len(res)==7:
    res="00"+res
    
  if(res[0:2]=="01"):
     s+="Health Status: ON,"
     qry+="Bad',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',battery_status='"
  else:
     s+="sensor failure:OFF,"
     qry+="Good',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',battery_status="
  if(res[2:4]=="01"):
    s+="Low battery alaram:ON,"
    qry+="'Week',Demonilation_status='"
  else:
    s+="Low battery alaram:OFF,"
    qry+="'Good',Demonilation_status='"

  if(res[4:6]=="00"):
     s+="Disassembled State:OFF"
     qry+="Normal' where sensor_id='"+devaddr+"'"
  else:
    s+="Disassembled State:ON"
    qry+="Bad' where sensor_id='"+devaddr+"'"
  print(qry)
  cursor.execute(qry)
  cursor.commit()
  return(s)

#function to extract the status of outdoor temperature and humidity sensor
                                              
def outdoor(msg,devaddr):
  qry="update oth_1 set status='"
  s=''
  if(msg[4:6]=="00"):
    s="Status:ON,"
    qry+="NO ALARAM',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',temperature_value='"
  else:
    s="Status:OFF,"
    qry+="ALARAM ON',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',temperature_value='"
  
  x1=msg[6:10] #EXTRACT TEMPERATURE 
  #print(x1)
  y1=int(x1, 16) # HEXA DECIMAL TO DECIMAL 
  #print(y1)
  s+="Temperature:"+str(y1/100)+","
  qry+=str(y1/100)+"',Humidity_value='"
  x11=msg[10:14] #EXTRACT HUMIDITY
  #print(x11)
  y11=(int(x11, 16))/10000 # HEXA DECIMAL TO DECIMAL FOR HUMIDITY
  #print(y11)
  s+="'Humidity ':'"+str(y11)+"',"
  qry+=str(y11)+"',battery_status='"
  

  x21=msg[14:18] # EXTRACT VOLTAGE  FROM DATA FRAME
  #print("VOLTAGE ",x21)
  y21=(int(x21, 16))/1000
  #print('y21',y21)
  s+="Voltage:"+str(y21)
  qry +=str(y21)+"' where sensor_id='"+devaddr+"'"
  print(qry)
  cursor.execute(qry)
  cursor.commit()

  return(s)

#function to extract the status of indoor temperature and humidity sensor

def indoor(msg,devaddr):
  qry="update ith_1 set status='"
  s=''
  if(msg[4:6]=="00"):
    s="Status:ON,"
    qry+="NO ALARAM',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',temperature_value='"
  else:
    s="Status:OFF,"
    qry+="ALARAM ON',last_detection='"+ datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"',temperature_value='"
  
  x1=msg[6:10] #EXTRACT TEMPERATURE 
  #print(x1)
  y1=int(x1, 16) # HEXA DECIMAL TO DECIMAL 
  #print(y1)
  s+="Temperature:"+str(y1/100)+","
  qry+=str(y1/100)+"',Humidity_value='"
  x11=msg[10:14] #EXTRACT HUMIDITY
  #print(x11)
  y11=(int(x11, 16))/10000 # HEXA DECIMAL TO DECIMAL FOR HUMIDITY
  #print(y11)
  s+="'Humidity ':'"+str(y11)+"',"
  qry+=str(y11)+"',battery_status='"
  

  x21=msg[14:18] # EXTRACT VOLTAGE VALUE FROM DATA FRAME
  #print("VOLTAGE ",x21)
  y21=(int(x21, 16))/1000
  #print('y21',y21)
  s+="Voltage:"+str(y21)
  qry +=str(y21)+"' where sensor_id='"+devaddr+"'"
  print(qry)
  cursor.execute(qry)
  cursor.commit()

  return(s)


#function to extract the status of Proximity sensor                                               

def proximitysensor(msg,devaddr):
  qry="update ps_1 set door_status="
  if(msg[4:7]=="00"):
    qry+="'open',last_detection='"+datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"' where sensor_id='"+devaddr+"'"
  else:
    qry+="'close',last_detection='"+datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"' where sensor_id='"+devaddr+"'"
 
  
  cursor.execute(qry)
  cursor.commit()

  return(qry)


# This is the Subscriber

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("uplink/lora")
  #publish(client)

def on_message(client, userdata, msg):
 
  my_json=msg.payload.decode()
  res = json.loads(my_json)
  process(res['data'],res['devaddr'])
  
 
  #print("Message Recieved:", data)
  #client.disconnect()

# Data Publish through MQTT
def publish(client,status1):
    msg_count = 0
   # global status1
    print("welcome to publish",status1)
        
    #while True:
    
    time.sleep(2)
       
    #machine_status for external off , status1 from smoke sensor
    if(status1=="off"):
          msg = 'true'
    else:
          msg = 'false'
        
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0 or status1=="off":
          print(f"Send `{msg}` to topic `{topic}`")
    else:
          print(f"Failed to send message to topic {topic}")
    msg_count += 1




def process(data,devaddr):
  print(data)
  pir_ids=["EA100204","EA100205","EA100206"]
  pes_ids=["EA100201","EA100202","EA100203"]
  outtemp_ids=["BB100101","BB100102"]
  intemp_ids=["BA10010C"]
  doorsensor_ids=["EB100104","EB100105"]
  
  if devaddr in pir_ids:
    R1=PIR(data,devaddr)
    print(R1)
  elif devaddr in pes_ids:
    R2=PES(data,devaddr)
    print(R2)
  elif devaddr in outtemp_ids:
    R3=outdoor(data,devaddr)
    print(R3)
  elif devaddr in intemp_ids:
    R4=indoor(data,devaddr)
    print(R4)
  elif devaddr in doorsensor_ids:
    R5=proximitysensor(data,devaddr)
    print(R5)
    
    
    
    
client = mqtt.Client()
#client.username_pw_set("admin","admin")
client.connect("10.3.1.147",1883,60)

client.on_connect = on_connect
client.on_message = on_message
time.sleep(2)
#publish(client)
client.loop_forever()
