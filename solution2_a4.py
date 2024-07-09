import random
import pyodbc
import json
from datetime import datetime  
import logging
import time
import schedule
#import pandas as pd
sensor_data =""
NewAlerts = ""
NewEvents = ""
events = ""

# Configure logging
logging.basicConfig(filename='actions.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to connect to MSSQL database
def connect_to_database():
    connection_string = 'DRIVER={SQL Server};Server=DESKTOP-EJ2R166;Database=dmlz_mshist;Trusted_Connection=True;'
    # connection_string = 'DRIVER={SQL Server};Server=B-Rad;Database=dmlz_mshist;Trusted_Connection=True;'
    logging.info('Connected to DB ok')
    conn = pyodbc.connect(connection_string)
    return conn.cursor()
    
def generate_15_digit_random_number():
    # Generate a random 14-digit number (as the first 14 digits)
    first_14_digits = ''.join([str(random.randint(0, 9)) for _ in range(14)])
    # Generate the last digit separately to ensure it's not zero
    last_digit = str(random.randint(1, 9))
    # Concatenate the first 14 digits and the last digit
    random_number = first_14_digits + last_digit
    return random_number



# Function to retrieve sensor readings and events/alerts data from MSSQL for a specific machine and generate JSON
def generate_json_for_machine(serial_number):
    print(serial_number)
    cursor = connect_to_database()
    # Retrieve sensor readings and events/alerts for the specified machine for the last 5 minutes
    query = "select top 20 hcs.HEALTH_LOG_OID, hcs.SAMPLE_VALUE as Sample_Value, md.PROTOCOL_UNIT_NAME AS MeasureName, hl.endtime AS endtime, hl.model_n AS modelnumber, hl.machine_N , hl.serialNumber AS serialnumber, hl.endtime AS endtime, hl.startTime AS StartTime FROM dbo.HEALTH_CONTINUOUS_SAMPLE AS hcs INNER JOIN dmlz_msmodel.dbo.MEASURE_DEF AS md ON hcs.MEASURE_OID = md.OID INNER JOIN dmlz_mshist.dbo.V_HEALTH_LOG AS hl ON hcs.HEALTH_LOG_OID = hl.OID WHERE hl.serialNumber =? GROUP BY PROTOCOL_UNIT_NAME, hcs.HEALTH_LOG_OID, hcs.SAMPLE_VALUE, hl.model_n, hl.endtime, hl.serialnumber, hl.startTime, hl.machine_N " 
    cursor.execute(query, (serial_number))
    # cursor.execute(query)
    sensor_data = cursor.fetchall()
    print(sensor_data)
  
    DeviceModel = ''
    timestamp = ''
    AlarmStartTime = ''
#measurements area     
    Measurements = []
    for row in sensor_data:
        # Map Cummins measure names to Sandvik engine names
        # serial_number = serial_number[0]
        CAT_measure_name = (row.MeasureName)
        endtime_ = (row.endtime)
        Machine = (row.machine_N)
        SerialNumber = (row.serialnumber)
        DeviceModel = (row.modelnumber) 
        Sample_Value = (row.Sample_Value)
        
        Measurements.append({
            "Machine": Machine,
            "SerialNumber": SerialNumber,
            "SignalName": CAT_measure_name,
            "timestamp": endtime_, #.strftime('%Y-%m-%d %H:%M:%S'),
            "value": Sample_Value
        })
    #Alerts 
 #Retrieve alert from event log   
    query = "SELECT top 10 DURATION AS AlarmActiveTime, EVENT_OID AS AlarmInstanceCount, HELEVEL AS AlarmCount, operator AS AlarmOperatorName, DATEADD(hh,9,SOURCE_TIMESTAMP_UTC) AS AlarmStartTime, MID AS Code,vmd.description AS Description ,CASE WHEN TYPE = 'Health Data' THEN 'PAlarm' ELSE 'null' END AS State,SOURCE_TIMESTAMP_UTC AS Timestamp ,ALARM_DESCRIPTION AS Title, vm.serialNumber AS Device, vm.name AS machine_N,vm.machineClass_N AS DeviceModel FROM dmlz_mshist.dbo.HEALTH_EVENT he INNER JOIN dmlz_mshist.dbo.V_MACHINE vm ON vm.OID = he.MACHINE_OID INNER JOIN dmlz_mshist.dbo.V_MID_DEF vmd ON vmd.OID = he.MID where vm.serialNumber = ? AND Type = 'Health Data' AND he.CLASS_ID = 'Deactivate' "   
    cursor.execute(query, (serial_number))
    alert_data = cursor.fetchall()
    NewAlerts=[]
 
    for row in alert_data:
        # serial_number = serial_number[0]
        AlarmActiveTime = (row.AlarmActiveTime)
        AlarmCount = (row.AlarmCount)
        AlarmInstanceCount = (row.AlarmInstanceCount)
        AlarmOperatorName = (row.AlarmOperatorName)
        AlarmStartTime = (row.AlarmStartTime)
        Code = (row.Code)
        Description = (row.Description)
        machine_id = (row.Device)
        Machine = (row.machine_N)
        state = (row.State)
        timestamp = (row.Timestamp)
        Title = (row.Title)
        NewAlerts.append({
            #"Alert Type": "AAlarm",
            "Machine Name": Machine,
            "Device": machine_id,
            "AlarmActiveTime": AlarmActiveTime, #.strftime('%Y-%m-%d %H:%M:%S'),
            "AlarmCount": AlarmCount,
            "AlarmInstanceCount":  AlarmInstanceCount,
            "AlarmOperatorName":  AlarmOperatorName,
            "AlarmStartTime":  AlarmStartTime,
            "Code":  Code,
            "Description": Description,
            "state":  "AAlarm",
            "timestamp":  timestamp,
            "Title":  Title
        })
        print(row)
        #Alerts PAlaram Health Maintenance
#Retrieve event from event log 
    query = "SELECT top 10 DURATION AS AlarmActiveTime, EVENT_OID AS AlarmInstanceCount, HELEVEL AS AlarmCount, operator AS AlarmOperatorName, DATEADD(hh,9,SOURCE_TIMESTAMP_UTC) AS AlarmStartTime, MID AS Code,vmd.description AS Description ,CASE WHEN TYPE = 'Health Maintenance' THEN 'AAlarm' ELSE 'null' END AS State,SOURCE_TIMESTAMP_UTC AS Timestamp ,ALARM_DESCRIPTION AS Title, vm.serialNumber AS Device, vm.name AS machine_N,vm.machineClass_N AS DeviceModel FROM dmlz_mshist.dbo.HEALTH_EVENT he INNER JOIN dmlz_mshist.dbo.V_MACHINE vm ON vm.OID = he.MACHINE_OID INNER JOIN dmlz_mshist.dbo.V_MID_DEF vmd ON vmd.OID = he.MID where vm.serialNumber = ? AND Type = 'Health Maintenance' AND he.CLASS_ID = 'Deactivate' "   
    cursor.execute(query, (serial_number,))
    event_data = cursor.fetchall()
    events = []
 
    for row in event_data:
        AlarmActiveTime = (row.AlarmActiveTime)
        AlarmCount = (row.AlarmCount)
        AlarmInstanceCount = (row.AlarmInstanceCount)
        AlarmOperatorName = (row.AlarmOperatorName)
        AlarmStartTime = (row.AlarmStartTime)
        Code = (row.Code)
        Description = (row.Description)
        machine_id = (row.Device)
        Machine = (row.machine_N)
        state = (row.State)
        timestamp = (row.Timestamp)
        Title = (row.Title)
        events.append({
            #"Alert Type": "PAlarm",
            "Machine Name": Machine,
            "Device": machine_id,
            "AlarmActiveTime": AlarmActiveTime, #.strftime('%Y-%m-%d %H:%M:%S'),
            "AlarmCount": AlarmCount,
            "AlarmInstanceCount":  AlarmInstanceCount,
            "AlarmOperatorName":  AlarmOperatorName,
            "AlarmStartTime":  AlarmStartTime,
            "Code":  Code,
            "Description": Description,
            "state":  "PAlarm",
            "timestamp":  timestamp,
            "Title":  Title
        })
        print(row)
    logging.info('Create JSON file for 3')
    x= generate_15_digit_random_number()
    # Creating JSON object
    json_data = {
        "AlertHistory": [],
        "Device": serial_number,	
        "DeviceModel": DeviceModel,
        "EndTime": timestamp,# datetime(endtime), #
        "Event History": [],
        "Measurements": Measurements,
        #"Samples": Samples,
        "NewAlerts": NewAlerts,        
        #"NewAlerts": NewAlerts2,        
        "events": events,
        "date": datetime.now().strftime('%Y-%m-%d'),
        "start_time": datetime.now().strftime('%H:%M:%S'),
        "serial_number": serial_number,
        "ProductVersion": "3.1.8.133",										
        "ReportId": x,													
        "ReportVersion": "3.0",												
        "SourceId": 12,														
        "StartTime": AlarmStartTime	
    }    
    # Step 2: Convert the datetime object to the desired format
    for reading in json_data['Measurements']:
        # reading['timestamp'] = datetime.strptime(reading['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
        reading['timestamp'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')        
    for events in json_data['events']:
        events['timestamp'] = datetime.strptime(events['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
        events['timestamp'] = events['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        events['AlarmStartTime'] = datetime.strptime(events['AlarmStartTime'], "%Y-%m-%d %H:%M:%S.%f")
        events['AlarmStartTime'] = events['AlarmStartTime'].strftime('%Y-%m-%d %H:%M:%S') 
    #for NewAlerts in json_data['NewAlerts']:
     #   NewAlerts['timestamp'] = NewAlerts['timestamp'].strftime('%Y-%m-%d %H:%M:%S') 
     #   NewAlerts['AlarmStartTime'] = NewAlerts['AlarmStartTime'].strftime('%Y-%m-%d %H:%M:%S') 
        #Create datetime object
    dt = datetime.now()
    logging.info('Create JSON file for 2')
    #Convert the datetime to strings
    dt_str = dt.strftime("%Y%m%d%H%M%S")
    
    # Writing JSON to file
    with open(f"Report-{serial_number}-{dt_str}.json", 'w') as json_file:
        json.dump(json_data, json_file, indent=4, default=str)
    logging.info('Create JSON file for 1')    

# Function to schedule JSON generation for each machine every 5 minutes
def schedule_json_generation():
    cursor = connect_to_database()
    cursor.execute("select DISTINCT top 200 serialnumber as SerialNumber FROM V_Health_Log")
    #serial_numbers = [row.SerialNumber for row in cursor.fetchall()]
    serial_numbers = cursor.fetchall()
    
    
    logging.info('Create JSON file for select machine') 
    
    for serial_number in serial_numbers:
        serial_number = serial_number[0]
        
        schedule.every(1).minutes.do(generate_json_for_machine, serial_number)
        time.sleep(5)
        
    while True:
        schedule.run_pending()
        time.sleep(2)

if __name__ == "__main__":
    schedule_json_generation()