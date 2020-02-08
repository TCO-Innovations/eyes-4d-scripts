import sys
import numbers
import psycopg2
import pytz
from datetime import datetime
import numpy as np
#import logging


conn = psycopg2.connect(host='localhost', user='postgres', password='postgres',dbname='eyes4d')

store_runs = conn.cursor()
store_fetchLog = conn.cursor()

load_fetchLog = conn.cursor()
load_runs = conn.cursor()

refresh_runs = conn.cursor()
fetch_run = conn.cursor()

check_contact_facility = conn.cursor()

from temba_client.v2 import TembaClient

client = TembaClient('rapidpro.ilhasoft.mobi', '')
group_uuid_eyes4d_december = "7332c53c-0cb8-48c0-9512-76e93d4dd590"
group_uuid_eyes4d_december_mbarali = "4afadd2f-c071-451c-8d3a-9fd6b533ac02"
group_uuid_eyes4d_december_mbeya = "688781d1-4b0b-4454-8e6a-d996365573aa"
fetched_on = datetime.now()

flow_uuid_live = 'a1438627-b4b2-49e7-be62-ed9c10cf9d98'
flow_uuid_pilot = '875c93ba-fdcb-4cfd-b6a2-f4a1777c8e75'

flow_uuid_arr = [flow_uuid_live, flow_uuid_pilot] 

flow_uuid_dict = { flow_uuid_live: group_uuid_eyes4d_december, flow_uuid_pilot:group_uuid_eyes4d_december}

head_of_house = ""
is_there_a_latrine = ""
does_it_have_a_lockable_door = ""
does_it_have_brick_wall=""
does_the_latrine_have_a_cemented_floor=""
has_iron_sheet_roof=""
does_it_have_a_bathroom_next_to_it=""
place_for_hand_washing=""
container_for_hand_washing=""
is_there_soap=""
kinyesi_kuzunguka_nyumba=""
kinyesi_ukutani=""

def lookup_village():
    print("Looking up Village")

def fetchRuns():
     for idx, val in flow_uuid_dict.items() :
        last_date_of_fetch = checkFetchLog(idx)
        count = 0
        for runs_batch in client.get_runs(flow=idx, after=last_date_of_fetch).iterfetches(retry_on_rate_exceed=True):
            for current_run in runs_batch:
                count += 1
                contact_uuid = current_run.contact.uuid
                flow_uuid = current_run.flow.uuid
                flow_uuid = current_run.flow.uuid
                run_id = str(current_run.id)
                createdon =  current_run.created_on.strftime('%Y-%m-%d %H:%M:%S')
                modifiedon =  current_run.created_on.strftime('%Y-%m-%d %H:%M:%S')
                # print(contact_uuid,run_id)
                values = current_run.values

                # subscriber = checkuuid(uuid)
                # if subscriber != None:
                #   refresh_subscription(uuid)
                # conn.commit() 
                
                for key in values:
                    # print (key, 'corresponds to', values[key].category)
                    label = key
                    theValue = ""
                    if values[key].category == "All Responses" :                 
                        theValue = values[key].value
                    else:                
                        theValue = values[key].category
                
                    setFieldvalue(label, theValue)
                run_sql = """INSERT INTO runs (contact_uuid, flow_uuid, run_id,  created_on, modified_on, head_of_house,  is_there_a_latrine, does_it_have_a_lockable_door, does_it_have_brick_wall, does_the_latrine_have_a_cemented_floor, has_iron_sheet_roof, does_it_have_a_bathroom_next_to_it, place_for_hand_washing, container_for_hand_washing, is_there_soap, kinyesi_kuzunguka_nyumba, kinyesi_ukutani)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                            
                #try:
                print(head_of_house, is_there_a_latrine, does_it_have_a_lockable_door, does_it_have_brick_wall, does_the_latrine_have_a_cemented_floor, has_iron_sheet_roof, does_it_have_a_bathroom_next_to_it, place_for_hand_washing, container_for_hand_washing, is_there_soap, kinyesi_kuzunguka_nyumba, kinyesi_ukutani)
                sqlvalues = (contact_uuid, flow_uuid, run_id, createdon, modifiedon, head_of_house, is_there_a_latrine, does_it_have_a_lockable_door, does_it_have_brick_wall, does_the_latrine_have_a_cemented_floor, has_iron_sheet_roof, does_it_have_a_bathroom_next_to_it, place_for_hand_washing, container_for_hand_washing, is_there_soap, kinyesi_kuzunguka_nyumba, kinyesi_ukutani)
                        
                    
                #print (sqlvalues)  
                store_runs.execute(run_sql, sqlvalues)

        print ("Flow ", idx, " total ", count)
        conn.commit()
        putFetchLog(idx)

   
def setFieldvalue(fieldLabel, fieldValue):
    if fieldLabel.upper() == "head_of_house".upper():
        global head_of_house 
        # print(fieldValue)
        head_of_house = fieldValue        
        #break
    elif fieldLabel.upper() == "is_there_a_latrine".upper():
        global is_there_a_latrine
        # print(fieldValue)
        is_there_a_latrine = fieldValue  
        #break
    elif fieldLabel.upper() == "does_it_have_a_lockable_door".upper():
        global does_it_have_a_lockable_door
        does_it_have_a_lockable_door = fieldValue  
        #break
    elif fieldLabel.upper() == "does_it_have_brick_wall".upper():
        global does_it_have_brick_wall 
        does_it_have_brick_wall =  fieldValue  
        #break
    elif fieldLabel.upper() == "does_the_latrine_have_a_cemented_floor".upper():
        global does_the_latrine_have_a_cemented_floor 
        does_the_latrine_have_a_cemented_floor = fieldValue  
        #break
    elif fieldLabel.upper() == "has_iron_sheet_roof".upper():
        global has_iron_sheet_roof 
        has_iron_sheet_roof = fieldValue  
        #break
    elif fieldLabel.upper() == "does_it_have_a_bathroom_next_to_it".upper():
        global does_it_have_a_bathroom_next_to_it 
        does_it_have_a_bathroom_next_to_it = fieldValue  
        #break
    elif fieldLabel.upper() == "place_for_hand_washing".upper():
        global place_for_hand_washing 
        place_for_hand_washing = fieldValue   
        #break
    elif fieldLabel.upper() == "container_for_hand_washing".upper():
        global container_for_hand_washing 
        container_for_hand_washing = fieldValue  
        #break
    elif fieldLabel.upper() == "is_there_soap".upper():
        global is_there_soap 
        is_there_soap = fieldValue   
        #break
    elif fieldLabel.upper() == "kinyesi_kuzunguka_nyumba".upper():
        global kinyesi_kuzunguka_nyumba 
        kinyesi_kuzunguka_nyumba = fieldValue  
        #break        
    elif fieldLabel.upper() == "kinyesi_ukutani".upper():
        global kinyesi_ukutani 
        kinyesi_ukutani = fieldValue  
        #break      
    else:
        # print(fieldLabel, fieldValue)
        x = 0
        #break 


def putFetchLog(uuid):
    last_successful_fetch_on = datetime.now()

    fetchLog_sql = """ INSERT INTO fetchlog (uuid,run_or_contact,last_successful_fetch_on,created_on)
            VALUES (%s,%s,%s,%s) """

    store_fetchLog.execute(fetchLog_sql,(uuid,'run',last_successful_fetch_on,fetched_on))

    conn.commit()
          

    #print ("success")

def checkFetchLog(uuid):

    load_fetchLog.execute("""SELECT last_successful_fetch_on
                  FROM fetchlog
                  WHERE run_or_contact=%s
                  AND uuid = %s
                  ORDER BY last_successful_fetch_on DESC
                  LIMIT 1
                  """,
               ('run', (uuid,)))

    row = load_fetchLog.fetchone()


    last_fetch_time = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
    if (row is not None):
        last_fetch_time =  row[0]
    print(last_fetch_time)
    utc = pytz.timezone('GMT')
    aware_last_fetch_time = utc.localize(last_fetch_time)
    aware_last_fetch_time.tzinfo # <UTC>
    #aware_last_fetch_time.strftime("%a %b %d %H:%M:%S %Y") # Wed Nov 11 13:00:00 2015

    return aware_last_fetch_time


fetchRuns()
