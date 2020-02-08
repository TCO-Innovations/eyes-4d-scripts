import sys
import psycopg2
import pytz
import re
from datetime import datetime
import numpy as np
#import logging

conn = psycopg2.connect(host='localhost', user='postgres', password='postgres',dbname='eyes4d')

store_contacts = conn.cursor()
fetch_uuid = conn.cursor()
refresh_subscribers = conn.cursor()
store_contact_groups = conn.cursor()
store_fetchLog = conn.cursor()
load_fetchLog = conn.cursor()
load_contacts = conn.cursor()

from temba_client.v2 import TembaClient

client = TembaClient('rapidpro.ilhasoft.mobi', '')
group_uuid_eyes4d_december = "7332c53c-0cb8-48c0-9512-76e93d4dd590"
group_uuid_eyes4d_december_mbarali = "4afadd2f-c071-451c-8d3a-9fd6b533ac02"
group_uuid_eyes4d_december_mbeya = "688781d1-4b0b-4454-8e6a-d996365573aa"
fetched_on = datetime.now()


group_uuid_arr = [group_uuid_eyes4d_december]

#logging.basicConfig(filename='output.log', filemode='w', level=logging.DEBUG)
#logger = logging.getLogger(__name__)
def fetchContacts():
    for contact_batch in client.get_contacts(group=group_uuid_eyes4d_december,after=last_date_of_fetch).iterfetches():
        for contact in contact_batch:
            # fetch values from rapid pro for each contact in the group
            contact_uuid = str(contact.uuid)
            contact_urn = contact.urns if contact.urns else ""
            contact_groups = contact.groups
            contact_name = contact.fields['contact_name'] if contact.fields['contact_name'] != None  else ''
            contact_region = contact.fields['region'] if contact.fields['region'] != None  else ''
            contact_district = contact.fields['district'] if contact.fields['district'] != None else ''
            contact_village_assigned = contact.fields['villageassigned'] if contact.fields['villageassigned'] != None else ''
            contact_households_visited = contact.fields['households_visited_total'] if contact.fields['households_visited_total'] != None else 0
            contact_gender = contact.fields['gender'] if contact.fields['gender'] != None else ''
            
            birth_year = contact.fields['born'] if contact.fields['born'] != None  else None
            # birth_year = birth_year if birth_year != None and birth_year.isdigit() else '0'
            last_household_visit_date = np.datetime64(contact.fields['last_household_visit_date']).astype(datetime).date() if contact.fields['last_household_visit_date'] != None else None
            created_on = np.datetime64(contact.created_on).astype(datetime)
            
            if True:
                #inserting values in the contacts table

                subscriber = checkuuid(contact_uuid)
                if subscriber != None:
                  refresh_subscription(contact_uuid)
                conn.commit()  

                print(contact_name,contact_uuid,contact_urn if contact_urn else "no urn details" )

                #last_successful_fetch_on = datetime.now()

                contact_sql = """INSERT INTO contacts (uuid,urn,name,birth_year,gender,region,district,village_assigned,households_visited,last_household_visit,created_on)
                        VALUES (%s, %s, %s,%s, %s , %s, %s, %s, %s, %s, %s ) """
                        
                try:
                #if True:        
                    store_contacts.execute(contact_sql,(contact_uuid,contact_urn,contact_name,birth_year,contact_gender,contact_region,contact_district,contact_village_assigned,contact_households_visited,last_household_visit_date,created_on))
                #break
                except:
                    print("Unexpected error: ", sys.exc_info()[0], sys.exc_info()[1] )
                    raise
        #commiting changes to the database


    conn.commit()
    putFetchLog(group_uuid_eyes4d_december)
    #print("Row id " , store_contacts.lastrowid)

    print ("success")


def refresh_subscription(uuid):
  refresh_query = """ DELETE FROM contacts WHERE contacts.uuid = %s """
  refresh_subscribers.execute(refresh_query,(uuid,))

def checkuuid(uuid):
  uuid_query = """ SELECT contacts.uuid FROM contacts WHERE contacts.uuid = %s """
  fetch_uuid.execute(uuid_query,(uuid,))
  subscriber = fetch_uuid.fetchone()
  return subscriber


def putFetchLog(uuid):
    last_successful_fetch_on = datetime.now()
    fetchLog_sql = """ INSERT INTO fetchlog (uuid,run_or_contact,last_successful_fetch_on,created_on)
            VALUES (%s,%s,%s,%s) """

    store_fetchLog.execute(fetchLog_sql,(uuid,'contact',last_successful_fetch_on,last_successful_fetch_on))

    conn.commit()
    print ("success fetchlog")

def checkFetchLog():

    load_fetchLog.execute("""SELECT last_successful_fetch_on
                  FROM fetchlog
                  WHERE run_or_contact=%s
                  AND uuid = %s
                  ORDER BY last_successful_fetch_on DESC
                  LIMIT 1
                  """,
               ('contact', group_uuid_eyes4d_december))

    row = load_fetchLog.fetchone()


    last_fetch_time = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
    if (row is not None):
        last_fetch_time =  row[0]

    utc = pytz.timezone('GMT')
    aware_last_fetch_time = utc.localize(last_fetch_time)
    aware_last_fetch_time.tzinfo # <UTC>
    #aware_last_fetch_time.strftime("%a %b %d %H:%M:%S %Y") # Wed Nov 11 13:00:00 2015

    return aware_last_fetch_time

def hasSevenDigitNumber(str):
   return bool(re.search(r'\d{7}', str))
   
last_date_of_fetch = checkFetchLog()

fetchContacts()
