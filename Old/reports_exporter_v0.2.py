#!/usr/bin/env python
# coding: utf-8

# In[90]:


# V0.1 basic functionalities. read and export Train List, Occupancy, and Booking Payment Detailed
# V0.2 handle all the information of each kind of report together


# In[91]:


get_ipython().system('pip install openpyxl')
get_ipython().system('pip install psycopg2')
get_ipython().system('pip install sqlalchemy')


# In[92]:


# connection to the database
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text

# Create an engine instance
alchemyEngine = create_engine("postgresql+psycopg2://postgres:Renfe2022@172.19.28.174:5433/SalesSystem", pool_recycle=5);


# In[93]:


# import 
import pandas as pd
import numpy as np
import datetime
import os
import warnings

# STATES
NO_REPORT = 0
TRAIN_LIST_REPORT = 1
OCCUPANCY_REPORT = 2
BOOKING_PAYMENT_REPORT = 3

# tables
train_list_table = 'train_list'
occupancy_table = 'occupancy_list_hist'
bpd_table = 'booking_payment_detailed'


# In[94]:


# function that just add a timestamps decoraton in print functions
def prt_info(string):
    import datetime
    
    time = datetime.datetime.now()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {string}")


# In[95]:


# function that detects which kind of report is the excel file
def get_report_name(excel_file_path):

    train_list_header = pd.DataFrame([
        'Departure Date',
        'Train Number',
        'OD',
        'Origin Station',
        'Destination Station',
        'Coach Number',
        'Seat Number',
        'Class',
        'Booking Code',
        'Ticket Number',
        'Tariff',
        'Status',
        'Payment Mode',
        'Media Type',
        'Sales Channel',
        'Base Price',
        'VAT Base Price',
        'Management Fee',
        'VAT Management Fee',
        'Payment Fee',
        'VAT Payment Fee',
        'Operation Amount',
        'Penalty Tariff',
        'Amount Not Refunded',
        'Compensation Type',
        'Compensation Reason',
        'Compensation Status',
        'Nationality',
        'Gender',
        'Name',
        'Surname',
        'Document',
        'Prefix',
        'Telephone',
        'Profile',
        'Special Needs',	
        'Validation Time',
        'Group',
        'Checked On Board',
        'Last Operation Channel',
        'Last Operation Equipment Code'
        ])

    occupancy_header = pd.DataFrame([
        'Date',
        'OD',
        'Origin Station',
        'Destination Station',
        'Train ID',
        'Train Number',
        'Class',
        'Total Seats (Quota + Carer + PRM)',
        'Quota Configuration',
        'Total Locks (Quota + Carer + PRM)',
        'For Sale',
        'Reserved Usual Seats',
        'Reserved PRM Seats',
        'Reserved Carer Seats',	
        'Ticket Reserved (Usual + Carer + PRM)',
        'Reserved & Lock Usual Seats',
        'Reserved & Lock PRM Seats',
        'Reserved & Lock Carer Seats',	
        'Total Available',
        'Validating',
        'No Show',
        'UnBooked',	
        'Passengers Inc. Infants',
        'Checked On Board'
    ])

    bpd_header = pd.DataFrame([
       'Booking Code',
       'Ticket Number',	
       'Operation Date',	
       'Base Price',
       'VAT Base Price',
       'Management Fee',
       'VAT Management Fee',
       'Payment Fee',
       'VAT Payment Fee',
       'Operation Amount',	
       'Penalty Tariff',	
       'Compensation Type',	
       'Compensation Reason',	
       'Compensation Status',
       'Card Number',
       'Authorization Code',
       'Order ID',
       'Transaction ID',
       'Status Payment Card',
       'Card Brand',
       'Bill Number',
       'Bill Status',
       'Train Number',	
       'Departure Date',	
       'Arrival Date',
       'OD',
       'Origin Station',
       'Destination Station',
       'Class',
       'Tariff',	
       'Reserved Number of Seats',
       'Status',
       'Card Serial Number',
       'Card User Name',
       'Sales Station',
       'Sales Channel',
       'Sales Equipment Code',
       'Payment Mode',
       'Coach Number',	
       'Seat Number',
       'Nationality',
       'Name',
       'Surname',
       'Gender',
       'Document Type',
       'Document',
       'Prefix',
       'Telephone',
       'Email',
       'Profile',	
       'Validation Time',
       'Checked On Board',	
       'Detail Type',
       'Tipology',
       'Last Operation Channel',
       'Last Operation Equipment Code'

    ])

    try:
        # read the header of the file
        file_header = pd.read_excel(excel_file_path, skiprows=6, nrows=2, header=None)
    except Exception as e:
        prt_info(f"There is a problem reading the file: {e}")
        return NO_REPORT
        
    # clean
    file_header = file_header.transpose()
    file_header_n7 = file_header[0]
    file_header_n8 = file_header[1]

    file_header_n7.dropna(axis=0, inplace=True)
    file_header_n7.reset_index(drop=True, inplace=True)
    file_header_n7 = pd.DataFrame(file_header_n7)
    file_header_n7.columns = [0]

    file_header_n8.dropna(axis=0, inplace=True)
    file_header_n8.reset_index(drop=True, inplace=True)
    file_header_n8 = pd.DataFrame(file_header_n8)
    file_header_n8.columns = [0]
    
    # comparision
    if(pd.DataFrame(file_header_n8).equals(train_list_header)): return TRAIN_LIST_REPORT
    elif(pd.DataFrame(file_header_n7).equals(occupancy_header)): return OCCUPANCY_REPORT
    elif(pd.DataFrame(file_header_n8).equals(bpd_header)): return BOOKING_PAYMENT_REPORT
    else: return NO_REPORT


# In[96]:


def read_train_list(file_name, alchemyEngine):
    # function to read the train_list excel file and calculate the extra columns of the report
    
    # train_list datatype
    train_list_datatype = {
    'Departure Date': str,
    'Train Number': str,
    'OD': str,
    'Origin Station': str,
    'Destination Station': str,
    'Coach Number': str,
    'Seat Number': str,
    'Class': str,
    'Booking Code': str,
    'Ticket Number': str,
    'Tariff': str,
    'Status': str,
    'Payment Mode': str,
    'Media Type': str,
    'Sales Channel': str,
    'Base Price': float,
    'VAT Base Price': float,
    'Management Fee': float,
    'VAT Management Fee': float,
    'Payment Fee': float,
    'VAT Payment Fee': float,
    'Operation Amount':	float,
    'Penalty Tariff': float,
    'Amount Not Refunded': float,
    'Compensation Type': str,
    'Compensation Reason': str,
    'Compensation Status': str,
    'Nationality': str,
    'Gender': str,
    'Name': str,
    'Surname': str,
    'Document': str,
    'Prefix': str,
    'Telephone': str,
    'Profile': str,
    'Special Needs': str,	
    'Validation Time': str,
    'Group': str,
    'Checked On Board': str,
    'Last Operation Channel': str,
    'Last Operation Equipment Code': str
    }
    
    try:
        # open file
        df_file = pd.read_excel(file_name, header=0, skiprows=7, dtype=train_list_datatype,
                                parse_dates=['Departure Date', 'Validation Time'],
                                date_format={'Departure Date': '%Y-%m-%d %H:%M', 'Validation Time': '%Y-%m-%d  %H:%M'})
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")
    
    # Remove the last row
    df_file = df_file.drop(df_file.index[-1])

    # check duplicates
    #duplicates = df_file["Ticket Number"].duplicated(keep='first')
    #if(duplicates.sum() > 0):
    #    prt_info(f"Deleting {duplicates.sum()} duplicated entries.")
    #    df_file.drop_duplicates(subset='Ticket Number', keep='first', inplace=True, ignore_index=True)

    # format current date columns
    departure_date_time = pd.to_datetime(df_file['Departure Date'], format="%Y-%m-%d %H:%M:%S")
    df_file['Departure Date'] = departure_date_time.dt.strftime('%Y-%m-%d %H:%M')
    df_file['Validation Time'] = pd.to_datetime(df_file['Validation Time'], format="%Y-%m-%d %H:%M:%S").dt.strftime('%Y-%m-%d %H:%M')
    
    # create extra columns
    df_file['Train_hour'] = departure_date_time.dt.strftime('%H:%M')
    df_file['Departure_Date_Short'] = departure_date_time.dt.strftime('%Y-%m-%d')
    df_file['Train-OD Short'] = df_file['Train Number'] + " - " + df_file['OD']
    df_file['CORRIDOR'] = df_file['Train Number'].str[:2]
    df_file['WEEK_DAY'] = departure_date_time.dt.strftime('%a')
    df_file['WEEK_NUM'] = departure_date_time.dt.isocalendar().week
    df_file['train_key'] = df_file['Departure_Date_Short'] + " - " + df_file['Train-OD Short']
    
    # get the train departure
    try:
        train_hours = pd.read_sql_table('train_departure_times', alchemyEngine, schema='AFC')
    except Exception as e:
        raise(f"Error fetching the departure times from database: {e}")
        
    train_hours.columns = ['Train Number', 'train_departure_date_time']
    df_file = pd.merge(df_file, train_hours, on="Train Number", how="inner")

    #check if there is missing hours for the train numbers of this file
    if(df_file['train_departure_date_time'].isnull().sum() > 0):
        trains_missing = df_file[df_file['train_departure_date_time'].isnull()]['Train Number'].unique()
        raise Exception(f"There are missing departing hours in the database. Please, check the following trains: {", ".join(trains_missing)}")

    # calculate the departing time of the train
    df_file['train_departure_date_time'] = pd.to_datetime(df_file['Departure_Date_Short'].astype(str) + " " + df_file['train_departure_date_time'].astype(str))
    train_date_adjustment = df_file['train_departure_date_time'].dt.time > departure_date_time.dt.time
    df_file['train_departure_date_time'] = df_file['train_departure_date_time'] - pd.to_timedelta(train_date_adjustment.astype(int), unit="D")
    df_file['train_departure_date_short'] = departure_date_time.dt.date - pd.to_timedelta(train_date_adjustment.astype(int), unit="D")
    
    # calculate the services date (reduce one day if it is an early train before maintenance window)
    service_date_adjustment = df_file['train_departure_date_time'].dt.time <= datetime.time(5, 0)
    df_file['Service_Date'] = df_file['train_departure_date_short'] - pd.to_timedelta(service_date_adjustment.astype(int), unit="D")
    
    # get the date time of the operation
    ticket_numbers = ", ".join(f"'{ticket}'" for ticket in df_file['Ticket Number'].unique())
    query = f"""
    SELECT ticket_number AS \"Ticket Number\", operation_date_time
    FROM \"AFC\".booking_payment_detailed
    WHERE ticket_number IN ({ticket_numbers})
    """
    df_operation_date_times = pd.read_sql_query(query, alchemyEngine)
    df_file = pd.merge(df_file, df_operation_date_times, on="Ticket Number", how="left")
    df_file['operation_date'] = df_file['operation_date_time'].dt.strftime("%Y-%m-%d")
    
    # set the headers according to database
    df_file.columns = [
    'departure_date', 
    'train_number', 
    'od', 
    'origin_station', 
    'destination_station',
    'coach_number', 
    'seat_number', 
    'class', 
    'booking_code', 
    'ticket_number', 
    'tariff', 
    'status', 
    'payment_mode', 
    'media_type', 
    'sales_channel', 
    'base_price', 
    'vat_base_price',
    'management_fee', 
    'vat_management_fee', 
    'payment_fee', 
    'vat_payment_fee', 
    'operation_amount', 
    'penalty_tariff', 
    'amount_not_refunded', 
    'compensation_type', 
    'compensation_reason', 
    'compensation_status', 
    'nationality', 
    'gender', 
    'name', 
    'surname', 
    'document', 
    'prefix', 
    'telephone', 
    'profile', 
    'special_needs', 
    'validating_time', 
    'groupyn', 
    'checked_on_board', 
    'last_operation_channel', 
    'last_operation_equipment_code', 
    'train_hour', 
    'departure_date_short', 
    'train_od_short', 
    'stretch', 
    'week_day', 
    'week_num', 
    'train_key', 
    'train_departure_date_time', 
    'train_departure_date_short', 
    'service_train_departure_date_short', 
    'operation_date_time', 
    'operation_date']

    return df_file



# In[97]:


def read_booking_payment(file_name):

    booking_payment_datatype = {
        'Booking Code':str,
       'Ticket Number':str,	
       'Operation Date':str,	
       'Base Price':float,
       'VAT Base Price':float,
       'Management Fee':float,
       'VAT Management Fee':float,
       'Payment Fee':float,
       'VAT Payment Fee':float,
       'Operation Amount':float,	
       'Penalty Tariff':float,	
       'Compensation Type':str,	
       'Compensation Reason':str,	
       'Compensation Status':str,
       'Card Number':str,
       'Authorization Code':str,
       'Order ID':str,
       'Transaction ID':str,
       'Status Payment Card':str,
       'Card Brand':str,
       'Bill Number':str,
       'Bill Status':str,
       'Train Number':str,	
       'Departure Date':str,	
       'Arrival Date':str,
       'OD':str,
       'Origin Station':str,
       'Destination Station':str,
       'Class':str,
       'Tariff':str,	
       'Reserved Number of Seats':str,
       'Status':str,
       'Card Serial Number':str,
       'Card User Name':str,
       'Sales Station':str,
       'Sales Channel':str,
       'Sales Equipment Code':str,
       'Payment Mode':str,
       'Coach Number':str,	
       'Seat Number':str,
       'Nationality':str,
       'Name':str,
       'Surname':str,
       'Gender':str,
       'Document Type':str,
       'Document':str,
       'Prefix':str,
       'Telephone':str,
       'Email':str,
       'Profile':str,	
       'Validation Time':str,
       'Checked On Board':str,	
       'Detail Type':str,
       'Tipology':str,
       'Last Operation Channel':str,
       'Last Operation Equipment Code':str
    }

    # read
    try:
        df_file = pd.read_excel(file_name, header=0, skiprows=7, dtype=booking_payment_datatype)
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")

    # Remove the last row
    df_file = df_file.drop(df_file.index[-1])
    
    # check duplicates
    #duplicates = df_file["Ticket Number"].duplicated(keep='first')
    #if(duplicates.sum() > 0):
    #    prt_info(f"Deleting {duplicates.sum()} duplicated entries.")
    #    df_file.drop_duplicates(subset='Ticket Number', keep='first', inplace=True, ignore_index=True)
    
    # format the dates
    df_file['Operation Date'] = pd.to_datetime(df_file['Operation Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M')
    df_file['Departure Date'] = pd.to_datetime(df_file['Departure Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M')
    df_file['Arrival Date'] = pd.to_datetime(df_file['Arrival Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M')
    
    # set column names
    df_file.columns = [
        'booking_code', 
        'ticket_number',
        'operation_date_time', 
        'base_price', 
        'base_price_vat', 
        'management_fee', 
        'management_fee_vat', 
        'payment_fee', 
        'payment_fee_vat', 
        'operation_amount', 
        'penalty_tariff', 
        'amount_not_refunded', 
        'compensation_type', 
        'compensation_reason', 
        'compensation_status', 
        'card_number', 
        'authorization_code', 
        'order_id', 
        'transaction_id', 
        'status_payment_card', 
        'card_brand', 
        'bill_number', 
        'bill_status', 
        'train_number', 
        'departure_date_time', 
        'arrival_date_time', 
        'od', 
        'origin_station', 
        'destination_station', 
        'class', 
        'tariff', 
        'reserved_number_of_seats', 
        'status', 
        'card_serial_number', 
        'card_user_name', 
        'sales_station', 
        'sales_channel', 
        'equipment_code', 
        'payment_mode', 
        'coach_number', 
        'seat_number', 
        'country_code', 
        'name', 
        'surname', 
        'gender', 
        'document_type', 
        'document', 
        'prefix', 
        'telephone', 
        'email', 
        'profile', 
        'validating_time', 
        'checked_on_board', 
        'detail_type', 
        'tipology', 
        #'compensated', 
        #'include_fare_revenue', 
        'last_operation_channel', 
        'last_operation_equipment_code'
    ]
    # return
    return df_file


# In[98]:


def read_occupancy(file_name):    
    
    # define the datatype
    occupancy_datatype = {
        'Date':str,
        'OD':str,
        'Origin Station':str,
        'Destination Station':str,
        'Train ID':str,
        'Train Number':str,
        'Class':str,
        'Total Seats (Quota + Carer + PRM)':str,
        'Quota Configuration':str,
        'Total Locks (Quota + Carer + PRM)':str,
        'For Sale':str,
        'Reserved Usual Seats':str,
        'Reserved PRM Seats':str,
        'Reserved Carer Seats':str,	
        'Ticket Reserved (Usual + Carer + PRM)':str,
        'Reserved & Lock Usual Seats':str,
        'Reserved & Lock PRM Seats':str,
        'Reserved & Lock Carer Seats':str,	
        'Total Available':str,
        'Validating':str,
        'No Show':str,
        'UnBooked':str,	
        'Passengers Inc. Infants':str,
        'Checked On Board':str
    }
    
    # read the file
    try:
        df_file = pd.read_excel(file_name, header=0, skiprows=6, dtype=occupancy_datatype, parse_dates=['Date'], date_format={'Date':'%Y-%m-%d %H:%M:%S'})
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")
    
    # Remove the last two row
    df_file.drop(df_file.index[-1], inplace=True)
    df_file.drop(df_file.index[-1], inplace=True)
        
    # reformat date columns
    date_time = pd.to_datetime(df_file['Date'], format='%Y-%m-%d %H:%M:%S')
    df_file['Date'] = date_time.dt.strftime('%Y-%m-%d %H:%M')
    
    # create the extra columns
    df_file['Data_Date'] = datetime.date.today()
    df_file['train_key'] = date_time.dt.strftime('%Y-%m-%d') + " - " + df_file['Train Number'] + " - " + df_file['OD']
    
    # rename the columns
    df_file.columns = [
        'date', 
        'od', 
        'origin_station', 
        'destination_station', 
        'train_id', 
        'train_number', 
        'class', 
        'total_seats', 
        'quota_configuration', 
        'total_locks', 
        'for_sale', 
        'reserved_usual_seats', 
        'reserved_prm_seats', 
        'reserved_carer_seats', 
        'ticket_reserved', 
        'reserved_lock_usual_seats', 
        'reserved_lock_prm_seats', 
        'reserved_lock_carer_seats', 
        'total_available', 
        'validating', 
        'no_show', 
        'unbooked', 
        'passengers_inc_infant', 
        'checked_on_board', 
        'data_date', 
        'train_key'
    ]
    

        
    # return
    return df_file


# In[99]:


def export_train_list(df_file, alchemyEngine):
    
    # Extract unique dates from the DataFrame
    unique_dates = df_file['departure_date_short'].unique()
    date_conditions = ', '.join([f"'{date}'" for date in unique_dates])
    
    # variables of the ddbb
    table_name = train_list_table
    db_schema = "AFC"
    
    # Write the DataFrame to the PostgreSQL table
    with alchemyEngine.connect() as conn:
        conn.autocommit = True
        for date, group in df_file.groupby('departure_date_short'):
            # Delete existing records for the current date
            try:
                delete_query = text(f"DELETE FROM \"{db_schema}\".{table_name} WHERE departure_date_short = \'{date}\'")
                conn.execute(delete_query)
                conn.commit()
                #prt_info(f"Previous data for {date} deleted successfully.")
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data for date {date}: {e}")
                prt_info("Trying to insert anyway...")
            
            # Insert the data for the current date
            try:
                num_rows = group.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                conn.commit()
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)


# In[100]:


def export_booking_payment(df_file, alchemyEngine):
    
    # Extract unique dates from the DataFrame
    dates = pd.to_datetime(df_file['operation_date_time'], format="%Y-%m-%d %H:%M").dt.date
    unique_dates = dates.unique()
    
    # variables of the ddbb
    table_name = bpd_table
    db_schema = "AFC"
    
    # Write the DataFrame to the PostgreSQL table
    with alchemyEngine.connect() as conn:
        conn.autocommit = True

        for date in unique_dates:
            group = df_file[dates == date]
            
            # Delete existing records for the current date
            try:
                delete_query = text(f"DELETE FROM \"{db_schema}\".{table_name} WHERE to_char(operation_date_time, 'yyyy-mm-dd') = \'{date}\'")
                conn.execute(delete_query)
                conn.commit()
                #prt_info(f"Previous data for {date} deleted successfully.")
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data for date {date}: {e}")
                prt_info("Trying to insert anyway...")
            
            # Insert the data for the current date
            try:
                num_rows = group.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                conn.commit()
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)


# In[101]:


def export_occupancy(df_file, alchemyEngine):
  
    # Extract unique dates from the DataFrame
    dates = pd.to_datetime(df_file['date']).dt.strftime('%Y-%m-%d')
    unique_dates = dates.sort_values().unique()
    today = datetime.date.today().strftime('%Y-%m-%d')
    
    # variables of the ddbb
    table_name = occupancy_table
    db_schema = "AFC"
    
    # Write the DataFrame to the PostgreSQL table
    with alchemyEngine.connect() as conn:
        conn.autocommit = True
    
        for date in unique_dates:
            group = df_file[dates == date]
            
            # Delete existing records for the current date
            try:
                delete_query = text(f"DELETE FROM \"{db_schema}\".{table_name} WHERE to_char(date, 'yyyy-mm-dd') = \'{date}\' and data_date = \'{today}\'")
                conn.execute(delete_query)
                #prt_info(f"Previous data for {date} deleted successfully.")
                conn.commit()
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data for date {date}: {e}")
                prt_info("Trying to insert anyway...")
            
            # Insert the data for the current date
            try:
                group.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                conn.commit()
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)


# In[ ]:


warnings.simplefilter("ignore")

files_found = {}

# get all xlsx files
for file in os.listdir("."):
    if file.endswith(".xlsx"):
        
        # check if it is a report
        kind_file = get_report_name(file)
        
        if(kind_file != NO_REPORT):
            # get the name
            if(kind_file == TRAIN_LIST_REPORT): kind_file_name = 'Train List'
            elif(kind_file == BOOKING_PAYMENT_REPORT): kind_file_name = 'Booking Payment Detailed'
            elif(kind_file == OCCUPANCY_REPORT): kind_file_name = 'Occupancy'
            else: kind_file_name = 'Unknown'
            prt_info(f"Found excel file {file} as {kind_file_name}")

            # add the file and the kind to the list
            if not kind_file_name in files_found:
                files_found[kind_file_name] = []
                
            files_found[kind_file_name].append(file)
        
        else: prt_info(f"Found file {file} but could not detect any report in it")

# export each report one by one
for report in files_found:
    df = pd.DataFrame()
    prt_info(f"Reading {report}...")
    
    # check all the files associated
    for file in files_found[report]:

        # reading files
        try:
            is_read = True
            if(report == 'Train List'): df_file = read_train_list(file, alchemyEngine)
            elif(report == 'Booking Payment Detailed'): df_file = read_booking_payment(file)
            elif(report == 'Occupancy'): df_file = read_occupancy(file)            
            else:
                prt_info(f"Reading of files {report} have not been implemented yet.")
                is_read = False

            if is_read:
                prt_info(f"{file} read.")
                df = pd.concat([df, df_file])
                
        except Exception as e:
            prt_info(e)
            prt_info(f"Reading of file {file} failed.")
            files_found[report].remove(file)
            continue
    
    # order the dataframe
    if(report == 'Train List'): sort_by = ['departure_date', 'operation_date_time']
    elif(report == 'Booking Payment Detailed'): sort_by = ['operation_date_time']
    elif(report == 'Occupancy'): sort_by = ['ticket_reserved', 'quota_configuration']
        
    df.sort_values(by= sort_by, ascending=True, inplace=True)

    # remove duplicates
    if(report == 'Train List'): subset_col = ['ticket_number']
    elif(report == 'Booking Payment Detailed'): subset_col = None
    elif(report == 'Occupancy'): subset_col = ['date', 'od','train_number', 'class']

    if subset_col is not None:
        duplicates = df.duplicated(subset=subset_col, keep='last')
        if(duplicates.sum() > 0):
            prt_info(f"Deleting {duplicates.sum()} duplicated entries.")
            df.drop_duplicates(subset=subset_col, keep='last', inplace=True, ignore_index=True)
        
    # export the valid files
    prt_info(f"Exporting {report}...")
    try:
        if(report == 'Train List'):
            export_train_list(df, alchemyEngine)
            prt_info(f"Report {report} exported successfully.")
        elif(report == 'Booking Payment Detailed'):
            export_booking_payment(df, alchemyEngine)
            prt_info(f"Report {report} exported successfully.")
        elif(report == 'Occupancy'):
            export_occupancy(df, alchemyEngine)
            prt_info(f"Report {report} exported successfully.")
        else:
            prt_info(f"Exportation of report {report} have not been implemented yet.")
    except Exception as e:
        prt_info(e)
        prt_info("Exportation failed. Exportation of the report aborted.")
        continue


prt_info("Exportation finished.")

