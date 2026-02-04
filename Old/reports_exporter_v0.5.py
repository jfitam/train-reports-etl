#!/usr/bin/env python
# coding: utf-8

# In[16]:


# V 0.1 
# basic functionalities. read and export Train List, Occupancy, and Booking Payment Detailed
# 
# V 0.2 
# handle all the information of each kind of report together. 
#
# v 0.3
# add general logging
# delete all the information in the same query
# added importation by chunks
# added information of the process of each day
#
# v 0.4
# correct a bug to insert properly insertion entry to audit table
# add an error log and alert window to alert for errors
# categorize all the message printed by their kind
# 
# v 0.41
# fixed bug missing some logging level information during exportation
# added message when deleting the day
# fixed minor bug regarding the number of entries inserted that are shown in screen
#
# v 0.5
# extend the number of rows to be checked for the header to 50
# delete incorrect rows at the end
# fixed minor bug with the exporting
# read all the sheets of an excel file
# improve the detection of the wrong lines at the end. saves them in file
# compress result as zip
# unify timestamps
# arrange output in folders
# save duplicates in file


# In[17]:


# connection to the database
# import psycopg2
from sqlalchemy import create_engine, text

# Create an engine instance
alchemyEngine = create_engine("postgresql+psycopg2://postgres:Renfe2022@172.19.28.174:5433/SalesSystem", pool_recycle=5);


# In[18]:


# import 
import pandas as pd
import openpyxl
# import numpy as np
from numpy import sort as np_sort
import datetime
import os
import warnings
import sys
import logging
import tkinter as tk
from tkinter import messagebox
import shutil

# tkinter
root = tk.Tk()
root.withdraw()  # Hide the root window

# STATES
NO_REPORT = 0
TRAIN_LIST_REPORT = 1
OCCUPANCY_REPORT = 2
BOOKING_PAYMENT_REPORT = 3

# ERRORS FOUND
errors_found = False

# timestamp for all the records
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# folders
log_folder = "log"
export_folder = "export"
data_folder = "data"

# tables
train_list_table = 'train_list'
occupancy_table = 'occupancy_list_hist'
bpd_table = 'booking_payment_detailed'

#train_list_table = 'train_list_test'
#occupancy_table = 'occupancy_list_hist_test'
#bpd_table = 'booking_payment_detailed_test'


# In[19]:


def prt_info(string, kind=logging.INFO, nl=True):
    global errors_found
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Get the current time
    info = f"[{current_time}] {string}"

    info = '\r'+info

    # log the message in the different loggers
    log.log(kind, info)
    
    # record there is error or warning to set the alert window at the end
    if kind >= logging.WARNING:
        error_log.log(kind, info)
        errors_found = True

    # Print the information
    print(info, end='' if not nl else '\n')


# In[20]:


# set up logging
log_name = 'exportation_' + current_time + '.log'

# create the directory if not exist
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log = logging.getLogger("log_general")
log.setLevel(logging.INFO) 
general_handler = logging.FileHandler(f"{log_folder}/{log_name}", mode='w')
general_handler.setLevel(logging.INFO)
log.addHandler(general_handler)

error_log = logging.getLogger("log_error")
error_handler = logging.FileHandler(f"{log_folder}/error_{log_name}", mode='w')
error_handler.setLevel(logging.WARNING)
error_log.addHandler(error_handler)

#debug_log = logging.getLogger("log_debug")
#debug_handler = logging.FileHandler("debug_" + log_name, mode='w')
#debug_handler.setLevel(logging.DEBUG)
# debug_log.addHandler(debug_handler)


# In[21]:


# version control
version = 0.5
max_control = 0

# check if the current version is the last one
query = "SELECT version FROM \"AFC\".exporter_version_control"
all_versions = pd.read_sql_query(query, alchemyEngine)
max_version = all_versions['version'].max()

try:
    if version > max_version:
        # add the new version
        with alchemyEngine.connect() as conn:
            query = text(f"insert into \"AFC\".exporter_version_control(date, version) values (\'{datetime.datetime.now()}\',\'{version}\')")
            conn.execute(query)
            conn.commit()
        
    elif version < max_version:
        # this program is out of date, terminating execution
        prt_info("Current exporter is out of date. Please, use the last version to export data.", logging.ERROR)
        sys.exit()

    #else:
        #current is the last version. Do nothing

except Exception as e:
    prt_info("The version could not be checked in the database", logging.ERROR)




# In[22]:


# function that detects which kind of report is the excel file
def get_report_name(excel_file_path, sheet=0):

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
        file_header = pd.read_excel(excel_file_path, sheet_name=sheet, nrows=50, header=None)
    except Exception as e:
        prt_info(f"There is a problem reading the file: {e}", kind=logging.ERROR)
        return 0, NO_REPORT
    
    # go through the read part of the file
    for index, row in enumerate(file_header.values):
        # clean
        row = row[pd.notnull(row)]
        row = pd.DataFrame(row)
        
        # comparision
        if(row.equals(train_list_header)): return index + 1, TRAIN_LIST_REPORT
        elif(row.equals(occupancy_header)): return index + 1, OCCUPANCY_REPORT
        elif(row.equals(bpd_header)): return index + 1, BOOKING_PAYMENT_REPORT
            
    # no report found
    return 0, NO_REPORT


# In[23]:


def read_train_list(file_name, alchemyEngine, sheet=0):

    global current_time
    
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
    
    # get the first line of the report
    first_row, name_report = get_report_name(file_name, sheet)
    
    if (name_report != TRAIN_LIST_REPORT):
        raise Exception(f"Wrong function invoked for sheet '{sheet}' from '{file_name}'")
        
    #read the file
    try:
        # open file
        df_file = pd.read_excel(file_name, header=0, sheet_name=sheet, skiprows=(first_row-1), dtype=str
                                #,                                 
                                #parse_dates=['Departure Date', 'Validation Time'],
                                #date_format={'Departure Date': '%Y-%m-%d %H:%M', 'Validation Time': '%Y-%m-%d  %H:%M'}
                               )
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")

    #delete empty columns
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    
    # format date columns
    date_cols = []
    for col in date_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d')
    
    # format datetime columns
    datetime_cols = ['Departure Date','Validation Time']
    for col in datetime_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    # format numeric columns
    num_cols = [
        'VAT Base Price',
        'Management Fee',
        'VAT Management Fee',
        'Payment Fee',
        'VAT Payment Fee',
        'Operation Amount',
        'Penalty Tariff',
        'Amount Not Refunded']
    
    for col in num_cols:
        df_file[col] = pd.to_numeric(df_file[col], errors='coerce')
    
    # turn nullable columns to empty space
    nullable_cols = ['Validation Time', 
                     'Special Needs', 
                     'Payment Mode', 
                     'Media Type',
                    'Penalty Tariff',
                    'Amount Not Refunded',
                    'Compensation Type',
                    'Compensation Reason',
                    'Compensation Status',
                    'Nationality',
                    'Special Needs',
                    'Last Operation Equipment Code',
                    'Group',
                    'Checked On Board']
    
    # check wrong lines. delete all nan or nat and save them in a separate file
    df_nan = df_file.dropna(inplace=True, how='any', subset=df_file.columns.difference(nullable_cols))
    if df_nan is not None:
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)
        df_nan.to_csv(f"{export_folder}/Train List error rows {current_time}.csv.zip")
    
    #check if there are still data entries
    if df_file.shape[0] == 0:
        raise Exception(f"Empty dataset after cleaning.")   
    
    # create extra columns
    df_file['Train_hour'] = df_file['Departure Date'].dt.strftime('%H:%M')
    df_file['Departure_Date_Short'] = df_file['Departure Date'].dt.strftime('%Y-%m-%d')
    df_file['Train-OD Short'] = df_file['Train Number'] + " - " + df_file['OD']
    df_file['CORRIDOR'] = df_file['Train Number'].str[:2]
    df_file['WEEK_DAY'] = df_file['Departure Date'].dt.strftime('%a')
    df_file['WEEK_NUM'] = df_file['Departure Date'].dt.isocalendar().week
    df_file['train_key'] = df_file['Departure_Date_Short'] + " - " + df_file['Train-OD Short']
    
    
    # get the train departure
    try:
        train_hours = pd.read_sql_table('train_departure_times', alchemyEngine, schema='AFC')
    except Exception as e:
        raise(f"Error fetching the departure times from database: {e}")
        
    train_hours.columns = ['Train Number', 'train_departure_date_time']
    df_file = pd.merge(df_file, train_hours, on="Train Number", how="left")
    
    #check if there is missing hours for the train numbers of this file
    if(df_file['train_departure_date_time'].isnull().sum() > 0):
        trains_missing = df_file['Train Number'][df_file['train_departure_date_time'].isnull()].unique()
        raise Exception(f"There are missing departing hours in the database. Please, check the following trains: {', '.join(trains_missing)}")
    
    # calculate the departing time of the train
    df_file['train_departure_date_time'] = pd.to_datetime(df_file['Departure_Date_Short'].astype(str) + " " + df_file['train_departure_date_time'].astype(str))
    train_date_adjustment = df_file['train_departure_date_time'].dt.time > df_file['Departure Date'].dt.time
    df_file['train_departure_date_time'] = df_file['train_departure_date_time'] - pd.to_timedelta(train_date_adjustment.astype(int), unit="D")
    df_file['train_departure_date_short'] = df_file['Departure Date'].dt.date - pd.to_timedelta(train_date_adjustment.astype(int), unit="D")
    
    # calculate the services date (reduce one day if it is an early train before maintenance window)
    service_date_adjustment = df_file['train_departure_date_time'].dt.time <= datetime.time(5, 0)
    df_file['Service_Date'] = df_file['train_departure_date_short'] - pd.to_timedelta(service_date_adjustment.astype(int), unit="D")
    
    # get the date time of the operation
    ticket_numbers = ", ".join(f"'{ticket}'" for ticket in df_file['Ticket Number'].unique())
    query = f"""
    SELECT ticket_number AS \"Ticket Number\", operation_date_time
    FROM \"AFC\".{bpd_table}
    WHERE ticket_number IN ({ticket_numbers})
    """
    df_operation_date_times = pd.read_sql_query(query, alchemyEngine)
    df_file = pd.merge(df_file, df_operation_date_times, on="Ticket Number", how="left")
    df_file['operation_date'] = pd.to_datetime(df_file['operation_date_time'], errors='coerce', format='%Y-%m-%d %H:%M:%S').dt.strftime("%Y-%m-%d")
    
    # transform columns date and datetime to text
    for col in datetime_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d %H:%M')
        
    for col in date_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d')
    
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



# In[24]:


def read_booking_payment(file_name, sheet=0):

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
        #amount no refunded
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

    # get the first line of the report
    first_row, name_report = get_report_name(file_name, sheet)

    if (name_report != BOOKING_PAYMENT_REPORT):
        raise Exception(f"Wrong function invoked for sheet '{sheet}' from '{file_name}'")
    
    # read
    try:
        df_file = pd.read_excel(file_name, header=0, sheet_name=sheet, skiprows=(first_row-1), dtype=str)
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")

    #delete empty columns
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
    
    # format date columns
    date_cols = []
    for col in date_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d')
    
    # format datetime columns
    datetime_cols = ['Operation Date','Departure Date','Arrival Date']
    for col in datetime_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    # format numeric columns
    num_cols = [
        'Base Price',
        'VAT Base Price',
        'Management Fee',
        'VAT Management Fee',
        'Payment Fee',
        'VAT Payment Fee',
        'Operation Amount',
        'Penalty Tariff']
    
    for col in num_cols:
        df_file[col] = pd.to_numeric(df_file[col], errors='coerce')
    
    # turn nullable columns to empty space
    nullable_cols = [
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
       'Reserved Number of Seats',
       'Card Serial Number',
       'Card User Name',
       'Sales Station',
       'Sales Equipment Code',
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
       'Last Operation Equipment Code']
    
    # check wrong lines. delete all nan or nat and save them in a separate file
    df_nan = df_file.dropna(inplace=True, how='any', subset=df_file.columns.difference(nullable_cols))
    if df_nan is not None:
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)
        df_nan.to_csv(f"{export_folder}/Booking Payment Detailed error rows {current_time}.csv.zip")
    
    #check if there are still data entries
    if df_file.shape[0] == 0:
        raise Exception(f"Empty dataset after cleaning.")   

    # transform columns date and datetime to text
    for col in datetime_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d %H:%M')
        
    for col in date_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d')
        
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
        #'amount_not_refunded', 
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


# In[25]:


def read_occupancy(file_name, sheet=0):    
    
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

    # get the first line of the report
    first_row, name_report = get_report_name(file_name, sheet)

    if (name_report != OCCUPANCY_REPORT):
        raise Exception(f"Wrong function invoked for sheet '{sheet}' from '{file_name}'")
        
    try:
        df_file = pd.read_excel(file_name, header=0, skiprows=(first_row-1), sheet_name=sheet, dtype=occupancy_datatype, parse_dates=['Date'], date_format={'Date':'%Y-%m-%d %H:%M:%S'})
    except Exception as e:
        raise Exception(f"Error opening the file: {e}")

    #delete empty columns
    df_file = df_file.loc[:, ~df_file.columns.str.contains('^Unnamed')]
                
    # format date columns
    date_cols = []
    for col in date_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d')
    
    # format datetime columns
    datetime_cols = ['Date']
    for col in datetime_cols:
        df_file[col] = pd.to_datetime(df_file[col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    # format numeric columns
    num_cols = []
    
    for col in num_cols:
        df_file[col] = pd.to_numeric(df_file[col], errors='coerce')
    
    # turn nullable columns to empty space
    nullable_cols = [
        'Origin Station',
        'Destination Station',
        'Train ID',
        'Total Seats (Quota + Carer + PRM)',
        'Total Locks (Quota + Carer + PRM)',
        'For Sale',
        'Reserved Usual Seats',
        'Reserved PRM Seats',
        'Reserved Carer Seats',	
        'Reserved & Lock Usual Seats',
        'Reserved & Lock PRM Seats',
        'Reserved & Lock Carer Seats',	
        'Total Available',
        'Validating',
        'No Show',
        'UnBooked',	
        'Passengers Inc. Infants',
        'Checked On Board']
    
    # check wrong lines. delete all nan or nat and save them in a separate file
    df_nan = df_file.dropna(inplace=True, how='any', subset=df_file.columns.difference(nullable_cols))
    if df_nan is not None:
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)
        df_nan.to_csv(f"{export_folder}/Occupancy error rows {current_time}.csv.zip")
    
    #check if there are still data entries
    if df_file.shape[0] == 0:
        raise Exception(f"Empty dataset after cleaning.")
    
    # transform columns date and datetime to text
    for col in datetime_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d %H:%M')
        
    for col in date_cols:
        df_file[col] = df_file[col].dt.strftime('%Y-%m-%d')
        
    # create the extra columns
    df_file['Data_Date'] = datetime.date.today()
    df_file['train_key'] = df_file['Date'] + " - " + df_file['Train Number'] + " - " + df_file['OD']
    
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


# In[26]:


# get each pair of dates (beginning, end) of the streak days in the submitted list of dates
def get_date_pairs(df, column):
    try:
        date_pairs = []
        date_col = np_sort(pd.to_datetime(df[column]).dt.date.unique())
        date_begin = date_col.min()
        date_end = date_col.min()
        day_count = (date_col.max() - date_col.min()).days + 1
        
        # if there is a single date, return that
        if(len(date_col) == 1):
            return [[date_col.min(), date_col.max()]]
        
        # iterate through the dates
        for d in date_col:
            #skip the first date
            if(d == date_col.min()): continue
        
            # check if it is continous
            if((d - date_end).days == 1):
                date_end = d
            else:
                date_pairs.append([date_begin.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d')])
                date_begin = d
                date_end = d
        #at the end insert the last element
        if(date_begin is not None):
            date_pairs.append([date_begin.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d')])
            
    
        return date_pairs
    
    except Exception as e: 
        raise Exception(f"There was an error while reading the dates of the report: {e}", logging.ERROR)


# In[27]:


def export_train_list(df_file, alchemyEngine):
    
    # Extract unique dates from the DataFrame
    unique_dates = df_file['departure_date_short'].unique()
    date_conditions = ', '.join([f"'{date}'" for date in unique_dates])
    
    # variables of the ddbb
    table_name = train_list_table
    db_schema = "AFC"
    
    # Write the DataFrame to the PostgreSQL table
    with alchemyEngine.connect() as conn:
        
        # get the dates of the record
        date_pairs = get_date_pairs(df_file, 'departure_date_short')

        # notice a warning if there are missing dates in the middle of the data
        if(len(date_pairs) >1):
            prt_info("The dates on the report Train List are not consecutive. Make sure all the files of the day has been submitted", kind=logging.WARNING)
        
        # delete the previous records
        for date_from, date_to in date_pairs:
            try:
                delete_query = text(f"DELETE FROM \"{db_schema}\".{table_name} WHERE departure_date_short between \'{date_from}\' and \'{date_to}\'")
                conn.execute(delete_query)
                conn.commit()
                prt_info(f"Previous data from {date_from} to {date_to} deleted successfully.")
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data from {date_from} to {date_to} {e}", kind=logging.ERROR)

        # insert the data day by day
        for date, group in df_file.groupby('departure_date_short'):
            # Insert the data for the current date
            chunk_size = 500
            try:
                for chunk in range(0, len(group), chunk_size):
                    df_chunk = group.iloc[chunk:chunk + chunk_size]
                    df_chunk.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                    conn.commit()
                    if ((chunk+chunk_size)<len(group)): prt_info(f"Data for {date}: {chunk+chunk_size} entries inserted.", kind=logging.DEBUG, nl=False)
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)
            conn.commit()


# In[28]:


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
                prt_info(f"Previous data for {date} deleted successfully.")
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data for date {date}: {e}", kind=logging.ERROR)
            
            # Insert the data for the current date
            chunk_size = 500
            try:
                for chunk in range(0, len(group), chunk_size):
                    df_chunk = group.iloc[chunk:chunk + chunk_size]
                    df_chunk.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                    conn.commit()
                    if ((chunk+chunk_size)<len(group)): prt_info(f"Data for {date}: {chunk+chunk_size} entries inserted.", kind=logging.DEBUG, nl=False)
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)
            conn.commit()


# In[29]:


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
        # get the dates of the record
        date_pairs = get_date_pairs(df_file, 'date')

        # notice a warning if there are missing dates in the middle of the data
        if(len(date_pairs) >1):
            prt_info("The dates on the report Occupancy are not consecutive. Make sure all the files of the day has been submitted", kind=logging.WARNING)
        
        # delete the previous records
        for date_from, date_to in date_pairs:
            try:
                delete_query = text(f"DELETE FROM \"{db_schema}\".{table_name} WHERE to_char(date, 'yyyy-mm-dd') between \'{date_from}\' and \'{date_to}\'and data_date = \'{today}\'")
                conn.execute(delete_query)
                conn.commit()
                prt_info(f"Previous data from {date_from} to {date_to} deleted successfully.")
            except Exception as e:
                conn.rollback()
                prt_info(f"An error occurred while deleting the previous data from {date_from} to {date_to}: {e}", kind=logging.ERROR)
    
        for date in unique_dates:
            group = df_file[dates == date]
            
            # Insert the data for the current date
            chunk_size = 500
            try:
                for chunk in range(0, len(group), chunk_size):
                    df_chunk = group.iloc[chunk:chunk + chunk_size]
                    df_chunk.to_sql(table_name, conn, schema=db_schema, if_exists='append', index=False)
                    conn.commit()
                    if ((chunk+chunk_size)<len(group)): prt_info(f"Data for {date}: {chunk+chunk_size} entries inserted.", kind=logging.DEBUG, nl=False)
                prt_info(f"Data for {date} inserted successfully ({group.shape[0]} inserted).")
            except Exception as e:
                conn.rollback()
                raise Exception(f"An error occurred while inserting data for {date}: {e}")
    
            #register the audit table
            audit_query = text(f"INSERT INTO \"AFC\".audit(timestamp, \"table\", operation, period, \"user\") VALUES (\'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\', \'{table_name}\', \'insert\', \'{date}\', \'{os.getlogin()}\')")
            conn.execute(audit_query)
            conn.commit()


# In[30]:


# ************************************* MAIN PROGRAM *****************************************************
warnings.simplefilter("ignore")

prt_info(f"\n**************** Exporter version {version} ***********************")
prt_info(f"Date {datetime.datetime.today()}")
files_found = {}

# get all xlsx files
for file in os.listdir("."):
    if file.endswith(".xlsx"):
        # check all the sheets on the report
        excel_file = pd.ExcelFile(file)

        # loop over the sheets
        for sheet_name in excel_file.sheet_names:
            # check if it is a report
            first_row, kind_file = get_report_name(file, sheet_name)
            
            if(kind_file != NO_REPORT):
                # get the name
                if(kind_file == TRAIN_LIST_REPORT): kind_file_name = 'Train List'
                elif(kind_file == BOOKING_PAYMENT_REPORT): kind_file_name = 'Booking Payment Detailed'
                elif(kind_file == OCCUPANCY_REPORT): kind_file_name = 'Occupancy'
                else: kind_file_name = 'Unknown'
                prt_info(f"Found sheet '{sheet_name}' from '{file}' as {kind_file_name}")
    
                # add the file and the kind to the list
                if not kind_file_name in files_found:
                    files_found[kind_file_name] = []
                    
                if [file, sheet_name] not in files_found[kind_file_name]:
                    files_found[kind_file_name].append([file, sheet_name])
            
            else: prt_info(f"No report found in sheet '{sheet_name}' from '{file}'", kind=logging.WARNING)

        # Close the Excel file
        excel_file.close()

# if there are no files, exit
if(len(files_found) == 0):
    prt_info("No valid files found. Exiting program", logging.WARNING)
    sys.exit()

# export each report one by one
for report in files_found:
    df = pd.DataFrame()
    prt_info(f"Reading {report}...")
    
    # check all the files associated
    for file, sheet_name in files_found[report]:
        # reading sheet
        try:
            is_read = True
            if(report == 'Train List'): df_file = read_train_list(file, alchemyEngine=alchemyEngine, sheet=sheet_name)
            elif(report == 'Booking Payment Detailed'): df_file = read_booking_payment(file, sheet=sheet_name)
            elif(report == 'Occupancy'): df_file = read_occupancy(file, sheet=sheet_name)            
            else:
                prt_info(f"Reading of files {report} have not been implemented yet.", logging.WARNING)
                is_read = False

            if is_read:
                prt_info(f"Sheet '{sheet_name}' from '{file}' read.")
                df = pd.concat([df, df_file])
                
        except Exception as e:
            prt_info(f"Reading of sheet '{sheet_name}' from '{file}' failed: {e}", logging.ERROR)
            files_found[report].remove([file, sheet_name])
            continue

    # if there is data
    if df.empty:
        prt_info(f'No data to export for report {report}', logging.WARNING)
    else:
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
                df_duplicates = df.drop_duplicates(subset=subset_col, keep='last', inplace=True, ignore_index=True)
                
                if not df_duplicates is None:
                    if os.path.exists(export_folder):
                        os.makedirs(export_folder)
                        
                    df_duplicates.to_csv(f"{export_folder}/{report} duplicates {current_time}.csv.zip")
            
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
                prt_info(f"Exportation of report {report} have not been implemented yet.", kind=logging.WARNING)
        except Exception as e:
            prt_info(e)
            prt_info("Exportation failed. Exportation of the report aborted.", kind=logging.ERROR)
            continue

        # save the results
        # Check if the directory exists, and if not, create it
        if not os.path.exists(export_folder):
            os.makedirs(export_folder)
        df.to_csv(f"{export_folder}/{report} data exported {current_time}.csv.zip", index=False, encoding='utf-8')


# move the original files to a subdirectory to arrange the output
if len(files_found) > 0:
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    for report in files_found:
        for file, sheet in files_found[report]:
            if os.path.exists(file):
                shutil.move(file, data_folder)


# finish
prt_info("Exportation finished.")

error_file = error_handler.baseFilename
logging.shutdown()

# if there is an error, generate an alert window
if(errors_found):
    messagebox.showinfo("Alert", "There were ERRORS or WARNINGS during the exportation process. Please check file error_" + log_name + " for details.")
else:
    # delete the error log
    if os.path.exists(error_file):
        if os.stat(error_file).st_size == 0:
            os.remove(error_file)

    messagebox.showinfo("Alert", "The exportation process has been completed SUCCESSFUL.")
    

