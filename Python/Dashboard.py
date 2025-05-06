# streamlit run Dashboard.py
import streamlit as st
import pandas as pd
from datetime import datetime, time, timedelta
from sqlalchemy import create_engine
import sys

sys.path.append('C:/Users/singh_kislay/Documents/GitHub/GVK-EMRI/Python/Billing/Processes')
import Server_Connections as sc

# Page Configuration
favicon = 'Images/108 Logo.jpg'
st.set_page_config(page_title='108 Dashboard', layout="wide", page_icon=favicon)

# Fetching Data
@st.cache_data
def load_data():
    query = '''
        select Cluster,incident_id,ambulance_assignment_time,DelayResponsetimeMinute,case_type_name,Ambulance_base_start_time,Ambulance_base_reach_time
        from [Billing108].[dbo].[cad_raw_data];
    '''
    return pd.read_sql(query, con=create_engine(sc.connection(16)))

all_data_df = load_data()

# st.logo(image='Images/108 Logo.jpg', icon_image='Images/108 Logo.jpg')

# Sidebar
with st.sidebar:
    st.image('Images/108 Logo.jpg', 'Sense, Reach, Care, Follow-up', 180)
    st.header("⚙️ Filters")

    min_date = all_data_df['ambulance_assignment_time'].min().date()
    max_date = all_data_df['ambulance_assignment_time'].max().date()
    start_date = st.date_input("Start Date", value=min_date, min_value=all_data_df['ambulance_assignment_time'].min().date(), max_value=max_date)
    end_date = st.date_input("End Date", value=max_date, min_value=all_data_df['ambulance_assignment_time'].min().date(), max_value=max_date)

date_filter_df = all_data_df[(all_data_df['ambulance_assignment_time'] >= datetime.strptime(str(start_date) + ' 00:00:00', '%Y-%m-%d %H:%M:%S'))
                             & (all_data_df['ambulance_assignment_time'] <= datetime.strptime(str(end_date) + ' 23:59:59', '%Y-%m-%d %H:%M:%S'))]
east_date_filter_df = date_filter_df[date_filter_df['Cluster'] == 'EAST']
west_date_filter_df = date_filter_df[date_filter_df['Cluster'] == 'WEST']

# Format Number
def format_number(number):
    num_str = str(number)[::-1]
    parts = [num_str[:3]]
    for i in range(3, len(num_str), 2):
        parts.append(num_str[i:i + 2])
    return ",".join(parts)[::-1]

# Title
st.title(':ambulance: :red[108 Dashboard]')

# Cases
st.header(':blue[Cases]', divider='blue')
total_cases_col, east_cases_col, west_cases_col = st.columns(3)
total_cases_col.metric('✔️ Total', format_number(date_filter_df.shape[0]), help='Total availed cases.')
east_cases_col.metric('👉 East', format_number(east_date_filter_df.shape[0]), help='East availed cases.')
west_cases_col.metric('👈 West', format_number(west_date_filter_df.shape[0]), help='West availed cases.')

with st.expander('Datewise', icon='📅'):
    def datewise_cases(filter_df, heading):
        datewise_case_df = filter_df.groupby([filter_df['ambulance_assignment_time'].dt.date, 'case_type_name'])['incident_id'].count().reset_index()
        datewise_case_df.columns = ['Date', 'case_type_name', 'incident_id']
        datewise_case_table = pd.pivot_table(datewise_case_df, values='incident_id', index='Date', columns='case_type_name',
                                             aggfunc='sum', margins=True, margins_name=heading)
        datewise_case_table.columns = ['Emergency', 'IFT', heading]
        return datewise_case_table

    total_cases_ex_col, east_cases_ex_col, west_cases_ex_col = st.columns(3)
    total_cases_ex_col.dataframe(datewise_cases(date_filter_df, 'Total Cases'))
    east_cases_ex_col.dataframe(datewise_cases(east_date_filter_df, 'East Cases'))
    west_cases_ex_col.dataframe(datewise_cases(west_date_filter_df, 'West Cases'))

# RTNM
st.header(':blue[RTNM]', divider='blue')
total_rtnm = date_filter_df['DelayResponsetimeMinute'].sum() * 60
east_rtnm = date_filter_df[date_filter_df['Cluster'] == 'EAST']['DelayResponsetimeMinute'].sum() * 60
west_rtnm = date_filter_df[date_filter_df['Cluster'] == 'WEST']['DelayResponsetimeMinute'].sum() * 60
total_rtnm_col, east_rtnm_col, west_rtnm_col = st.columns(3)
total_rtnm_col.metric('✔️ Total', f'₹ {format_number(total_rtnm)}', help='Total RTNM amount.')
east_rtnm_col.metric('👉 East', f'₹ {format_number(east_rtnm)}', help='East RTNM amount.')
west_rtnm_col.metric('👈 West', f'₹ {format_number(west_rtnm)}', help='West RTNM amount.')

with st.expander('Datewise', icon='📅'):
    def datewise_rtnm(filter_df, heading):
        datewise_rtnm_df = filter_df.groupby([filter_df['ambulance_assignment_time'].dt.date, 'case_type_name'])['DelayResponsetimeMinute'].sum().reset_index()
        datewise_rtnm_df.columns = ['Date', 'case_type_name', 'Amount']
        datewise_rtnm_df['Amount'] = datewise_rtnm_df['Amount'] * 60
        datewise_rtnm_table = pd.pivot_table(datewise_rtnm_df, values='Amount', index='Date', columns='case_type_name',
                                             aggfunc='sum', margins=True, margins_name=heading)
        datewise_rtnm_table.columns = ['Emergency', 'IFT', heading]
        return datewise_rtnm_table

    total_rtnm_ex_col, east_rtnm_ex_col, west_rtnm_ex_col = st.columns(3)
    total_rtnm_ex_col.dataframe(datewise_rtnm(date_filter_df, 'Total RTNM'))
    east_rtnm_ex_col.dataframe(datewise_rtnm(east_date_filter_df, 'East RTNM'))
    west_rtnm_ex_col.dataframe(datewise_rtnm(west_date_filter_df, 'West RTNM'))

# Cycle Time
st.header(':blue[Cycle Time]', divider='blue')
def time_filter_df(start_hour, start_minute, start_second, end_hour, end_minute, end_second):
    return date_filter_df[(date_filter_df['ambulance_assignment_time'].dt.time >= time(start_hour, start_minute, start_second))
        & (date_filter_df['ambulance_assignment_time'].dt.time <= time(end_hour, end_minute, end_second))][['Ambulance_base_start_time', 'Ambulance_base_reach_time']]

morning_filter_df = time_filter_df(8, 0, 0, 11, 59, 59)
evening_filter_df = time_filter_df(17, 0, 0, 20, 59, 59)
non_peak_filter_df = pd.concat([time_filter_df(12, 0, 0, 16, 59, 59),
                                time_filter_df(21, 0, 0, 7, 59, 59)])
morning_avg = (morning_filter_df['Ambulance_base_reach_time'] - morning_filter_df['Ambulance_base_start_time']).dt.total_seconds().mean()
evening_avg = (evening_filter_df['Ambulance_base_reach_time'] - evening_filter_df['Ambulance_base_start_time']).dt.total_seconds().mean()
non_peak_avg = (non_peak_filter_df['Ambulance_base_reach_time'] - non_peak_filter_df['Ambulance_base_start_time']).dt.total_seconds().mean()

morning_cycle_time_col, evening_cycle_time_col, non_peak_cycle_time_col = st.columns(3)
morning_cycle_time_col.metric('☀️️ Morning Peak Hours', str(timedelta(seconds=round(morning_avg))), help='Average cycle time from 8 AM to 12 PM.')
evening_cycle_time_col.metric('🌙 Evening Peak Hours', str(timedelta(seconds=round(evening_avg))), help='Average cycle time from 5 PM to 9 PM.')
non_peak_cycle_time_col.metric('🌅 Non-Peak Hours', str(timedelta(seconds=round(non_peak_avg))), help='Average cycle time from 12 PM to 5 PM & 9 PM to 8 AM.')