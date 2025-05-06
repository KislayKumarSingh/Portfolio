from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import Billing.Processes.Server_Connections as sc

def user_time_validation(user_id):
    if user_id in [1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890]:
        start_time = datetime.strptime('08:05:00', '%H:%M:%S').time()
        end_time = datetime.strptime('08:40:00', '%H:%M:%S').time()
        current_time = datetime.now().time()
        if start_time <= current_time <= end_time:
            return False, 'Bot can\'t be used from 8:05 AM to 8:40 AM.'
        else:
            return True, 'Success.'
    else:
        return False, 'Invalid User. User ID - ' + str(user_id)

def help_commands():
    return '''
<b>Commands of Bot:-</b>
<b>/migrate</b> [start date] [end date] [buffer minutes(optional)]
Description - Migrate billing data.
Example - /migrate 2023-09-15 2023-09-30 20

<b>/freeze</b> [start date] [end date] [last modified date] [scope(optional,default 0)]
Description - Freeze billing data.
    scope = 0(IT IS,RTNM Desk,ERC), 
                   1(IT IS,RTNM Desk), 2(IT IS)
Example - /freeze 2023-09-15 2023-09-20 
        2023-09-21 11:30:00 1

<b>/unfreeze</b> [reason] [cases]
Description - Unfreeze billing data.
Example - /unfreeze Correction
            20231090233664,
            20231090234706,
            20231090234706

<b>/online</b>
Description - Get Online Status of Server.
Example - /online

<b>/sync</b>
Description - Get Last Sync Time of data in 202 Server.
Example - /sync

<b>/analysis</b> [start date] [end date] [send email]
Description - Send Billing Data Analysis.
Example - /analysis 2023-09-15 2023-09-30 Yes

<b>/rtnm</b> [start date] [end date]
Description - Get RTNM Amount.
Example - /rtnm 2023-09-15 2023-09-30

<b>/kpi_govind</b> [start date] [end date]
Description - Send Govind's KPI report.
Example - /kpi_govind 2023-09-15 2023-09-30

<b>/nrf</b> [cases]
Description - Send No Record Found cases.
Example - /nrf 20231090233664,
            20231090234706,
            20231090234706

<b>/kma</b> [nrf email]
Description - Unlock KMs Activity Cases.
Example - /kma yes

<b>/scheduler</b>
Description - Run Scheduler manually.
Example - /scheduler

<b>/pending</b> [start date] [end date] [unfreeze_nrf(optional)]
Description - Send Pending Cases of CPED & RTNM Desk.
Example - /pending 2023-09-16 2023-09-17 no

<b>/uad</b> [start date] [end date]
Description - Get Pending UAD Cases.
Example - /uad 2023-09-16 2023-09-17

<b>/missing</b> [start date] [end date]
Description - Get Pending Cases to be freezed.
Example - /missing 2023-09-16 2023-09-17

<b>/kmd</b> [start date] [end date]
Description - Delete CPED KMs Activity cases.
Example - /kmd 2023-09-16 2023-09-17
'''

def date_order(start_date, end_date):
    if datetime.strptime(start_date, '%Y-%m-%d').date() <= datetime.strptime(end_date, '%Y-%m-%d').date():
        return True, 'Success.'
    else:
        return False, 'Start Date is greater than End Date.'

def date_duration(start_date, end_date):
    if (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days <= 31:
        return True, 'Success.'
    else:
        return False, 'Duration can\'t be more than 1 month.'

def validations(user_id, start_date, end_date):
    val_status, val_message = user_time_validation(user_id)
    if val_status:
        do_status, do_message = date_order(start_date, end_date)
        if do_status:
            dd_status, dd_message = date_duration(start_date, end_date)
            if dd_status:
                return True
            else:
                return dd_message
        else:
            return do_message
    else:
        return val_message

def online():
    def status(server, table):
        df = pd.read_sql('SELECT COUNT(*) FROM ' + table, con=create_engine(sc.connection(server)))
        if df.loc[0].values[0] >= 0:
            return 'Online : ' + str(server)

    def offline(server):
        return 'OFFLINE : ' + str(server)

    try:
        stat_71 = status(71, 'test.freeze_ids')
    except:
        stat_71 = offline(71)

    try:
        stat_73 = status(73, 'test.freeze_ids')
    except:
        stat_73 = offline(73)

    try:
        stat_202 = status(202, 'REPORTS.freeze_ids')
    except:
        stat_202 = offline(202)

    try:
        stat_17 = status(17, 'REPORTS.freeze_ids')
    except:
        stat_17 = offline(17)

    try:
        stat_16 = status(16, '[Billing108].[dbo].[Billing Process Queries List]')
    except:
        stat_16 = offline(16)

    return stat_71 + '\n' + stat_73 + '\n' + stat_202 + '\n' + stat_17 + '\n' + stat_16

def sync():
    east = pd.read_sql('SELECT MAX(t.creation_date) FROM CAD_UP_PROD.t_cad_incident t;', con=create_engine(sc.connection(202)))
    west = pd.read_sql('SELECT MAX(t.creation_date) FROM CAD_UP_WEST_PROD.t_cad_incident t;', con=create_engine(sc.connection(202)))
    return 'East : ' + str(east.iat[0, 0]) + '\n' + 'West : ' + str(west.iat[0, 0])

def rtnm(start_date, end_date):
    query = 'select * from rtnm(\'' + start_date + '\', \'' + end_date + '\') order by [Date];'
    return pd.read_sql(query, con=create_engine(sc.connection(16))).to_string(index=False, header=False)