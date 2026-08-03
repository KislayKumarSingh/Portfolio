import traceback
from multiprocessing import Value
from multiprocessing import Process
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import math
from plyer import notification
from datetime import datetime, timedelta
import time
import Billing.Processes.Billing_Data_Analysis as bda
import Billing.Processes.Server_Connections as sc
import Billing.Processes.Telegram_Bot.Queries as q
import Billing.Processes.VIP_Duty_Data_Migration as vdm
import Billing.Processes.Vehicle_Offroad_Data_Migration as vodm
import Billing.Billing_Unfreeze_Data as bud
import Billing.Processes.Manual_to_GPS_Data_Migration as mtgdm

# region Code
con_dict = {
    'sql_188': (188, 'export', 'CPED_Master', 'sqlalchemy'),
    'sql_16': (16, 'export', 'Billing108', 'sqlalchemy'),
    'export_202': (202, 'export', 'REPORTS', 'sqlalchemy'),
    'export_71': (71, 'export', 'test', 'sqlalchemy'),
    'export_73': (73, 'export', 'test', 'sqlalchemy'),
    'export_17': (17, 'export', 'BILLING_ACTIVITIES_UP_WEST_PROD', 'sqlalchemy'),
    'export_204': (204, 'export', 'BILLING_ACTIVITIES_UP_PROD', 'sqlalchemy'),
    'conn_202': (202, 'run', 'REPORTS', 'mysql')
}

ENGINES = {}

def db_conn(server):
    ip, query_type, database, connection_type = con_dict[server]
    if connection_type == 'sqlalchemy':
        if server not in ENGINES:
            conn = sc.connection(ip, query_type, database)
            ENGINES[server] = create_engine(conn)
        return ENGINES[server]

    if connection_type == 'mysql':
        conn = sc.connection(ip, query_type, database)
        kwargs = {'user': conn[0], 'password': conn[1], 'host': conn[2], 'database': conn[3]}
        if sc.ip_address() != '192.168.86.164':
            kwargs['ssl_disabled'] = True
        return mysql.connector.connect(**kwargs)
    return None

def Manual_PCR_Data_202(start_date, end_date):
    # Manual PCR Data Migration from Server 188 to 202
    df = pd.read_sql(f'''
        select 
        case
            when substring(convert(varchar,[Incident ID]),5,1)=1 then 'East'
            else 'West'
        end as 'Cluster',
        [Incident Id] as Incident_ID,[Base Start ODO] as base_start_odo,[Scene Reach ODO] as pickup_reach_odo,[Hospital Reach ODO] as hsptl_reach_odo,
        [Base Reach ODO] as base_reach_odo,[Call Assigned Date & Time] as Ambulance_Assignment_Time,[Depatured Data & Time] as Ambulance_base_start_time,
        [Scene Arrival Date & Time] as Ambulance_pickup_point_reach_time,[Scene Departure Date & Time] as Ambulance_pickup_point_departure_time,
        [Hospital Arrival Date & Time] as Ambulance_destination_reach_time,[Hospital Departure Date & Time] as Ambulance_destination_depart_time,
        [Back2Base Date & Time] as Ambulance_base_reach_time
        from [CPED_Master].[dbo].[Manual_PCR_Data]
        where [Call Assigned Date & Time] between '{start_date} 00:00:00' and '{end_date} 23:59:59';
    ''', con=db_conn('sql_188'))
    delete_odo_query = f'''
        delete 
        from REPORTS.Billing_ODO_Timings 
        where Ambulance_Assignment_Time between '{start_date} 00:00:00' and '{end_date} 23:59:59';
    '''
    db_conn('export_202').execute(delete_odo_query)
    df.to_sql('Billing_ODO_Timings', db_conn('export_202'), if_exists='append', index=False)
    print("                  EAST & WEST - Manual PCR Data Migration :", len(df))

def East_202(start_date, end_date):
    # East Data Processing in Server 202
    conn_202 = db_conn('conn_202')
    east_cur_202 = None
    try:
        east_cur_202 = conn_202.cursor()
        east_cur_202.callproc('REPORTS.generate_raw_dataeast', args=(start_date, end_date))
        conn_202.commit()
        print('EAST - Committed Stage - 1')
        east_cur_202.callproc('REPORTS.generate_raw_data_finaleast')
        conn_202.commit()
        print('EAST - Committed Stage - 2')
        east_cur_202.callproc('REPORTS.raw_data_east')
        conn_202.commit()
        print('EAST - Committed Stage - 3')
    finally:
        if east_cur_202:
            east_cur_202.close()
        conn_202.close()

def West_202(start_date, end_date):
    # West Data Processing in Server 202
    conn_202 = db_conn('conn_202')
    west_cur_202 = None
    try:
        west_cur_202 = conn_202.cursor()
        west_cur_202.callproc('REPORTS.generate_raw_datawest', args=(start_date, end_date))
        conn_202.commit()
        print('                                     WEST - Committed Stage - 1')
        west_cur_202.callproc('REPORTS.generate_raw_data_finalwest')
        conn_202.commit()
        print('                                     WEST - Committed Stage - 2')
        west_cur_202.callproc('REPORTS.raw_data_west')
        conn_202.commit()
        print('                                     WEST - Committed Stage - 3')
    finally:
        if west_cur_202:
            west_cur_202.close()
        conn_202.close()

def East_71(east_row_count):
    # East Data Migration from Server 202 to 71
    db_conn('export_71').execute('delete from test.cad_raw_data_tmp;')
    df = pd.read_sql('select * FROM REPORTS.cad_raw_data_tmp_east;', con=db_conn('export_202'))
    east_row_count.value = len(df)
    df.to_sql("cad_raw_data_tmp", db_conn('export_71'), if_exists='append', index=False, chunksize=1000)
    print('East_71_Process completed')

def West_73(west_row_count):
    # West Data Migration from Server 202 to 73
    db_conn('export_73').execute('delete from test.cad_raw_data_tmp;')
    df = pd.read_sql('select * FROM REPORTS.cad_raw_data_tmp_west;', con=db_conn('export_202'))
    west_row_count.value = len(df)
    df.to_sql("cad_raw_data_tmp", db_conn('export_73'), if_exists='append', index=False, chunksize=1000)
    print('                                     West_73_Process completed')

def East_204():
    # East Data Migration from Server 202 to 204
    db_conn('export_204').execute('delete from BILLING_ACTIVITIES_UP_PROD.cad_raw_data_tmp;')
    df = pd.read_sql('SELECT * FROM REPORTS.cad_raw_data_tmp_east;', con=db_conn('export_202'))
    df.to_sql("cad_raw_data_tmp", db_conn('export_204'), if_exists='append', index=False, chunksize=1000)
    print('East_204_Process completed')

def West_17():
    # West Data Migration from Server 202 to 17
    db_conn('export_17').execute('delete from BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp;')
    df = pd.read_sql('SELECT * FROM REPORTS.cad_raw_data_tmp_west;', con=db_conn('export_202'))
    df.to_sql("cad_raw_data_tmp", db_conn('export_17'), if_exists='append', index=False, chunksize=1000)
    print('                                     West_17_Process completed')

def migration_query():
    # Data Migration query from Server 202 to 16
    return '''
        SELECT
        incident_id, callreferenceid, Cluster, vehicle_base_district, is_mci, case_type_name, creation_date, Level1_end_call_time,
        `Source of Distance`, map_distance, base_start_odo, pickup_reach_odo, hsptl_reach_odo, hsptl_depart_odo, base_reach_odo,
        base_to_scene_gps_km, scene_to_base_gps_km, scene_to_hsptl_gps_km, hsptl_to_base_gps_km, Total_gps_trip_kms, `Total Trip Kilometer`,
        ambulance_assignment_time, Ambulance_base_start_time, Ambulance_pickup_point_reach_time, Ambulance_pickup_point_departure_time,
        Ambulance_destination_reach_time, Ambulance_destination_depart_time, Ambulance_base_reach_time, Standard_remarks, Call_Type,
        Phone_no_of_the_Caller, beneficary_contact_number, benficiary_district, Gender, Age, vehicle_number, Destination_hospital,
        pilot_mobile_number, latitude, longitude, pcr_upload, Hyperlink_tab, update_from, `Response time`, `Delay in Response time`,
        DelayResponsetimeMinute, Hospital_category, at_hospital_gps_km, at_scene_gps_km, beneficary_trip_uad, avail_status_on_pcr,
        backup_vehicle_number, Beneficiary_name, Destination_district, emt_name, Pickup_Location, Name_of_the_caller, drift_status,
        SubEmergencyTypeName, last_modified_by, Vehicle_base_location
        FROM
    '''

def East_16():
    # East Data Migration from Server 202 to 16
    df = pd.read_sql(migration_query() + 'REPORTS.cad_raw_data_tmp_east;', con=db_conn('export_202'))
    df.to_sql("cad_raw_data", db_conn('sql_16'), if_exists='append', index=False)
    print('East_16_Process completed')

def West_16():
    # West Data Migration from Server 202 to 16
    df = pd.read_sql(migration_query() + 'REPORTS.cad_raw_data_tmp_west;', con=db_conn('export_202'))
    df.to_sql("cad_raw_data", db_conn('sql_16'), if_exists='append', index=False)
    print('                                     West_16_Process completed')

def main(start_date, end_date, buffer_minutes, migrate_cases='no', cases='', gps_manual_data='yes'):
    if sc.get_status() == 'Idle':
        try:
            sc.set_status('Busy')
            if q.online().find('OFFLINE') == -1:
                sync_time = q.sync()
                east_sync_time = datetime.strptime(sync_time[7:26], '%Y-%m-%d %H:%M:%S')
                west_sync_time = datetime.strptime(sync_time[34:53], '%Y-%m-%d %H:%M:%S')
                print('Start Date :', start_date, ', End Date :', end_date, ', East Last Sync Time :', east_sync_time, ', West Last Sync Time :', west_sync_time)
                buffer_time = datetime.now() - timedelta(minutes=buffer_minutes)
                if buffer_time > east_sync_time or buffer_time > west_sync_time:
                    time_difference = round(((datetime.now() - (east_sync_time if east_sync_time <= west_sync_time else west_sync_time)).total_seconds()) / 60)
                    return ('Last Sync Time is ' + str(time_difference) + ' Minutes old.' + '\n' +
                            'East : ' + str(east_sync_time) + '\n' + 'West : ' + str(west_sync_time))
                else:
                    start_time = time.mktime(time.localtime())
                    print('🔵 Billing Data Migration Started at : ' + str(time.strftime("%H:%M:%S", time.localtime())))
                    if gps_manual_data.lower() == 'yes':
                        # Manual PCR Data Migration from Server 188 to 202
                        try:
                            Manual_PCR_Data_202(start_date, end_date)
                        except:
                            print('🔴 Manual PCR Data Migration FAILED, Retrying...')
                            time.sleep(5)
                            Manual_PCR_Data_202(start_date, end_date)
                        # Manual to GPS Data Migration  (this should be after "Manual PCR Data Migration from Server 188 to 202")
                        mtgdm.main(start_date=start_date, end_date=end_date)

                    # VIP Duty Data Migration
                    vdm.main()
                    # Vehicle Offroad Data Migration
                    vodm.main()
                    # Deleting from CRD 16
                    if migrate_cases.lower() == 'yes':
                        delete_crd_query = f'''
                            delete 
                            from [Billing108].[dbo].[cad_raw_data]
                            where incident_id in ({cases});
                        '''
                    else:
                        delete_crd_query = f'''
                            delete 
                            from [Billing108].[dbo].[cad_raw_data] 
                            where ambulance_assignment_time between '{start_date} 00:00:00' and '{end_date} 23:59:59';
                        '''
                    db_conn('sql_16').execute(delete_crd_query)
                    # East Data Processing in Server 202
                    East_202_Process = Process(target=East_202, args=(start_date, end_date))
                    East_202_Process.start()
                    # West Data Processing in Server 202
                    West_202_Process = Process(target=West_202, args=(start_date, end_date))
                    West_202_Process.start()
                    # Waiting for West_202_Process to complete
                    West_202_Process.join()
                    # West Data Migration from Server 202 to 73
                    west_row_count = Value('i', 0)
                    West_73_Process = Process(target=West_73, args=(west_row_count,))
                    West_73_Process.start()
                    # West Data Migration from Server 202 to 17
                    West_17_Process = Process(target=West_17)
                    West_17_Process.start()
                    # West Data Migration from Server 202 to 16
                    West_16_Process = Process(target=West_16)
                    West_16_Process.start()
                    # Waiting for East_202_Process to complete
                    East_202_Process.join()
                    # East Data Migration from Server 202 to 71
                    east_row_count = Value('i', 0)
                    East_71_Process = Process(target=East_71, args=(east_row_count,))
                    East_71_Process.start()
                    # East Data Migration from Server 202 to 204
                    East_204_Process = Process(target=East_204)
                    East_204_Process.start()
                    # East Data Migration from Server 202 to 16
                    East_16_Process = Process(target=East_16)
                    East_16_Process.start()
                    # Waiting for West_17_Process to complete
                    West_17_Process.join()
                    # Waiting for West_16_Process to complete
                    West_16_Process.join()
                    # Waiting for West_73_Process to complete
                    West_73_Process.join()
                    # Waiting for East_16_Process to complete
                    East_16_Process.join()
                    # Delete UAD Cases
                    bud.delete_uad()
                    # Waiting for East_71_Process to complete
                    East_71_Process.join()
                    # Waiting for East_204_Process to complete
                    East_204_Process.join()

                    print('Billing Data Migration Completed at :', time.strftime("%H:%M:%S", time.localtime()))
                    east_cases = east_row_count.value
                    west_cases = west_row_count.value
                    print('East Cases :', east_cases, ', West Cases :', west_cases, ', Total Cases :', east_cases + west_cases)
                    end_time = time.mktime(time.localtime())
                    print("Total Time Taken :", math.ceil((end_time - start_time) / 60), "Minutes")

                    if (East_202_Process.exitcode == 0 and West_202_Process.exitcode == 0 and East_71_Process.exitcode == 0
                            and West_73_Process.exitcode == 0 and East_204_Process.exitcode == 0 and West_17_Process.exitcode == 0
                            and East_16_Process.exitcode == 0 and West_16_Process.exitcode == 0):
                        return 'Migration Completed.'
                    else:
                        return 'Migration FAILED.'
            else:
                return 'Server is OFFLINE.'
        finally:
            sc.set_status('Idle')
            for engine in ENGINES.values():
                engine.dispose()
            ENGINES.clear()
    else:
        return sc.running_status()
# endregion

if __name__ == "__main__":

    start_date = '2026-08-01'
    end_date = '2026-08-01'
    buffer_minutes = 10
    analysis = 'yes'

    # region Code

    # if select cases' migration is required then set migrate_cases='yes' and provide the cases below and update the first procedures
    # (REPORTS.generate_raw_dataeast, REPORTS.generate_raw_datawest) of data migration and provide the cases in "tci.incident_id IN ()"
    migrate_cases = 'no'    # default = 'no'
    # "gps_manual_data" parameter should be 'yes' if data is corrected by cped. It can be 'no' when "migrate_cases" parameter is 'yes'
    # and cases are not corrected by cped as we don't require gps to manual and manual to gps cases data to resolve anomaly.
    # This is done to prevent gps manual data conflict.
    gps_manual_data = 'yes'     # default = 'yes'
    cases = '''
    
    '''

    try:
        migration_status = main(start_date, end_date, buffer_minutes, migrate_cases, cases, gps_manual_data)
        print(migration_status, '\n')
        if migration_status == 'Migration Completed.' and analysis.lower() == 'yes':
            bda.main(start_date, end_date, 'No')
        notification.notify(title='Success', message='Data Migration Completed')
    except Exception:
        traceback.print_exc()
        notification.notify(title='Error', message='Data Migration Failed')
    # endregion
