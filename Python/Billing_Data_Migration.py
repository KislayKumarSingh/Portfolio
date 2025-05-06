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
import Billing.Billing_Data_Analysis as bda
import Billing.Processes.Server_Connections as sc
import Billing.Telegram_Bot.Queries as q
import Billing.Processes.VIP_Duty_Data_Migration as vdm
import Billing.Processes.Vehicle_Offroad_Data_Migration as vodm
import Billing.Billing_Unfreeze_Data as bud
import Billing.Processes.Manual_to_GPS_Data_Migration as mtgdm

# region Code
sql_188 = create_engine(sc.connection(188, 'export', 'CPED_Master'))
sql_16 = create_engine(sc.connection(16, 'export', 'Billing108'))
export_202 = create_engine(sc.connection(202, 'export', 'REPORTS'))
export_71 = create_engine(sc.connection(71, 'export', 'test'))
export_73 = create_engine(sc.connection(73, 'export', 'test'))
export_17 = create_engine(sc.connection(17, 'export', 'BILLING_ACTIVITIES_UP_WEST_PROD'))
run_202 = sc.connection(202, 'run', 'REPORTS')
conn_202 = mysql.connector.connect(user=run_202[0], password=run_202[1], host=run_202[2], database=run_202[3])

def Manual_PCR_Data_202(start_date, end_date):
    print("                  EAST & WEST - Manual PCR Data Migration Started at :", time.strftime("%H:%M:%S", time.localtime()))
    df = pd.read_sql('''
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
        where [Call Assigned Date & Time] between \'''' + start_date + ''' 00:00:00\' and \'''' + end_date + ''' 23:59:59\';
    ''', con=sql_188)
    print("                                Rows Read :", df.shape[0])
    export_202.execute('delete from REPORTS.Billing_ODO_Timings where Ambulance_Assignment_Time between \'' + start_date + ' 00:00:00\' and \'' + end_date + ' 23:59:59\';')
    df.to_sql('Billing_ODO_Timings', export_202, if_exists='append', index=False)
    print("                  EAST & WEST - Manual PCR Data Migration Completed at :", time.strftime("%H:%M:%S", time.localtime()))

def Delete_16(start_date, end_date):
    print("                  EAST & WEST - Deleting from 16...")
    sql_16.execute('delete from [Billing108].[dbo].[cad_raw_data] where ambulance_assignment_time between \'' + start_date + ' 00:00:00\' and \'' + end_date + ' 23:59:59\';')

def East_202(start_date, end_date):
    # East Data Processing in Server 202
    east_cur_202 = conn_202.cursor()

    # REPORTS.generate_raw_dataeast
    print('EAST - Executing Stage - 1...')
    east_cur_202.callproc('REPORTS.generate_raw_dataeast', args=(start_date, end_date))
    conn_202.commit()
    print('EAST - Committed Stage - 1')

    # REPORTS.generate_raw_data_finaleast
    print('EAST - Executing Stage - 2...')
    east_cur_202.callproc('REPORTS.generate_raw_data_finaleast')
    conn_202.commit()
    print('EAST - Committed Stage - 2')

    # REPORTS.raw_data_east
    print('EAST - Executing Stage - 3...')
    east_cur_202.callproc('REPORTS.raw_data_east')
    conn_202.commit()
    print('EAST - Committed Stage - 3')

    east_cur_202.close()
    conn_202.close()

    print('East_202_Process completed')

def West_202(start_date, end_date):
    # West Data Processing in Server 202
    west_cur_202 = conn_202.cursor()

    # REPORTS.generate_raw_datawest
    print('                                     WEST - Executing Stage - 1...')
    west_cur_202.callproc('REPORTS.generate_raw_datawest', args=(start_date, end_date))
    conn_202.commit()
    print('                                     WEST - Committed Stage - 1')

    # REPORTS.generate_raw_data_finalwest
    print('                                     WEST - Executing Stage - 2...')
    west_cur_202.callproc('REPORTS.generate_raw_data_finalwest')
    conn_202.commit()
    print('                                     WEST - Committed Stage - 2')

    # REPORTS.raw_data_west
    print('                                     WEST - Executing Stage - 3...')
    west_cur_202.callproc('REPORTS.raw_data_west')
    conn_202.commit()
    print('                                     WEST - Committed Stage - 3')

    west_cur_202.close()
    conn_202.close()

    print('                                     West_202_Process completed')

def East_71(east_row_count):
    # East Data Migration from Server 202 to 71
    export_71.execute('delete from test.cad_raw_data_tmp')
    print("EAST - Reading from 202 for Migration to 71...")
    df = pd.read_sql('select * FROM REPORTS.cad_raw_data_tmp_east', con=export_202)
    print("EAST - Rows Read :", df.shape[0], ", Columns Read :", df.shape[1])
    east_row_count.value=df.shape[0]
    print("EAST - Exporting to 71 ...")
    df.to_sql("cad_raw_data_tmp", export_71, if_exists='append', index=False, chunksize=1500)
    print('East_71_Process completed')

def West_73(west_row_count):
    # West Data Migration from Server 202 to 73
    export_73.execute('delete from test.cad_raw_data_tmp')
    print("                                     WEST - Reading from 202 for Migration to 73...")
    df = pd.read_sql('select * FROM REPORTS.cad_raw_data_tmp_west', con=export_202)
    print("                                     WEST - Rows Read :", df.shape[0], ", Columns Read :", df.shape[1])
    west_row_count.value = df.shape[0]
    print("                                     WEST - Exporting to 73...")
    df.to_sql("cad_raw_data_tmp", export_73, if_exists='append', index=False, chunksize=1500)
    print('                                     West_73_Process completed')

def West_17():
    # West Data Migration from Server 202 to 17
    export_17.execute('delete from BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp')
    print("                                     WEST - Reading from 202 for Migration to 17...")
    df = pd.read_sql('SELECT * FROM REPORTS.cad_raw_data_tmp_west;', con=export_202)
    print("                                     WEST - Rows Read :", df.shape[0], ", Columns Read :", df.shape[1])
    print("                                     WEST - Exporting to 17...")
    df.to_sql("cad_raw_data_tmp", export_17, if_exists='append', index=False, chunksize=1500)
    print('                                     West_17_Process completed')

query = '''
    SELECT 
    incident_id,
    callreferenceid,
    Cluster,
    vehicle_base_district,
    is_mci,
    case_type_name,
    creation_date,
    Level1_end_call_time,
    `Source of Distance`,
    map_distance,
    base_start_odo,
    pickup_reach_odo,
    hsptl_reach_odo,
    hsptl_depart_odo,
    base_reach_odo,
    base_to_scene_gps_km,
    scene_to_base_gps_km,
    scene_to_hsptl_gps_km,
    hsptl_to_base_gps_km,
    Total_gps_trip_kms,
    `Total Trip Kilometer`,
    ambulance_assignment_time,
    Ambulance_base_start_time,
    Ambulance_pickup_point_reach_time,
    Ambulance_pickup_point_departure_time,
    Ambulance_destination_reach_time,
    Ambulance_destination_depart_time,
    Ambulance_base_reach_time,
    Standard_remarks,
    Call_Type,
    Phone_no_of_the_Caller,
    beneficary_contact_number,
    benficiary_district,
    Gender,
    Age,
    vehicle_number,
    Destination_hospital,
    pilot_mobile_number,
    latitude,
    longitude,
    pcr_upload,
    Hyperlink_tab,
    update_from,
    `Response time`,
    `Delay in Response time`,
    DelayResponsetimeMinute,
    Hospital_category,
    at_hospital_gps_km,
    at_scene_gps_km,
    beneficary_trip_uad,
    avail_status_on_pcr,
    backup_vehicle_number,
    Beneficiary_name,
    Destination_district,
    emt_name,
    Pickup_Location,
    Name_of_the_caller,
    drift_status,
    SubEmergencyTypeName,
    last_modified_by
    FROM
'''

def East_16():
    # East Data Migration from Server 202 to 16
    print("EAST - Reading from 202 for Migration to 16...")
    df = pd.read_sql(query + 'REPORTS.cad_raw_data_tmp_east', con=export_202)
    print("EAST - Rows Read :", df.shape[0], ", Columns Read :", df.shape[1])
    print("EAST - Exporting to 16...")
    df.to_sql("cad_raw_data", sql_16, if_exists='append', index=False)
    print('East_16_Process completed')

def West_16():
    # West Data Migration from Server 202 to 16
    print("                                     WEST - Reading from 202 for Migration to 16...")
    df = pd.read_sql(query + 'REPORTS.cad_raw_data_tmp_west', con=export_202)
    print("                                     WEST - Rows Read :", df.shape[0], ", Columns Read :", df.shape[1])
    print("                                     WEST - Exporting to 16...")
    df.to_sql("cad_raw_data", sql_16, if_exists='append', index=False)
    print('                                     West_16_Process completed')

def main(start_date, end_date, buffer_minutes):
    if sc.get_status() == 'Idle':
        try:
            sc.set_status('Busy')
            print('Start Date :', start_date, ', End Date :', end_date)
            if q.online().find('OFFLINE') == -1:
                sync_time = q.sync()
                east_sync_time = datetime.strptime(sync_time[7:26], '%Y-%m-%d %H:%M:%S')
                west_sync_time = datetime.strptime(sync_time[34:53], '%Y-%m-%d %H:%M:%S')
                print('East Last Sync Time :', east_sync_time, ', West Last Sync Time :', west_sync_time)
                buffer_time = datetime.now() - timedelta(minutes=buffer_minutes)
                if buffer_time > east_sync_time or buffer_time > west_sync_time:
                    time_difference = round(((datetime.now() - (east_sync_time if east_sync_time <= west_sync_time else west_sync_time)).total_seconds()) / 60)
                    return 'Last Sync Time is ' + str(time_difference) + ' Minutes old.' + '\n' + 'East : ' + str(east_sync_time) + '\n' + 'West : ' + str(west_sync_time)
                else:
                    start_time = time.mktime(time.localtime())
                    sc.highlight('Billing Data Migration Started at : ' + str(time.strftime("%H:%M:%S", time.localtime())), 'blue')

                    # Manual PCR Data Migration from Server 188 to 202
                    try:
                        Manual_PCR_Data_202(start_date, end_date)
                    except:
                        sc.highlight('Manual PCR Data Migration FAILED, Retrying...')
                        time.sleep(5)
                        Manual_PCR_Data_202(start_date, end_date)

                    # Manual to GPS Data Migration  (this should always be after "Manual PCR Data Migration from Server 188 to 202")
                    mtgdm.main(start_date=start_date, end_date=end_date)

                    # VIP Duty Data Migration
                    vdm.main()

                    # Vehicle Offroad Data Migration
                    vodm.main()

                    # Deleting from 16
                    Delete_16(start_date, end_date)

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
                    west_16_process = Process(target=West_16)
                    west_16_process.start()

                    # Waiting for East_202_Process to complete
                    East_202_Process.join()

                    # East Data Migration from Server 202 to 71
                    east_row_count = Value('i', 0)
                    East_71_Process = Process(target=East_71, args=(east_row_count,))
                    East_71_Process.start()

                    # East Data Migration from Server 202 to 16
                    East_16_Process = Process(target=East_16)
                    East_16_Process.start()

                    # Waiting for West_17_Process to complete
                    West_17_Process.join()

                    # Waiting for west_16_process to complete
                    west_16_process.join()

                    # Waiting for West_73_Process to complete
                    West_73_Process.join()

                    # Waiting for East_16_Process to complete
                    East_16_Process.join()

                    # Delete UAD Cases
                    bud.delete_uad()

                    # Waiting for East_71_Process to complete
                    East_71_Process.join()

                    print('Billing Data Migration Completed at :', time.strftime("%H:%M:%S", time.localtime()))
                    print('East Row Count :', east_row_count.value)
                    print('West Row Count :', west_row_count.value)
                    print('Total Row Count :', east_row_count.value + west_row_count.value)
                    end_time = time.mktime(time.localtime())
                    print("Total Time Taken :", math.ceil((end_time - start_time) / 60), "Minutes")

                    if (East_202_Process.exitcode == 0 and West_202_Process.exitcode == 0 and East_71_Process.exitcode == 0 and West_73_Process.exitcode == 0
                            and West_17_Process.exitcode == 0 and East_16_Process.exitcode == 0 and west_16_process.exitcode == 0):
                        return 'Migration Completed.'
                    else:
                        return 'Migration FAILED.'
            else:
                return 'Server is OFFLINE.'
        except:
            raise
        finally:
            sc.set_status('Idle')
    else:
        return sc.running_status()
# endregion

if __name__ == "__main__":

    start_date = '2025-04-27'
    end_date = '2025-04-27'
    buffer_minutes = 10
    analysis = 'yes'

    # region Code
    try:
        migration_status = main(start_date, end_date, buffer_minutes)
        print(migration_status, '\n')
        if migration_status == 'Migration Completed.':
            if analysis.lower() == 'yes':
                bda.main(start_date, end_date, 'No')

        notification.notify(title='Success', message='Data Migration Completed')
    except:
        traceback.print_exc()
        notification.notify(title='Error', message='Data Migration Failed')
    # endregion