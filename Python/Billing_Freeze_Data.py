from sqlalchemy import create_engine
import pandas as pd
import pyodbc
from datetime import datetime, date, timedelta
import mysql.connector
import os
import time
from plyer import notification
import Billing.Processes.Server_Connections as sc
import Billing.Telegram_Bot.Queries as q
import Billing.Pending_UAD as pu

def main(start_date_time, end_date_time, last_modified_date, process_type='Automatic', manual_start_date=str(date.today() - timedelta(days=1)), manual_end_date=str(date.today()), scope=0):
    if sc.get_status() == 'Idle':
        try:
            sc.set_status('Busy')
            if q.online().find('OFFLINE') == -1:
                print('Start Date :', start_date_time, ', End Date :', end_date_time, ', Last Modified Date :', last_modified_date)
                print('Billing Freeze Data Started at :', datetime.now().strftime("%H:%M:%S"), '\n')

                start_date = str(datetime.strptime(start_date_time, '%Y-%m-%d %H:%M:%S').date())
                end_date = str(datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S').date())
                con_16 = create_engine(sc.connection(16))
                con_run_16 = sc.connection(16, 'run', 'Billing108')
                con_202 = create_engine(sc.connection(202))
                con_Reports_202 = create_engine(sc.connection(202, 'export', 'REPORTS'))
                con_71 = create_engine(sc.connection(71, 'export', 'test'))
                con_73 = create_engine(sc.connection(73, 'export', 'test'))
                con_17 = create_engine(sc.connection(17, 'export', 'REPORTS'))

                # Run Analysis
                if process_type == 'Automatic':
                    print('Analysis Started...')
                    sp_query = f'exec Billing108.dbo.Billing_Data_Analysis \'{start_date}\', \'{end_date}\', \'Automatic\';'
                    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={con_run_16[2]};DATABASE={con_run_16[3]};UID={con_run_16[0]};PWD={con_run_16[1]}')
                    cursor_16 = conn.cursor()
                    cursor_16.execute(sp_query)
                    cursor_16.commit()
                    cursor_16.close()
                    conn.close()
                    print('Analysis Completed.\n')

                # Anomaly Cases
                print('Anomaly Cases Started...')
                anomaly_df = pd.DataFrame
                if process_type == 'Automatic':
                    if scope == 1:
                        scopes = 'IT IS,RTNM Desk'
                    elif scope == 2:
                        scopes = 'IT IS'
                    else:
                        scopes = 'IT IS,RTNM Desk,ERC'

                    anomaly_query = f'''
                        select Observation, incident_id, ambulance_assignment_time from pending_cases('{start_date}','{end_date}','{scopes}');
                    '''
                    anomaly_df = pd.read_sql(anomaly_query, con=con_16)
                    anomaly_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\Anomaly -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                    anomaly_df.to_excel(anomaly_filename, index=False)
                elif process_type == 'Manual':
                    anomaly_df = pd.read_excel(r'C:\Users\singh_kislay\Desktop\Pending Cases.xlsb', 'Pending Cases', usecols='A,B,C')

                anomaly_cases = anomaly_df.shape[0]
                print("Anomaly Cases :", anomaly_cases)
                print('Anomaly Cases Completed.\n')

                # Pending UAD Cases
                print('Pending UAD Cases Started...')
                if process_type == 'Automatic':
                    pu.main(start_date, end_date, 'Manual')
                elif process_type == 'Manual':
                    pu.main(start_date, end_date, 'Automatic', anomaly_cases)
                print('Pending UAD Cases Completed.\n')

                # All Cases
                print('All Cases Started...')
                all_cases_query = '''
                    select incident_id,ambulance_assignment_time as ambulance_assignment
                    from [Billing108].[dbo].[cad_raw_data]
                    where ambulance_assignment_time between \'''' + start_date_time + '''\' AND \'''' + end_date_time + '''\';
                '''
                all_cases_df = pd.read_sql(all_cases_query, con=con_16)
                all_cases_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\All Cases -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                all_cases_df.to_excel(all_cases_filename, index=False)
                print("All Cases :", all_cases_df.shape[0])
                print('All Cases Completed.\n')

                # Freezed Cases
                print('Freezed Cases Started...')
                freezed_query = '''
                    SELECT 'Freezed' as 'Observation',crd.incident_id,crd.ambulance_assignment_time
                    FROM test.cad_raw_data crd
                    WHERE ambulance_assignment_time BETWEEN \'''' + start_date_time + '''\' AND \'''' + end_date_time + '''\';
                '''
                east_freezed_df = pd.read_sql(freezed_query, con=con_71)
                west_freezed_df = pd.read_sql(freezed_query, con=con_73)
                freezed_df = pd.concat([east_freezed_df, west_freezed_df])
                print("Freezed Cases :", freezed_df.shape[0])
                print('Freezed Cases Completed.\n')

                # Manual Cases
                print(f'Manual Cases from {manual_start_date} to {manual_end_date} Started...')
                manual_query = '''
                    select 'Manual' as 'Observation',incident_id,ambulance_assignment_time
                    from [Billing108].[dbo].[cad_raw_data]
                    where [Source of Distance]='Manual'
                    and ambulance_assignment_time between \'''' + manual_start_date + ''' 00:00:00\' AND \'''' + manual_end_date + ''' 23:59:59\';
                '''
                manual_df = pd.read_sql(manual_query, con=con_16)
                print("Manual Cases :", manual_df.shape[0])
                print('Manual Cases Completed.\n')

                # IFT Cases
                print('IFT Cases today Started...')
                ift_query = '''
                    select 'IFT' as 'Observation',incident_id,ambulance_assignment_time
                    from [Billing108].[dbo].[cad_raw_data]
                    where case_type_name='IFT'
                    and ambulance_assignment_time between \'''' + str(date.today()) + ''' 00:00:00\' AND \'''' + str(date.today()) + ''' 23:59:59\';
                '''
                ift_df = pd.read_sql(ift_query, con=con_16)
                print("IFT Cases :", ift_df.shape[0])
                print('IFT Cases Completed.\n')

                # KMs Activity Cases
                print('KMs Activity Cases Started...')
                kms_query = '''
                    SELECT 'KMs Activity' as 'Observation',incident_id,ambulance_assignment_time
                    FROM REPORTS.KMs_Activity
                    WHERE Ambulance_Assignment_Time BETWEEN \'''' + start_date_time + '''\' AND \'''' + end_date_time + '''\';
                '''
                kms_df = pd.read_sql(kms_query, con=con_202)
                sc.highlight('KMs Activity Cases : ' + str(kms_df.shape[0]))
                print('KMs Activity Cases Completed.\n')

                # Cases Hold
                print('Cases Hold Started...')
                cases_hold_df = pd.concat([anomaly_df, freezed_df, manual_df, ift_df, kms_df])
                cases_hold_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\Cases Hold -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                cases_hold_df.to_excel(cases_hold_filename, index=False)
                print('Cases Hold :', cases_hold_df.shape[0])
                print('Cases Hold Completed.\n')

                # Freeze Cases - Stage 1
                print('Freeze Cases - Stage 1 Started...')
                freeze_cases_stage_1_df = pd.merge(cases_hold_df, all_cases_df, on='incident_id', how='right', indicator=True)
                freeze_cases_stage_1_df = freeze_cases_stage_1_df[freeze_cases_stage_1_df['_merge'] == 'right_only']
                freeze_cases_stage_1_df = freeze_cases_stage_1_df[["incident_id", "ambulance_assignment"]]
                freeze_cases_stage_1_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\Freeze Cases Stage 1 -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                freeze_cases_stage_1_df.to_excel(freeze_cases_stage_1_filename, index=False)
                print("Freeze Cases - Stage 1 :", freeze_cases_stage_1_df.shape[0])
                print('Freeze Cases - Stage 1 Completed.\n')

                # Freeze Cases Upload
                def freeze_cases_upload(df):
                    # REPLACE is used in below statements
                    df.to_sql('freeze_ids', con=con_71, if_exists='replace', index=False, chunksize=6000)
                    df.to_sql('freeze_ids', con=con_73, if_exists='replace', index=False, chunksize=6000)
                    df.to_sql('freeze_ids', con=con_Reports_202, if_exists='replace', index=False, chunksize=6000)
                    df.to_sql('freeze_ids', con=con_17, if_exists='replace', index=False, chunksize=6000)

                # Upload Freeze Cases - Stage 1
                print('Upload Freeze Cases - Stage 1 Started...')
                freeze_cases_upload(freeze_cases_stage_1_df)
                print('Upload Freeze Cases - Stage 1 Completed.\n')

                # Modified Cases
                print('Modified Cases Started...')
                def modified_cases_query(database):
                    return '''
                        SELECT 'Modified' as 'Observation',f.incident_id,f.ambulance_assignment as 'ambulance_assignment_time'
                        FROM REPORTS.freeze_ids f
                        INNER JOIN ''' + database + '''.t_beneficiary_scheduled_trip_details t
                        ON f.incident_id=t.incident_id
                        WHERE t.last_modified_date>\'''' + last_modified_date + '''\';
                    '''
                east_modified_cases_df = pd.read_sql(modified_cases_query('CAD_UP_PROD'), con=con_202)
                west_modified_cases_df = pd.read_sql(modified_cases_query('CAD_UP_WEST_PROD'), con=con_202)
                modified_cases_df = pd.concat([east_modified_cases_df, west_modified_cases_df])
                modified_cases_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\Modified Cases -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                modified_cases_df.to_excel(modified_cases_filename, index=False)
                sc.highlight('Modified Cases : ' + str(modified_cases_df.shape[0]))
                print('Modified Cases Completed.\n')

                if modified_cases_df.shape[0] > 0:
                    # Freeze Cases - Stage 2
                    print('Freeze Cases - Stage 2 Started...')
                    freeze_cases_stage_2_df = pd.merge(modified_cases_df, freeze_cases_stage_1_df, on='incident_id', how='right', indicator=True)
                    freeze_cases_stage_2_df = freeze_cases_stage_2_df[freeze_cases_stage_2_df['_merge'] == 'right_only'].reset_index()
                    freeze_cases_stage_2_df = freeze_cases_stage_2_df[["incident_id", "ambulance_assignment"]]
                    freeze_cases_stage_2_filename = r'C:\Users\singh_kislay\Desktop\Billing Freeze Data\Freeze Cases Stage 2 -' + datetime.now().strftime("%d-%m-%Y %H.%M.%S") + '.xlsx'
                    freeze_cases_stage_2_df.to_excel(freeze_cases_stage_2_filename, index=False)
                    print("Freeze Cases - Stage 2 :", freeze_cases_stage_2_df.shape[0])
                    print('Freeze Cases - Stage 2 Completed.\n')

                    # Upload Freeze Cases - Stage 2
                    print('Upload Freeze Cases - Stage 2 Started...')
                    freeze_cases_upload(freeze_cases_stage_2_df)
                    print('Upload Freeze Cases - Stage 2 Completed.\n')

                # Run Query
                def run_query(server, query_name, query):
                    df_run = sc.connection(server, 'run')
                    mysql_con = mysql.connector.connect(user=df_run[0], password=df_run[1], host=df_run[2])
                    cursor = mysql_con.cursor()
                    cursor.execute(query)
                    print('Commit', query_name, ':', cursor.rowcount)
                    cursor.close()
                    return mysql_con

                conn_list = []
                try:
                    # Transfer from cad_raw_data_tmp to up108east(west)_new.t_billing_live_caseids
                    print('Transfer from cad_raw_data_tmp to up108east(west)_new.t_billing_live_caseids Started...')
                    def crdt_to_tblc_query(tblc_db, crd_db, freeze_db):
                        return '''
                            INSERT INTO ''' + tblc_db + '''.t_billing_live_caseids(caseid,datasource,assigntime,createdtime)
                            SELECT crd.incident_id,crd.`Source of Distance`,crd.ambulance_assignment_time,NOW()
                            FROM ''' + crd_db + '''.cad_raw_data_tmp crd
                            INNER JOIN ''' + freeze_db + '''.freeze_ids f
                            ON crd.incident_id=f.incident_id;
                        '''
                    conn_list.append(run_query(71, 'test.cad_raw_data_tmp to up108east_new.t_billing_live_caseids',
                                               crdt_to_tblc_query('up108east_new', 'test', 'test')))
                    conn_list.append(run_query(73, 'test.cad_raw_data_tmp to up108west_new.t_billing_live_caseids',
                                               crdt_to_tblc_query('up108west_new', 'test', 'test')))
                    conn_list.append(run_query(17, 'BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to up108west_new.t_billing_live_caseids',
                                               crdt_to_tblc_query('up108west_new', 'BILLING_ACTIVITIES_UP_WEST_PROD', 'REPORTS')))
                    print('Transfer from cad_raw_data_tmp to up108east(west)_new.t_billing_live_caseids Completed.\n')

                    # Transfer from test.cad_raw_data_tmp to test.cad_raw_data
                    print('Transfer from test.cad_raw_data_tmp to test.cad_raw_data Started...')
                    crdt_to_crd_query = '''
                        INSERT INTO test.cad_raw_data
                        SELECT crd.*
                        FROM test.cad_raw_data_tmp crd
                        INNER JOIN test.freeze_ids f
                        ON crd.incident_id=f.incident_id;
                    '''
                    conn_list.append(run_query(71, 'test.cad_raw_data_tmp to test.cad_raw_data', crdt_to_crd_query))
                    conn_list.append(run_query(73, 'test.cad_raw_data_tmp to test.cad_raw_data', crdt_to_crd_query))
                    print('Transfer from test.cad_raw_data_tmp to test.cad_raw_data Completed.\n')

                    # Transfer from BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data
                    print('Transfer from BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data Started...')
                    crdt_to_crd_17_query = '''
                        INSERT INTO BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data
                        SELECT crd.*
                        FROM BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp crd
                        INNER JOIN REPORTS.freeze_ids f
                        ON crd.incident_id=f.incident_id;
                    '''
                    conn_list.append(run_query(17, 'BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data',
                                               crdt_to_crd_17_query))
                    print('Transfer from BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data Completed.\n')

                    # Transfer from cad_raw_data_tmp to REPORTS.t_cm_dashboard_rawdata_billing_east(west)
                    print('Transfer from cad_raw_data_tmp to REPORTS.t_cm_dashboard_rawdata_billing_east(west) Started...')
                    def crdt_to_cm_dashboard_query(cm_table, crdt_table):
                        return '''
                            INSERT INTO REPORTS.''' + cm_table + '''
                            SELECT crd.vehicle_tracking_url newlink,crd.incident_id,crd.creation_date calltime,crd.incident_id tripid,
                            crd.call_type case_type,crd.Vehicle_base_location baselocation,crd.vehicle_base_district District,
                            crd.vehicle_number,crd.level1_end_call_time CallEndTime,crd.ambulance_assignment_time AssignTime,
                            crd.ambulance_base_start_time base_start_time,crd.ambulance_pickup_point_reach_time pickup_reach_time,
                            crd.ambulance_destination_reach_time hsptl_reach_time,crd.ambulance_base_reach_time base_reach_time,
                            crd.`Response time` ResponseTime, crd.base_start_odo, crd.pickup_reach_odo, crd.hsptl_reach_odo, crd.base_reach_odo,
                            crd.total_kms_entered_by_ero CCDKms,crd.total_gps_trip_kms operationalgpskms, crd.total_gps_trip_kms GpsKm,
                            crd.`source of distance` CaseDataFrom,crd.base_to_scene_gps_km SceneKms,crd.scene_to_hsptl_gps_km HospitalKms,
                            crd.`source of distance` closed_from,crd.`source of distance` CaseDataFrom1,'Yes' CaseClosed,crd.Is_Avail,crd.is_rt,
                            crd.Beneficiary_name, crd.benficiary_district, crd.Beneficiary_Village, crd.beneficary_contact_number
                            FROM ''' + crdt_table + ''' crd
                            INNER JOIN REPORTS.freeze_ids f
                            ON crd.incident_id=f.incident_id;
                        '''
                    conn_list.append(run_query(202, 'REPORTS.cad_raw_data_tmp_east to REPORTS.t_cm_dashboard_rawdata_billing_east',
                              crdt_to_cm_dashboard_query('t_cm_dashboard_rawdata_billing_east', 'REPORTS.cad_raw_data_tmp_east')))
                    conn_list.append(run_query(17, 'BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp to REPORTS.t_cm_dashboard_rawdata_billing_west',
                              crdt_to_cm_dashboard_query('t_cm_dashboard_rawdata_billing_west', 'BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp')))
                    print('Transfer from cad_raw_data_tmp to REPORTS.t_cm_dashboard_rawdata_billing_east(west) Completed.\n')

                    # Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.t_billing_live_caseids_new
                    print('Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.t_billing_live_caseids_new Started...')
                    def crdt_to_tblcn_query(crdt_table):
                        return '''
                            INSERT INTO REPORTS.t_billing_live_caseids_new(caseid,datasource,assigntime,createdtime,vehicle_base_district)
                            SELECT crd.incident_id,crd.`Source of Distance`,crd.ambulance_assignment_time,NOW(),crd.vehicle_base_district
                            FROM ''' + crdt_table + ''' crd
                            INNER JOIN REPORTS.freeze_ids f
                            ON crd.incident_id=f.incident_id;
                        '''
                    conn_list.append(run_query(202, 'REPORTS.cad_raw_data_tmp_east to REPORTS.t_billing_live_caseids_new',
                                               crdt_to_tblcn_query('REPORTS.cad_raw_data_tmp_east')))
                    conn_list.append(run_query(17, 'REPORTS.cad_raw_data_tmp_west to REPORTS.t_billing_live_caseids_new',
                                               crdt_to_tblcn_query('BILLING_ACTIVITIES_UP_WEST_PROD.cad_raw_data_tmp')))
                    print('Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.t_billing_live_caseids_new Completed.\n')

                    # Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.freezed_(east)west_cases
                    print('Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.freezed_(east)west_cases Started...')
                    def crdt_to_fc_query(fc_table,crdt_table):
                        return f'''
                            INSERT INTO REPORTS.{fc_table}
                            SELECT crd.incident_id,crd.ambulance_assignment_time,crd.insert_date,crd.Ambulance_base_start_time,
                            crd.Ambulance_pickup_point_reach_time,crd.Ambulance_pickup_point_departure_time,crd.Ambulance_destination_reach_time,
                            crd.Ambulance_destination_depart_time,crd.Ambulance_base_reach_time
                            FROM REPORTS.{crdt_table} crd
                            INNER JOIN REPORTS.freeze_ids f
                            ON crd.incident_id=f.incident_id;
                        '''
                    conn_list.append(run_query(202, 'REPORTS.cad_raw_data_tmp_east to REPORTS.freezed_east_cases',
                                               crdt_to_fc_query('freezed_east_cases', 'cad_raw_data_tmp_east')))
                    conn_list.append(run_query(202, 'REPORTS.cad_raw_data_tmp_west to REPORTS.freezed_west_cases',
                                               crdt_to_fc_query('freezed_west_cases', 'cad_raw_data_tmp_west')))
                    print('Transfer from REPORTS.cad_raw_data_tmp_east(west) to REPORTS.freezed_(east)west_cases Completed.\n')
                except:
                    for con in conn_list:
                        con.rollback()
                        con.close()
                    sc.highlight('INSERT FAILED.')
                    notification.notify(title='Error', message='Freezing Failed')
                    exit()
                try:
                    for con in conn_list:
                        con.commit()
                        con.close()
                except:
                    sc.highlight('COMMIT FAILED.')
                    exit()
                print('Billing Freeze Data Completed at :', datetime.now().strftime("%H:%M:%S"))
                return 'Freeze Completed.'
            else:
                return 'Server is OFFLINE.'
        except:
            pass
        finally:
            sc.set_status('Idle')
    else:
        return sc.running_status()

if __name__ == "__main__":
    # process_type = 'Automatic'
    process_type = 'Manual'
    start_date = '2025-04-27 00:00:00'
    end_date = '2025-04-27 23:59:59'
    last_modified_date = '2025-04-29 11:40:00'
    automatic_scope = 2     # scope = 0(IT IS,RTNM Desk,ERC), 1(IT IS,RTNM Desk), 2(IT IS), IT IS / CPED is included in all scopes by default

    main(start_date, end_date, last_modified_date, process_type, scope=automatic_scope)
    # main(start_date, end_date, last_modified_date, process_type, '2025-05-01', '2025-05-01', scope=automatic_scope)
    # region Code
    if process_type == 'Manual':
        filename = (r'C:\Users\singh_kislay\Desktop\Pending Cases '
                    + str(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').day)
                    + ' to ' + str(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').day) + ' '
                    + str(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').strftime('%b')) + ' - ' + str(datetime.now().day)
                    + ' ' + str(datetime.now().strftime('%b')) + ' ' + str(time.strftime("%I.%M %p")) + '.xlsb')
        os.rename(r'C:\Users\singh_kislay\Desktop\Pending Cases.xlsb', filename)
    # endregion