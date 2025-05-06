from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, date, timedelta
from plyer import notification
from colorama import Fore, Style
import shutil
import xlwings as xw
import Billing.Processes.Server_Connections as sc
import Billing.Call_Data_Extraction.Extract_Details as ed

# region Code
def export_cases(cases_date):
    query = '''
                select distinct [Incident ID]
                from [Billing108].[dbo].[cad_raw_data_anomaly]
                where [Insert Date]=(select max([Insert Date]) as 'Last Insert Date' from [Billing108].[dbo].[cad_raw_data_anomaly])
                and [Ambulance Assignment Time] between \'''' + cases_date + ''' 00:00:00' AND \'''' + cases_date + ''' 23:59:59'
                and Observation in
                (
                    'Call Start Null or 01-01-1900',
                    'Call End Null or 01-01-1900',
                    'Call Start > Call end',
                    'Call Start = Call end',
                    'Call End < Assignment',
                    'Call End = Assignment',
                    'Call duration >= 30 Min',
                    'Call Start = Assignment',
                    'Call Reference ID Null',
                    'Call Duration < 40 seconds',
                    'Call Duration < 10 seconds',
                    'Call Start > Assignment',
                    'Call Start = Assignment'
                );
            '''
    df = pd.read_sql(query, con=create_engine(sc.connection(16)))
    rows = df.shape[0]
    if rows > 0:
        if rows <= 300:
            return ','.join(map(str, df['Incident ID'].astype('int64').tolist()))
        else:
            print('More than 300 call anomalies.')
    else:
        print('No call anomaly available in', cases_date)

def export_template(cluster, db, cases, file):
    query = '''
        SELECT if(MID(tci.incident_id,5,1)=1,'East','West') AS Cluster,tci.incident_id,
        IF(tci.callreferenceid IS NULL,teci11.reference_number,tci.callreferenceid) AS callreferenceid,
        tci.created_by AS agent_id,tci.phone_number,tci.creation_date,teci11.call_end_time,
        tiam.creation_date AS AmbyAssignTime,tbstd.pickup_reach_time,tci.parent_incident_id,
        IF(tci.level2reason='MCI' OR teia.emergencies_id=5,1,0) AS is_mci
        FROM t_cad_incident tci
        JOIN t_incident_ambulance_mapping tiam ON tiam.incident_id = tci.incident_id
        JOIN t_beneficiary_scheduled_trip_details tbstd ON tbstd.incident_id = tci.incident_id
        LEFT JOIN 
        (
            SELECT * FROM t_end_call_information ok4
            WHERE ok4.id = (SELECT MAX(id) FROM t_end_call_information
            WHERE incident_id=ok4.incident_id  AND call_end_time <>'')
        ) teci11 ON teci11.incident_id=tci.incident_id
        LEFT JOIN 
        (
            SELECT DISTINCT cad_incident_incident_id,IFNULL(emergencies_id,0) emergencies_id 
            FROM t_cad_incident_emergencies WHERE emergencies_id=5
        ) teia ON teia.cad_incident_incident_id=tci.incident_id
        WHERE tci.incident_id IN
        (
            ''' + cases + '''
        );
    '''
    df = pd.read_sql(query, con=create_engine(sc.connection(202, 'export', db))).fillna('Null')
    xw.App(visible=False)
    wb = xw.Book(file)
    ws = wb.sheets(cluster)
    ws.cells(2, 1).options(index=False, header=False).value = df
    wb.save(file)
    wb.close()

def api_export(api_url):
    output = ed.run_api(api_url)
    if output['CallEndTime'] == '' or output['CallEndTime'] == '0000-00-00 00:00:00':
        raise
    else:
        return output['CallEndTime']

def export_by_phone(server, phone_number, reference_no, call_start, amby_assign, db):
    outbound_query = '''
        SELECT ol.call_hit_referenceno,ol.call_end_time,
        TIMESTAMPDIFF(SECOND,\'''' + str(call_start) + '''\',ol.call_end_time) AS `Call_Duration(sec.)`
        FROM ''' + db + '''.convoxccs_outbound_log ol
        WHERE ol.phone_number LIKE CONCAT('%',\'''' + str(phone_number) + '''\')
        AND DATE(ol.entry_date) = \'''' + str(amby_assign)[0:10] + '''\'

        UNION
    '''
    query = '''
        SELECT call_mapping_referenceno, call_end_time, `Call_Duration(sec.)`
        FROM
        (
            SELECT al.call_mapping_referenceno,al.call_end_time,
            TIMESTAMPDIFF(SECOND,\'''' + str(call_start) + '''\',al.call_end_time) AS `Call_Duration(sec.)`
            FROM ''' + db + '''.convoxccs_agent_log al
            WHERE al.phone_number LIKE CONCAT('%',\'''' + str(phone_number) + '''\')
            AND DATE(al.call_start_time) = \'''' + str(amby_assign)[0:10] + '''\'

            UNION

            ''' + ed.generate_outbound_query(server, outbound_query) + '''

            SELECT cl.call_mapping_referenceno,cl.call_end_time,
            TIMESTAMPDIFF(SECOND,\'''' + str(call_start) + '''\',cl.call_end_time) AS `Call_Duration(sec.)`
            FROM ''' + db + '''.convoxccs_cdr_log cl
            WHERE cl.phone_number LIKE CONCAT('%',\'''' + str(phone_number) + '''\')
            AND DATE(cl.call_start_time) = \'''' + str(amby_assign)[0:10] + '''\'
            
            UNION
            
            SELECT cl.call_mapping_referenceno,cl.call_end_time,
            TIMESTAMPDIFF(SECOND,\'''' + str(call_start) + '''\',cl.call_end_time) AS `Call_Duration(sec.)`
            FROM ''' + db + '''.convoxccs_cdr_log cl
            WHERE cl.call_mapping_referenceno=\'''' + reference_no + '''\'
            AND DATE(cl.call_start_time) = \'''' + str(amby_assign)[0:10] + '''\'
            
            UNION
            
            SELECT ml.call_hit_referenceno,ml.end_time,
            TIMESTAMPDIFF(SECOND,\'''' + str(call_start) + '''\',ml.end_time) AS `Call_Duration(sec.)`
            FROM ''' + db + '''.convoxccs_manual_log ml 
            WHERE ml.call_hit_referenceno=\'''' + reference_no + '''\'
            AND DATE(ml.entry_time) = \'''' + str(amby_assign)[0:10] + '''\'
        ) ext
        WHERE call_mapping_referenceno <> ''
        ORDER BY `Call_Duration(sec.)`;
    '''
    return pd.read_sql(query, con=create_engine(sc.connection(server)))

def export_by_reference(server, reference_number, call_start, amby_assign, pickup_reach, db):
    query = '''
        SELECT al.call_mapping_referenceno,al.call_end_time 
        FROM ''' + db + '''.convoxccs_agent_log al
        WHERE al.call_mapping_referenceno IN(''' + reference_number + ''')
        AND al.call_end_time >= DATE_ADD(\'''' + str(call_start) + '''\',INTERVAL 40 SECOND)
        AND al.call_end_time < DATE_ADD(\'''' + str(call_start) + '''\',INTERVAL 30 MINUTE)
        AND al.call_end_time > \'''' + str(amby_assign) + '''\'
        AND al.call_end_time < \'''' + str(pickup_reach) + '''\'
        ORDER BY al.call_end_time LIMIT 1;
    '''
    return pd.read_sql(query, con=create_engine(sc.connection(server)))

def call_table(live_dblog, uac, dial_112, db, phone_no, call_start, amby_assign, pickup_reach, reference_no):
    live_dblog_all_df = export_by_phone(live_dblog, phone_no, reference_no, call_start, amby_assign, db)
    live_dblog_df = live_dblog_all_df[(live_dblog_all_df['call_end_time'] > amby_assign) & (live_dblog_all_df['call_end_time'] < pickup_reach)].reset_index(drop=True)
    live_dblog_df_40_1799 = live_dblog_df.loc[(live_dblog_df['Call_Duration(sec.)'] > 39) & (live_dblog_df['Call_Duration(sec.)'] < 1800)].reset_index(drop=True)
    if live_dblog_df_40_1799.shape[0] > 0:
        print(live_dblog_df_40_1799.at[0, 'call_mapping_referenceno'], ',', live_dblog_df_40_1799.at[0, 'call_end_time'], ', live_dblog_40_1799')
        return pd.DataFrame([live_dblog_df_40_1799.at[0, 'call_mapping_referenceno'] + ' , ' + str(live_dblog_df_40_1799.at[0, 'call_end_time']) + ' , live_dblog_40_1799'])
    else:
        uac_all_df = export_by_phone(uac, phone_no, reference_no, call_start, amby_assign, 'convoxccs3')
        uac_df = uac_all_df[(uac_all_df['call_end_time'] > amby_assign) & (uac_all_df['call_end_time'] < pickup_reach)].reset_index(drop=True)
        uac_df_40_1799 = uac_df.loc[(uac_df['Call_Duration(sec.)'] > 39) & (uac_df['Call_Duration(sec.)'] < 1800)].reset_index(drop=True)
        if uac_df_40_1799.shape[0] > 0:
            print(uac_df_40_1799.at[0, 'call_mapping_referenceno'], ',', uac_df_40_1799.at[0, 'call_end_time'], ', uac_40_1799')
            return pd.DataFrame([uac_df_40_1799.at[0, 'call_mapping_referenceno'] + ' , ' + str(uac_df_40_1799.at[0, 'call_end_time']) + ' , uac_40_1799'])
        else:
            dial_112_all_df = export_by_phone(dial_112, phone_no, reference_no, call_start, amby_assign, 'convoxccs3')
            dial_112_df = dial_112_all_df[(dial_112_all_df['call_end_time'] > amby_assign) & (dial_112_all_df['call_end_time'] < pickup_reach)].reset_index(drop=True)
            dial_112_df_40_1799 = dial_112_df.loc[(dial_112_df['Call_Duration(sec.)'] > 39) & (dial_112_df['Call_Duration(sec.)'] < 1800)].reset_index(drop=True)
            if dial_112_df_40_1799.shape[0] > 0:
                print(dial_112_df_40_1799.at[0, 'call_mapping_referenceno'], ',', dial_112_df_40_1799.at[0, 'call_end_time'], ', dial_112_40_1799')
                return pd.DataFrame([dial_112_df_40_1799.at[0, 'call_mapping_referenceno'] + ' , ' + str(dial_112_df_40_1799.at[0, 'call_end_time']) + ' , dial_112_40_1799'])
            else:
                live_dblog_all_df = live_dblog_all_df['call_mapping_referenceno'].drop_duplicates().reset_index(drop=True)
                rows = live_dblog_all_df.shape[0]
                if rows > 0:
                    all_references = ''
                    for i in range(0, rows):
                        if i != rows - 1:
                            all_references = all_references + '\'' + live_dblog_all_df.at[i] + '\','
                        else:
                            all_references = all_references + '\'' + live_dblog_all_df.at[i] + '\''
                    all_reference_df = export_by_reference(live_dblog, all_references, call_start, amby_assign, pickup_reach, db)
                    if all_reference_df.shape[0] > 0:
                        print(all_reference_df.at[0, 'call_mapping_referenceno'], ',', all_reference_df.at[0, 'call_end_time'], ', live_dblog_all_reference')
                        return pd.DataFrame([all_reference_df.at[0, 'call_mapping_referenceno'] + ' , ' + str(all_reference_df.at[0, 'call_end_time']) + ' , live_dblog_all_reference'])
                    else:
                        if (amby_assign-call_start).total_seconds() > 36 and (amby_assign + timedelta(seconds=3)) < pickup_reach:
                            print(Fore.LIGHTRED_EX + reference_no, ',', amby_assign + timedelta(seconds=3), ', Amby Assign + 3 Seconds', Style.RESET_ALL)
                            return pd.DataFrame([reference_no + ' , ' + str(amby_assign + timedelta(seconds=3)) + ' , Amby Assign + 3 Seconds'])
                        elif call_start + timedelta(seconds=40) < pickup_reach:
                            print(Fore.LIGHTRED_EX + reference_no, ',', call_start + timedelta(seconds=40), ', Call Start + 40 Seconds', Style.RESET_ALL)
                            return pd.DataFrame([reference_no + ' , ' + str(call_start + timedelta(seconds=40)) + ' , Call Start + 40 Seconds'])
                        else:
                            print(Fore.LIGHTRED_EX + reference_no, ', No Call End Possible', Style.RESET_ALL)
                            return pd.DataFrame([reference_no + ' , No Call End Possible , No Call End Possible'])
                else:
                    print(Fore.LIGHTRED_EX + reference_no, ', No Call Mapping Reference Available', Style.RESET_ALL)
                    return pd.DataFrame([reference_no + ' , No Call Mapping Reference Available , No Call Mapping Reference Available'])

def assign_server(cluster, call_date, phone_no, call_start, amby_assign, pickup_reach, reference_no):
    days_live, live, db_log, uac, dial_112 = ed.server_ip(cluster)
    if (date.today() - call_date).days < days_live:
        return call_table(live, uac, dial_112, 'convoxccs3', phone_no, call_start, amby_assign, pickup_reach, reference_no)
    else:
        return call_table(db_log, uac, dial_112, 'convoxccs3_log', phone_no, call_start, amby_assign, pickup_reach, reference_no)

def main(process_type, cases='', cases_date=''):
    if process_type == 'Automatic':
        cases = export_cases(cases_date)
    if cases != None:
        source_file = r'C:\Users\singh_kislay\Documents\GitHub\GVK-EMRI\108 Billing Process\Templates\Call Details.xlsx'
        destination_file = r'C:\Users\singh_kislay\Desktop\Call Details - ' + datetime.now().strftime('%Y-%m-%d %H.%M.%S') + '.xlsx'
        shutil.copyfile(source_file, destination_file)
        export_template('East', 'CAD_UP_PROD', cases, destination_file)
        export_template('West', 'CAD_UP_WEST_PROD', cases, destination_file)
        for cluster in ['East', 'West']:
            results_df = pd.DataFrame()
            excel_df = pd.read_excel(destination_file, sheet_name=cluster, usecols='C,E,F,H,I,J,K')
            if excel_df.loc[0].values[1] > 0:
                print('---------------------' + cluster + ' Cluster Data---------------------')
                for x in range(0, len(excel_df.index)):
                    reference_no = excel_df.loc[x].values[0]
                    phone_no = excel_df.loc[x].values[1]
                    call_start = excel_df.loc[x].values[2]
                    call_date = excel_df.loc[x].values[2].date()
                    amby_assign = excel_df.loc[x].values[3]
                    pickup_reach = excel_df.loc[x].values[4]
                    parent_incident_id = excel_df.loc[x].values[5]
                    is_mci = excel_df.loc[x].values[6]
                    if parent_incident_id != 'Null' and is_mci == 1:
                        print(reference_no, ',', amby_assign, ', child mci case')
                        results_df = pd.concat([results_df, pd.DataFrame([reference_no + ' , ' + str(amby_assign) + ' , child mci case'])])
                    else:
                        source, live_db_log, uac, dial_112 = ed.api(cluster, call_start, reference_no)
                        if source != '':
                            try:
                                call_end_time = api_export(live_db_log)
                            except:
                                try:
                                    call_end_time = api_export(uac)
                                except:
                                    try:
                                        call_end_time = api_export(dial_112)
                                    except:
                                        call_end_time = 'Not found in API'
                            if call_end_time != 'Not found in API':
                                api_call_end = datetime.strptime(call_end_time, '%Y-%m-%d %H:%M:%S')
                                call_duration = (api_call_end - call_start).total_seconds()
                                if api_call_end > amby_assign and api_call_end < pickup_reach and call_duration > 39 and call_duration < 1800:
                                    print(reference_no, ',', api_call_end, ', api_40_1799')
                                    results_df = pd.concat([results_df, pd.DataFrame([reference_no + ' , ' + str(api_call_end) + ' , api_40_1799'])])
                                else:
                                    results_df = pd.concat([results_df, assign_server(cluster, call_date, phone_no, call_start, amby_assign, pickup_reach, reference_no)], ignore_index=True)
                            else:
                                results_df = pd.concat([results_df, assign_server(cluster, call_date, phone_no, call_start, amby_assign, pickup_reach, reference_no)], ignore_index=True)
                        else:
                            results_df = pd.concat([results_df, assign_server(cluster, call_date, phone_no, call_start, amby_assign, pickup_reach, reference_no)], ignore_index=True)

            wb = xw.Book(destination_file)
            ws = wb.sheets(cluster)
            ws.cells(2, 12).options(index=False, header=False).value = results_df
            wb.save(destination_file)
            wb.close()
        return destination_file
# endregion

if __name__ == "__main__":
    cases = '''
20252040394194
    '''
    main('Manual', cases)
    # main(process_type='Automatic', cases_date='2024-08-29')
    notification.notify(title='Success', message='Call Data Extraction Completed')