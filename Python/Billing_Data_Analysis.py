import pandas as pd
import pyodbc
import xlwings as xw
import shutil
import time
from datetime import datetime
from sqlalchemy import create_engine
from pathlib import Path
import Billing.Processes.Server_Connections as sc
import Billing.Billing_Unfreeze_Data as bud
import Billing.Processes.Send_Email as se

def main(start_date, end_date, send_email, table='cad_raw_data', generate_file='yes', overlap_unfreeze='yes'):
    if sc.get_status() == 'Idle':
        sql_engine = None
        conn = None
        cursor = None
        app = None
        wb = None
        mysql_engine = None
        try:
            sc.set_status('Busy')
            print("Billing Data Analysis Started at :", time.strftime("%H:%M:%S", time.localtime()))
            sql_engine = create_engine(sc.connection(16))

            # UAD CPED
            uad_cped_query = f'''
                select distinct crd.incident_id
                from [Billing108].[dbo].[cad_raw_data] crd
                inner join [Billing108].[dbo].exceptional_cases('{start_date} 00:00:00', '{end_date} 23:59:59') ec on crd.incident_id=ec.[Incident Id]
                where crd.ambulance_assignment_time between '{start_date} 00:00:00' and '{end_date} 23:59:59'
                and ec.[Standard Remarks] in ('UAD Case','Escalated Case (Case Overlap)');
            '''
            uad_cped_df = pd.read_sql(uad_cped_query, con=sql_engine)
            if not uad_cped_df.empty:
                bud.main('UAD_CPED', ','.join(map(str, uad_cped_df['incident_id'].astype('int64').tolist())))

            # Run Analysis Procedure
            con_df = sc.connection(16, 'run', 'Billing108')
            conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={con_df[2]};DATABASE={con_df[3]};UID={con_df[0]};PWD={con_df[1]}')
            sp_query = f'''
                exec Billing108.dbo.Billing_Data_Analysis 
                '{datetime.strptime(start_date, '%Y-%m-%d').date()}', '{datetime.strptime(end_date, '%Y-%m-%d').date()}',
                'Manual', '{table}';
            '''
            cursor = conn.cursor()
            cursor.execute(sp_query)

            filepath = ''
            if generate_file.lower() == 'yes':
                source = fr'C:\Users\{sc.username}\Documents\GitHub\GVK-EMRI\108 Billing Process\Templates\108 Data Analysis.xlsb'
                filepath = fr'C:\Users\{sc.username}\Desktop\108 Data Analysis ' + str(datetime.strptime(start_date, '%Y-%m-%d').day) \
                           + ' to ' + str(datetime.strptime(end_date, '%Y-%m-%d').day) + ' ' \
                           + str(datetime.strptime(end_date, '%Y-%m-%d').strftime('%b')) + ' - ' + str(datetime.now().day) \
                           + ' ' + str(datetime.now().strftime('%b')) + ' ' + str(time.strftime("%I.%M %p")) + '.xlsb'
                shutil.copyfile(source, filepath)

                # wb = xw.Book(filepath)     # this can be used instead of the below 2 statements
                app = xw.App(visible=True)
                wb = app.books.open(filepath)

                def write_sheet(sheet_name, df, row):
                    ws = wb.sheets(sheet_name)
                    if not df.empty:
                        ws.cells(row, 1).options(index=False, header=False).value = df
                    else:
                        ws.visible = False

                # Anomaly Data
                anomaly_rows = []
                for rec in cursor:
                    anomaly_rows.append({'Observation': rec[0], 'Incident ID': rec[1], 'Ambulance Assignment Time': rec[2], 'Cluster Name': rec[3],
                            'is mci': rec[4], 'Source of Distance': rec[5], 'Case Type': rec[6], 'Map Distance': rec[7],
                            'Update From': rec[8], 'Call End': rec[9], 'Scope': rec[10], 'Standard Remarks': rec[11]})
                anomaly = pd.DataFrame(anomaly_rows)
                write_sheet('Data', anomaly, 2)

                # Case Overlap
                if cursor.nextset():
                    case_overlap_rows = []
                    for rec in cursor:
                        case_overlap_rows.append({'Overlapping ID': rec[0], 'Overlapping AT': rec[1], 'Overlapping BRT': rec[2], 'update_from': rec[3],
                                'Standard Remarks': rec[4], 'Overlapped ID': rec[5], 'Overlapped AT': rec[6], 'Overlapped BRT': rec[7]})
                    case_overlap = pd.DataFrame(case_overlap_rows)
                    write_sheet('Case Overlap', case_overlap, 3)
                    if not case_overlap.empty and overlap_unfreeze == 'yes':
                        bud.main('Overlapping', ','.join(map(str, case_overlap['Overlapping ID'].astype('int64').tolist())))

                # VIP Duty Overlap
                if cursor.nextset():
                    vip_overlap_rows = []
                    for rec in cursor:
                        vip_overlap_rows.append({'incident_id': rec[0], 'Cluster': rec[1], 'vehicle_number': rec[2], 'ambulance_assignment_time': rec[3],
                                'Ambulance_base_reach_time': rec[4], 'update_from': rec[5], 'Standard Remarks': rec[6],
                                'ID': rec[7], 'Start Date': rec[8], 'End Date': rec[9]})
                    vip_overlap = pd.DataFrame(vip_overlap_rows)
                    write_sheet('VIP Duty Overlap', vip_overlap, 3)

                # Vehicle Offroad Case Overlap
                if cursor.nextset():
                    offroad_overlap_rows = []
                    for rec in cursor:
                        offroad_overlap_rows.append({'incident_id': rec[0], 'vehicle_number': rec[1], 'ambulance_assignment_time': rec[2],
                                                     'Ambulance_base_reach_time': rec[3], 'Standard Remarks': rec[4], 'off_road_time': rec[5],
                                                     'on_road_time': rec[6]})
                    offroad_overlap = pd.DataFrame(offroad_overlap_rows)
                    write_sheet('Vehicle Offroad Case Overlap', offroad_overlap, 3)

                wb.sheets['Summary'].activate()
                wb.save(filepath)
            cursor.commit()

            # Beneficiary Contact Number
            bcn_query = '''
                select iif(SUBSTRING(convert(varchar,[Incident ID]),5,1)=1,'East','West') as 'Cluster',
                [Incident ID] as 'IncidentID',[Ambulance Assignment Time] as 'Ambulance_Assignment_Time'
                from [Billing108].[dbo].[cad_raw_data_anomaly]
                where [Insert Date]=
                (
                    select max([Insert Date])
                    from [Billing108].[dbo].[cad_raw_data_anomaly]
                )
                and Observation='Benef. Contact No. in more than 2 Districts';
            '''
            bcn_df = pd.read_sql(bcn_query, con=sql_engine)
            if not bcn_df.empty:
                bcn_all_query = '''
                    SELECT IncidentID
                    FROM REPORTS.Billing_Contact_Number
                    WHERE Ambulance_Assignment_Time > DATE_SUB(NOW(),INTERVAL 35 DAY);
                '''
                mysql_engine = create_engine(sc.connection(202, 'export', 'REPORTS'))
                bcn_all_df = pd.read_sql(bcn_all_query, con=mysql_engine)
                upload_bcn_df = pd.merge(bcn_df, bcn_all_df, on='IncidentID', how='left', indicator=True)
                upload_bcn_df = upload_bcn_df[upload_bcn_df['_merge'] == 'left_only'].reset_index(drop=True)
                contacts_cases = upload_bcn_df.shape[0]
                if contacts_cases > 0:
                    print('Benef. Contact No. in more than 2 Districts:', contacts_cases)
                    upload_bcn_df = upload_bcn_df[['Cluster', 'IncidentID', 'Ambulance_Assignment_Time']]
                    upload_bcn_df.to_sql('Billing_Contact_Number', mysql_engine, if_exists='append', index=False)

            # Send Email
            if generate_file.lower() == 'yes' and send_email.lower() == 'yes':
                filename = Path(filepath).stem
                email_body = '''Dear Sir,
        
Please find attached file containing 108 Data Analysis. UAD cases may not be excluded from Analysis.

Regards,
Kislay Kumar Singh
IS Department
                '''
                se.send_mail(['vishal_jayaswal@emri.in', 'kvishesh_bahadur@emri.in'], filename, email_body,
                             ['sanjay_yadav@emri.in', 'up_cped@emri.in'], filepath, filename)

            print("Billing Data Analysis Completed at :", time.strftime("%H:%M:%S", time.localtime()))
            return True
        except Exception as e:
            print('🔴 Billing Data Analysis FAILED.', e)
            raise
        finally:
            sc.set_status('Idle')
            if sql_engine:
                sql_engine.dispose()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            if wb:
                wb.close()
            if app:
                app.quit()
            if mysql_engine:
                mysql_engine.dispose()
    else:
        return sc.running_status()

if __name__ == "__main__":
    start_date = '2026-05-20'
    end_date = '2026-05-20'
    main(start_date, end_date, 'Yes')