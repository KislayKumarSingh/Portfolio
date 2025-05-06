import pandas as pd
import pyodbc
import xlwings as xw
import shutil
import time
from datetime import datetime
from sqlalchemy import create_engine
import Billing.Processes.Server_Connections as sc
import Billing.Billing_Unfreeze_Data as bud
import Billing.Processes.Send_Email as se

def main(start_date, end_date, send_email, table='cad_raw_data', generate_file='yes'):
    if sc.get_status() == 'Idle':
        try:
            sc.set_status('Busy')
            print("Billing Data Analysis Started at :", time.strftime("%H:%M:%S", time.localtime()))
            sql_engine = create_engine(sc.connection(16))

            # UAD CPED
            uad_cped_query = '''
                select distinct crd.incident_id
                from [Billing108].[dbo].[cad_raw_data] crd
                inner join [Billing108].[dbo].exceptional_cases(\'''' + start_date + ''' 00:00:00', \'''' + end_date + ''' 23:59:59') ec
                on crd.incident_id=ec.[Incident Id]
                where crd.ambulance_assignment_time between \'''' + start_date + ''' 00:00:00' and \'''' + end_date + ''' 23:59:59'
                and ec.[Standard Remarks] in ('UAD Case','Escalated Case (Case Overlap)');
            '''
            uad_cped_df = pd.read_sql(uad_cped_query, con=sql_engine)
            if uad_cped_df.shape[0] > 0:
                bud.main('UAD', ','.join(map(str, uad_cped_df['incident_id'].astype('int64').tolist())))

            # Run Analysis Procedure
            con_df = sc.connection(16, 'run', 'Billing108')
            conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={con_df[2]};DATABASE={con_df[3]};UID={con_df[0]};PWD={con_df[1]}')
            sp_query = '''
                exec Billing108.dbo.Billing_Data_Analysis
                \'''' + str(datetime.strptime(start_date, '%Y-%m-%d').date()) + '''\',
                \'''' + str(datetime.strptime(end_date, '%Y-%m-%d').date()) + '''\',
                'Manual',
                \'''' + table + '''\';
            '''
            cursor = conn.cursor()
            cursor.execute(sp_query)

            destination = ''
            if generate_file.lower() == 'yes':
                source = r'C:\Users\singh_kislay\Documents\GitHub\GVK-EMRI\108 Billing Process\Templates\108 Data Analysis.xlsb'
                destination = r'C:\Users\singh_kislay\Desktop\108 Data Analysis ' + str(datetime.strptime(start_date, '%Y-%m-%d').day) \
                              + ' to ' + str(datetime.strptime(end_date, '%Y-%m-%d').day) + ' ' \
                              + str(datetime.strptime(end_date, '%Y-%m-%d').strftime('%b')) + ' - ' + str(datetime.now().day) \
                              + ' ' + str(datetime.now().strftime('%b')) + ' ' + str(time.strftime("%I.%M %p")) + '.xlsb'
                shutil.copyfile(source, destination)
                wb = xw.Book(destination)

                # Anomaly Data
                anomaly = pd.DataFrame()
                for rec in cursor:
                    data = {'Observation': rec[0], 'Incident ID': rec[1], 'Ambulance Assignment Time': rec[2], 'Cluster Name': rec[3],
                            'is mci': rec[4], 'Source of Distance': rec[5], 'Case Type': rec[6], 'Map Distance': rec[7],
                            'Update From': rec[8], 'Call End': rec[9], 'Scope': rec[10], 'Standard Remarks': rec[11]}
                    anomaly = pd.concat([anomaly, pd.DataFrame([data])], ignore_index=True)
                if anomaly.shape[0] > 0:
                    ws = wb.sheets('Data')
                    ws.cells(2, 1).options(index=False, header=False).value = anomaly

                # Case Overlap
                case_overlap = pd.DataFrame()
                if cursor.nextset():
                    for rec in cursor:
                        data = {'Overlapping ID': rec[0], 'Overlapping AT': rec[1], 'Overlapping BRT': rec[2], 'update_from': rec[3],
                                'Standard Remarks': rec[4], 'Overlapped ID': rec[5], 'Overlapped AT': rec[6], 'Overlapped BRT': rec[7]}
                        case_overlap = pd.concat([case_overlap, pd.DataFrame([data])], ignore_index=True)
                    ws = wb.sheets('Case Overlap')
                    if case_overlap.shape[0] > 0:
                        ws.cells(3, 1).options(index=False, header=False).value = case_overlap
                        bud.main('Overlapping', ','.join(map(str, case_overlap['Overlapping ID'].astype('int64').tolist())))
                    else:
                        ws.visible = False

                # VIP Duty Overlap
                vip_overlap = pd.DataFrame()
                if cursor.nextset():
                    for rec in cursor:
                        data = {'incident_id': rec[0], 'Cluster': rec[1], 'vehicle_number': rec[2], 'ambulance_assignment_time': rec[3],
                                'Ambulance_base_reach_time': rec[4], 'update_from': rec[5], 'Standard Remarks': rec[6],
                                'ID': rec[7], 'Start Date': rec[8], 'End Date': rec[9]}
                        vip_overlap = pd.concat([vip_overlap, pd.DataFrame([data])], ignore_index=True)
                    ws = wb.sheets('VIP Duty Overlap')
                    if vip_overlap.shape[0] > 0:
                        ws.cells(3, 1).options(index=False, header=False).value = vip_overlap
                    else:
                        ws.visible = False

                # Vehicle Offroad Case Overlap
                offroad_overlap = pd.DataFrame()
                if cursor.nextset():
                    for rec in cursor:
                        data = {'incident_id': rec[0], 'vehicle_number': rec[1], 'ambulance_assignment_time': rec[2], 'Ambulance_base_reach_time': rec[3],
                                'Standard Remarks': rec[4], 'off_road_time': rec[5], 'on_road_time': rec[6]}
                        offroad_overlap = pd.concat([offroad_overlap, pd.DataFrame([data])], ignore_index=True)
                    ws = wb.sheets('Vehicle Offroad Case Overlap')
                    if offroad_overlap.shape[0] > 0:
                        ws.cells(3, 1).options(index=False, header=False).value = offroad_overlap
                    else:
                        ws.visible = False

                wb.sheets['Summary'].activate()
                wb.save(destination)
                wb.close()

            cursor.commit()
            cursor.close()
            conn.close()

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
            if bcn_df.shape[0] > 0:
                bcn_all_query = '''
                    SELECT IncidentID
                    FROM REPORTS.Billing_Contact_Number
                    WHERE Ambulance_Assignment_Time > DATE_SUB(NOW(),INTERVAL 35 DAY);
                '''
                mysql_engine = create_engine(sc.connection(202, 'export', 'REPORTS'))
                bcn_all_df = pd.read_sql(bcn_all_query, con=mysql_engine)
                upload_bcn_df = pd.merge(bcn_df, bcn_all_df, on='IncidentID', how='left', indicator=True)
                upload_bcn_df = upload_bcn_df[upload_bcn_df['_merge'] == 'left_only'].reset_index(drop=True)
                if upload_bcn_df.shape[0] > 0:
                    upload_bcn_df = upload_bcn_df[['Cluster', 'IncidentID', 'Ambulance_Assignment_Time']]
                    upload_bcn_df.to_sql('Billing_Contact_Number', mysql_engine, if_exists='append', index=False)

            # Send Email
            if generate_file.lower() == 'yes' and send_email.lower() == 'yes':
                subject = destination.split('\\')[4].split('.')
                email_body = '''Dear Sir,
        
Please find attached file containing 108 Data Analysis. UAD cases may not be excluded from Analysis.

Regards,
Kislay Kumar Singh
IS Department
                '''
                se.send_mail(['a.b@c.in', 'a.b@c.in'], subject[0] + '.' + subject[1], email_body,
                             ['a.b@c.in', 'a.b@c.in', 'a.b@c.in'], destination, subject[0] + '.' + subject[1])

            print("Billing Data Analysis Completed at :", time.strftime("%H:%M:%S", time.localtime()))
        except:
            sc.highlight('Billing Data Analysis FAILED.')
            raise
        finally:
            sc.set_status('Idle')
    else:
        return sc.running_status()

if __name__ == "__main__":
    start_date = '2025-04-24'
    end_date = '2025-04-24'
    print(main(start_date, end_date, 'No'))