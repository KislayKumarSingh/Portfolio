import pandas as pd
import threading
import Billing.Call_Data_Extraction.Extract_Details as ed

filename = r'C:\Users\singh_kislay\Desktop\Call Details.xlsx'
no_of_threads = 50

# region Code
def api_export(api_url):
    return pd.DataFrame([ed.run_api(api_url)])

def assign_server(cluster, df, thread_id, result_dfs, lock):
    data_df = pd.DataFrame()
    for i in range(df.shape[0]):
        reference_no = df.loc[i, 'Av_CallHitReferenceNo']
        call_start = df.loc[i, 'CallStartTime']
        source, live_db_log, uac, dial_112 = ed.api(cluster, call_start, reference_no)
        if source != '':
            live_db_log_df = api_export(live_db_log)
            if live_db_log_df.shape[1] > 1:
                data_df = pd.concat([data_df, live_db_log_df]).reset_index(drop=True)
            else:
                uac_df = api_export(uac)
                if uac_df.shape[1] > 1:
                    data_df = pd.concat([data_df, uac_df]).reset_index(drop=True)
                else:
                    dial_112_df = api_export(dial_112)
                    if dial_112_df.shape[1] > 1:
                        data_df = pd.concat([data_df, dial_112_df]).reset_index(drop=True)
    lock.acquire()
    try:
        result_dfs[thread_id] = data_df
    finally:
        lock.release()

for cluster in ['East', 'West']:
    excel_df = pd.read_excel(filename, sheet_name=cluster, usecols='C, F')
    range_step = excel_df.shape[0] / no_of_threads
    result_dfs = [None] * no_of_threads
    threads = []
    for i in range(no_of_threads):
        extract_df = excel_df.iloc[int(i * range_step):int((i + 1) * range_step)].reset_index(drop=True)
        thread = threading.Thread(target=assign_server, args=(cluster, extract_df, i, result_dfs, threading.Lock(),))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    merged_df = pd.concat(result_dfs, ignore_index=True)
    merged_df.to_excel('C:\\Users\\singh_kislay\\Desktop\\' + cluster + ' Call Details.xlsx', index=False)
# endregion