import threading
import numpy as np
import pandas as pd
import Billing.Call_Data.Testing_Extraction.Extract_Details as ed

filename = r"C:\Users\singh_kislay\Desktop\Call Details.xlsx"
no_of_threads = 50

def api_export(api_url):
    return pd.DataFrame([ed.run_api(api_url)])

def assign_server(cluster, df, thread_id, result_dfs):
    dfs = []
    for row in df.itertuples(index=False):
        reference_no = row.Av_CallHitReferenceNo
        call_start = row.CallStartTime
        source, live_db_log, uac, dial_112 = ed.api(cluster, call_start, reference_no)
        if not source:
            continue
        for api_url in (live_db_log, uac, dial_112):
            api_df = api_export(api_url)

            if api_df.shape[1] > 1:
                dfs.append(api_df)
                break
    if dfs:
        result_dfs[thread_id] = pd.concat(dfs, ignore_index=True)
    else:
        result_dfs[thread_id] = pd.DataFrame()

for cluster in ["East", "West"]:
    excel_df = pd.read_excel(filename, sheet_name=cluster, usecols="C,F")
    chunks = np.array_split(excel_df, no_of_threads)
    result_dfs = [None] * len(chunks)
    threads = []

    for thread_id, chunk in enumerate(chunks):
        chunk = chunk.reset_index(drop=True)
        thread = threading.Thread(target=assign_server, args=(cluster, chunk, thread_id, result_dfs))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    merged_df = pd.concat([df for df in result_dfs if df is not None and not df.empty], ignore_index=True)
    merged_df.to_excel(rf"C:\Users\singh_kislay\Desktop\{cluster} Call Details.xlsx", index=False)
