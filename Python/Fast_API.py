# python -m uvicorn Billing.Fast_API:app --host 0.0.0.0 --port 8001

from sqlalchemy import create_engine
import pandas as pd
import uvicorn
from fastapi import Query
from typing_extensions import Annotated
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import Billing.Processes.Server_Connections as sc

security = HTTPBasic()
app = FastAPI(dependencies=[Depends(security)])
users = {"admin": {"password": "password", "token": "", "priviliged": True}}

def verify(creds: HTTPBasicCredentials = Depends(security)):
    username = creds.username
    password = creds.password
    if username in users and password == users[username]["password"]:
        print("User Validated")
        return True
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password", headers={"WWW-Authenticate": "Basic"},)

@app.get("/")
async def index(status = Depends(verify)):
   if status:
      return 'http://192.168.12.34:8001/'

@app.get("/freezed")
async def freezed(id: Annotated[str, Query(min_length=14, max_length=14)], status = Depends(verify)):
   if status:
      query = '''
         SELECT 
         incident_id,
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
         ambulance_assignment_time,
         Ambulance_base_start_time,
         Ambulance_pickup_point_reach_time,
         Ambulance_pickup_point_departure_time,
         Ambulance_destination_reach_time,
         Ambulance_destination_depart_time,
         Ambulance_base_reach_time,
         vehicle_number,
         update_from,
         DelayResponsetimeMinute,
         at_hospital_gps_km,
         at_scene_gps_km,
         backup_vehicle_number
         FROM test.cad_raw_data WHERE incident_id=''' + id + ''';
      '''
      df = pd.read_sql(query, con=create_engine(sc.connection(73)))
      data = {
         'incident_id': df['incident_id'][0],
         'Cluster': df['Cluster'][0],
         'vehicle_base_district': df['vehicle_base_district'][0],
         'is_mci': int(df['is_mci'][0]),
         'case_type_name': df['case_type_name'][0],
         'creation_date': df['creation_date'][0],
         'Level1_end_call_time': df['Level1_end_call_time'][0],
         'Source of Distance': df['Source of Distance'][0],
         'map_distance': df['map_distance'][0],
         'base_start_odo': int(df['base_start_odo'][0]),
         'pickup_reach_odo': int(df['pickup_reach_odo'][0]),
         'hsptl_reach_odo': int(df['hsptl_reach_odo'][0]),
         'hsptl_depart_odo': int(df['hsptl_depart_odo'][0]),
         'base_reach_odo': int(df['base_reach_odo'][0]),
         'base_to_scene_gps_km': df['base_to_scene_gps_km'][0],
         'scene_to_base_gps_km': df['scene_to_base_gps_km'][0],
         'scene_to_hsptl_gps_km': df['scene_to_hsptl_gps_km'][0],
         'hsptl_to_base_gps_km': df['hsptl_to_base_gps_km'][0],
         'Total_gps_trip_kms': df['Total_gps_trip_kms'][0],
         'ambulance_assignment_time': df['ambulance_assignment_time'][0],
         'Ambulance_base_start_time': df['Ambulance_base_start_time'][0],
         'Ambulance_pickup_point_reach_time': df['Ambulance_pickup_point_reach_time'][0],
         'Ambulance_pickup_point_departure_time': df['Ambulance_pickup_point_departure_time'][0],
         'Ambulance_destination_reach_time': df['Ambulance_destination_reach_time'][0],
         'Ambulance_destination_depart_time': df['Ambulance_destination_depart_time'][0],
         'Ambulance_base_reach_time': df['Ambulance_base_reach_time'][0],
         'vehicle_number': df['vehicle_number'][0],
         'update_from': df['update_from'][0],
         'DelayResponsetimeMinute': int(df['DelayResponsetimeMinute'][0]),
         'at_hospital_gps_km': df['at_hospital_gps_km'][0],
         'at_scene_gps_km': df['at_scene_gps_km'][0],
         'backup_vehicle_number': df['backup_vehicle_number'][0]
      }
      return data

if __name__ == "__main__":
   uvicorn.run("Fast_API:app", host="0.0.0.0", port=8001)