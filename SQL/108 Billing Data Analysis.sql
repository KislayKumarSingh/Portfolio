-- 108 Billing Data Analysis SQL Server Stored Procedures & Functions
/********************************************************************/

-- VIP Overlapping Cases
select a.vehicle_number,a.id as 'Overlapping ID',a.[Start_Date] as 'OG_Start_Date',a.[End_Date] as 'OG_End_Date',
b.id as 'Overlapped ID',b.[Start_Date] as 'OD_Start_Date',b.[End_Date] as 'OD_End_Date'
from [Billing108].[dbo].[vip_duties] a
join [Billing108].[dbo].[vip_duties] b
on a.vehicle_number=b.vehicle_number
where a.id < b.id
and a.[Start_Date] <= b.End_Date
and a.End_Date >= b.[Start_Date]
order by a.vehicle_number,a.[Start_Date];
GO

-- RTNM Amount
alter function rtnm(@start_date date, @end_date date)
returns table as return
	select CONVERT(date,ambulance_assignment_time) as 'Date', SUM(DelayResponsetimeMinute)*60 as 'Amount'
	from [Billing108].[dbo].[cad_raw_data]
	where ambulance_assignment_time between CONCAT(@start_date,' 00:00:00') AND CONCAT(@end_date,' 23:59:59')
	and DelayResponsetimeMinute>0
	group by CONVERT(date,ambulance_assignment_time);	
GO

-- Benef. Contact No. in more than 2 Districts
alter function contact_number()
returns table as return
(
	WITH DistrictFirstOccurrence AS
	(
		SELECT beneficary_contact_number, benficiary_district, MIN(ambulance_assignment_time) AS FirstSeen
		FROM [Billing108].[dbo].[cad_raw_data]
		WHERE ambulance_assignment_time between '2026-08-01 00:00:00' AND '2026-08-31 23:59:59'
		and beneficary_contact_number <> ''
		AND benficiary_district <> ''
		GROUP BY beneficary_contact_number, benficiary_district
	), 
	RankedDistricts AS
	(
		SELECT beneficary_contact_number, benficiary_district, 
		ROW_NUMBER() OVER(PARTITION BY beneficary_contact_number ORDER BY FirstSeen) AS DistrictRank
		FROM DistrictFirstOccurrence
	)
	SELECT 'Benef. Contact No. in more than 2 Districts' as Observation, crd.Cluster,
	crd.incident_id,crd.ambulance_assignment_time,crd.beneficary_contact_number,crd.benficiary_district,
	crd.is_mci,crd.[Source of Distance],crd.case_type_name,crd.map_distance,crd.update_from,crd.Level1_end_call_time
	FROM [Billing108].[dbo].[cad_raw_data] crd
	INNER JOIN RankedDistricts rd 
	ON rd.beneficary_contact_number = crd.beneficary_contact_number AND rd.benficiary_district = crd.benficiary_district
	WHERE crd.ambulance_assignment_time between '2026-08-01 00:00:00' AND '2026-08-31 23:59:59'
	and rd.DistrictRank > 2
);
GO

select * from contact_number();
GO

-- Exceptional Cases (CPED Standard Remarks)
alter function exceptional_cases(@start_date_time as datetime, @end_date_time as datetime)
returns table as return
	select distinct [Incident Id],Observation,[Standard Remarks]
	from
	(
		select Observation,[Incident Id],[Standard Remarks],
		ROW_NUMBER() over(partition by [Incident Id],Observation order by [Insert Timestamp] desc) as Row_Order
		from [172.16.108.188].[CPED_Master].[dbo].[EMTS_ExceptionalCases]
		where [Assign Time] between @start_date_time and @end_date_time
	) as alias
	where Row_Order=1;
GO

-- Pending Cases
alter function pending_cases(@start_date date,@end_date date,@scope varchar(50))
returns table as return
	select a.Observation,crd.incident_id,crd.ambulance_assignment_time,crd.Cluster,crd.is_mci,crd.[Source of Distance],crd.case_type_name,
	crd.map_distance,crd.update_from,crd.Level1_end_call_time,a.Scope,a.[Standard Remarks],Day(crd.ambulance_assignment_time) as 'Day',
	case when crd.is_mci=0 then 1 else 0 end as 'Non MCI',ql.is_critical
	from [Billing108].[dbo].[cad_raw_data_anomaly] a
	inner join [Billing108].[dbo].[cad_raw_data] crd on a.[Incident ID]=crd.incident_id
	inner join [Billing108].[dbo].[Billing Process Queries List] ql on a.Observation=ql.Observation
	where a.[Insert Date]=(select max([Insert Date]) from [Billing108].[dbo].[cad_raw_data_anomaly])
	and a.[Ambulance Assignment Time] between CONCAT(@start_date,' 00:00:00') AND CONCAT(@end_date,' 23:59:59')
	and 
	(
		-- (Scopes - 'IT IS,RTNM Desk,ERC')
		a.Scope in (SELECT value FROM STRING_SPLIT(@scope, ','))
		or
		(
			a.Scope='IT IS / CPED'
			and 
			(	
				a.[Standard Remarks] is null 
				or (a.[Standard Remarks]='ok as per Manual' and crd.[Source of Distance]='Gps')
				or (a.[Standard Remarks]='ok as per GPS' and crd.[Source of Distance]='Manual')
				-- 'Escalated Case (Case Overlap)' & 'UAD Case' are UAD Cases (automatic process in Billing_Data_Analysis.py)
				-- 'Case Done Within VIP Duty' is VIP Overlapping Case, resolved by Vishesh & will be available in 'VIP Duty Overlap' sheet of Analysis file
				-- Rest of the remarks are acceptable except 'ok as per GPS' & 'ok as per Manual' which are handled in the above two OR conditions
				or a.[Standard Remarks] not in('Escalated Case (Case Overlap)','Case Done Within VIP Duty','As per EMT Vehicle was on road during assignment',
												'ok(ERC Reattempt post Analysis-Non Critical anomaly)','ok as per Exception','ok as per GPS','ok as per Manual',
												'ok as per RTNM Desk','Short Distance Case','Short Duration Case','UAD case','Wrong EM Type','Off road before hospital reach')
				or (a.Observation in(select Observation from [Billing108].[dbo].[Billing Process Queries List] where is_critical='Yes' and Scope='IT IS / CPED'))
			)
		)
	);
GO

-- GPS Manual Summary
ALTER PROCEDURE gps_manual_summary(@start_date DATE, @end_date DATE) AS
BEGIN
    
    SET NOCOUNT ON;

    SELECT
        vehicle_number,
        DAY(ambulance_assignment_time) AS ReportDay,
        [Source of Distance]
    INTO #BaseData
    FROM Billing108.dbo.cad_raw_data
    WHERE ambulance_assignment_time BETWEEN CONCAT(@start_date,' 00:00:00') AND CONCAT(@end_date,' 23:59:59');

    DECLARE @columns NVARCHAR(MAX);
    DECLARE @select_columns NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);

    SELECT
        @columns =
			STRING_AGG(QUOTENAME(ReportDay), ',')
            WITHIN GROUP (ORDER BY ReportDay),

        @select_columns =
            STRING_AGG('MP.' + QUOTENAME(ReportDay), ',')
            WITHIN GROUP (ORDER BY ReportDay)
    FROM
    (
        SELECT DISTINCT ReportDay
        FROM #BaseData
    ) D;

    SET @sql = '
        WITH VehicleSummary AS
        (
            SELECT
                vehicle_number,
                COUNT(*) AS Total,
                SUM(CASE
                        WHEN [Source of Distance] = ''Manual''
                        THEN 1
                        ELSE 0
                    END) AS Manual
            FROM #BaseData
            GROUP BY vehicle_number
        ),
        ManualPivot AS
        (
            SELECT *
            FROM
            (
                SELECT
                    vehicle_number,
                    ReportDay,
                    COUNT(*) AS ManualCount
                FROM #BaseData
                WHERE [Source of Distance] = ''Manual''
                GROUP BY vehicle_number, ReportDay
            ) M
            PIVOT
            (
                SUM(ManualCount)
                FOR ReportDay IN (' + @columns + ')
            ) P
        )
        SELECT
            VS.vehicle_number AS [Vehicle],
            VS.Total,
            VS.Total - VS.Manual AS GPS,
            VS.Manual,
            ROUND(VS.Manual * 100.0 / NULLIF(VS.Total,0),2) AS [Manual %],
            ' + @select_columns + '
        FROM VehicleSummary VS
        LEFT JOIN ManualPivot MP ON VS.vehicle_number = MP.vehicle_number
        WHERE (VS.Manual * 100.0) / NULLIF(VS.Total,0) >= 5
        ORDER BY [Manual %] DESC;
    ';

    EXEC sp_executesql @sql;

END;
GO

exec gps_manual_summary '2026-07-01','2026-07-31';

-- Manual Cases Analysis
WITH VehicleStats AS
(
    SELECT vehicle_number
    FROM [Billing108].[dbo].[cad_raw_data]
	-- below date should always be month start & end date to caluclate % of manual cases
    WHERE ambulance_assignment_time between '2026-07-01 00:00:00' AND '2026-07-31 23:59:59'
    GROUP BY vehicle_number
    HAVING SUM(CASE WHEN [Source of Distance] = 'Manual' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 5
)
SELECT crd.incident_id, crd.ambulance_assignment_time, crd.vehicle_number, mpd.Insert_Timestamp AS [CPED Manual Data Insert]
FROM [Billing108].[dbo].[cad_raw_data] crd
INNER JOIN VehicleStats vs ON crd.vehicle_number = vs.vehicle_number
LEFT JOIN [172.16.108.188].[CPED_Master].[dbo].[Manual_PCR_Data] mpd ON crd.incident_id = mpd.[Incident Id]
WHERE crd.ambulance_assignment_time between '2026-07-01 00:00:00' AND '2026-07-31 23:59:59'
AND crd.[Source of Distance] = 'Manual'
--AND mpd.[Incident Id] IS not NULL
ORDER BY crd.ambulance_assignment_time;

-- Average Trip KMs per Ambulance per Day
select round((sum(Total_gps_trip_kms)/2200.0/30),2) as 'Avg Trip KMs per Amby per Day' 
from [Billing108].[dbo].[cad_raw_data]
where ambulance_assignment_time between '2026-01-01 00:00:00' AND '2026-01-30 23:59:59';

-- Execute Billing Data Analysis
exec Billing_Data_Analysis '2026-07-26','2026-07-26','Manual';
exec Billing_Data_Analysis '2026-01-28','2026-01-29','Manual','crd_km';
GO

-- Billing Data Analysis
alter procedure Billing_Data_Analysis(@start_date as date, @end_date as date, @process_type as varchar(9), @table_name as varchar(100)='cad_raw_data') as
begin
SET NOCOUNT ON;

declare @start_date_time as datetime = CONCAT(@start_date,' 00:00:00');
declare @end_date_time as datetime = CONCAT(@end_date,' 23:59:59');
declare @DV_end_date as datetime = DATEADD(day,1,@end_date_time);

declare @query as varchar(max);

-- Deleting UAD Cases
set @query = '
	delete
	from [Billing108].[dbo].' + QUOTENAME(@table_name) + '
	where incident_id in
	(
		select incident_id from [Billing108].[dbo].[uad_cases]
	);
';
Exec (@query);

-- Temporary Table Creation
drop table if exists #temp_table;

select * 
into #temp_table
from [Billing108].[dbo].[cad_raw_data]
where 1=0;

set @query = concat('
	insert into #temp_table
	select * 
	from [Billing108].[dbo].' , QUOTENAME(@table_name) , '
	where ambulance_assignment_time between ''' , @start_date , ' 00:00:00'' and ''' , @end_date , ' 23:59:59'';
');

Exec (@query);

-- Report Table Creation
drop table if exists #report_table;

create table #report_table
(
	Observation varchar(200), "Incident ID" bigint, "Ambulance Assignment Time" datetime, "Cluster Name" varchar(5), "is mci" TINYINT,
	"Source of Distance" varchar(10), "Case Type" varchar(15), "Map Distance" float, "Update From" varchar(30), "Call End" datetime
);

-- 1-B2S GPS Null
insert into #report_table 
SELECT 'B2S GPS Null',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE base_to_scene_gps_km IS NULL;

-- 2-S2H GPS Null
insert into #report_table 
SELECT 'S2H GPS Null',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE scene_to_hsptl_gps_km IS NULL;

-- 3-H2B GPS Null
insert into #report_table 
SELECT 'H2B GPS Null',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE hsptl_to_base_gps_km IS NULL;

-- 4-Call Start Null
insert into #report_table 
SELECT 'Call Start Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE creation_date IS NULL;

-- 5-Call End Null
insert into #report_table 
SELECT 'Call End Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Level1_end_call_time IS NULL;

-- 6-Assignment Null
insert into #report_table 
SELECT 'Assignment Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE ambulance_assignment_time IS NULL;

-- 7-Departure Null
insert into #report_table 
SELECT 'Departure Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_base_start_time IS NULL;

-- 8-Pickup reach Null
insert into #report_table 
SELECT 'Pickup reach Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_reach_time IS NULL;
 
-- 9-Pickup depart Null
insert into #report_table 
SELECT 'Pickup depart Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_departure_time IS NULL;

-- 10-Destination reach Null
insert into #report_table 
SELECT 'Destination reach Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_reach_time IS NULL;

-- 11-Destination depart Null
insert into #report_table 
SELECT 'Destination depart Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_depart_time IS NULL;

-- 12-Base reach Null
insert into #report_table 
SELECT 'Base reach Null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_base_reach_time IS NULL;

-- 13-Call Start > Call end
insert into #report_table 
SELECT 'Call Start > Call end',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE creation_date > Level1_end_call_time;

-- 14-Call Start = Call end
insert into #report_table 
SELECT 'Call Start = Call end',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE creation_date = Level1_end_call_time and is_mci=0;

-- 15-Call End < Assignment
insert into #report_table 
SELECT 'Call End < Assignment',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Level1_end_call_time < ambulance_assignment_time;

-- 16-Call End = Assignment
insert into #report_table 
SELECT 'Call End = Assignment',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Level1_end_call_time = ambulance_assignment_time and is_mci=0;

-- 17-Assignment > Departure
insert into #report_table 
SELECT 'Assignment > Departure',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE ambulance_assignment_time > Ambulance_base_start_time;

-- 18-Assignment = Departure
insert into #report_table 
SELECT 'Assignment = Departure',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE ambulance_assignment_time = Ambulance_base_start_time;

-- 19-Base depart > Pickup reach
insert into #report_table
SELECT 'Base depart > Pickup reach',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_base_start_time > Ambulance_pickup_point_reach_time;

-- 20-Base depart = Pickup reach (EM)
insert into #report_table 
SELECT 'Base depart = Pickup reach (EM)',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE case_type_name='EMERGENCY' and Ambulance_base_start_time = Ambulance_pickup_point_reach_time;

-- 21-Pickup reach < Assignment
insert into #report_table 
SELECT 'Pickup reach < Assignment',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_reach_time < ambulance_assignment_time;

-- 22-Pickup reach > Pickup depart
insert into #report_table 
SELECT 'Pickup reach > Pickup depart',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_reach_time > Ambulance_pickup_point_departure_time;

-- 23-Pickup reach = Pickup depart (EM)
insert into #report_table 
SELECT 'Pickup reach = Pickup depart (EM)',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE case_type_name='EMERGENCY' and Ambulance_pickup_point_reach_time = Ambulance_pickup_point_departure_time;

-- 24-Pickup depart > Destination reach
insert into #report_table 
SELECT 'Pickup depart > Destination reach',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_departure_time > Ambulance_destination_reach_time;

-- 25-Pickup depart = Destination reach
insert into #report_table 
SELECT 'Pickup depart = Destination reach',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_departure_time = Ambulance_destination_reach_time;

-- 26-Destination reach > Destination depart
insert into #report_table 
SELECT 'Destination reach > Destination depart',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_reach_time > Ambulance_destination_depart_time;

-- 27-Destination reach = Destination depart
insert into #report_table 
SELECT 'Destination reach = Destination depart',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_reach_time = Ambulance_destination_depart_time;

-- 28-Destination depart > Base reach
insert into #report_table 
SELECT 'Destination depart > Base reach',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_depart_time > Ambulance_base_reach_time;

-- 32-B2S speed > 90 KM/h
insert into #report_table 
SELECT 'B2S speed > 90 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_base_start_time <> Ambulance_pickup_point_reach_time and
(base_to_scene_gps_km/(DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time)/3600.0)) > 90;

-- 33-S2H speed > 90 KM/h
insert into #report_table 
SELECT 'S2H speed > 90 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_departure_time <> Ambulance_destination_reach_time  and
(scene_to_hsptl_gps_km/(DATEDIFF(SECOND,Ambulance_pickup_point_departure_time,Ambulance_destination_reach_time)/3600.0)) > 90;

-- 34-H2B speed > 90 KM/h
insert into #report_table 
SELECT 'H2B speed > 90 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_destination_depart_time <> Ambulance_base_reach_time and
(hsptl_to_base_gps_km/(DATEDIFF(SECOND,Ambulance_destination_depart_time,Ambulance_base_reach_time)/3600.0)) > 90;

-- 36-Improper age format
insert into #report_table 
select 'Improper age format',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table 
where Age is null or Age not in(
'1 DAYS','1 MONTHS','1 YEARS','10 DAYS','10 MONTHS','10 YEARS','100 YEARS','11 DAYS','11 MONTHS','11 YEARS','12 DAYS',
'12 MONTHS','12 YEARS','13 DAYS','13 YEARS','14 DAYS','14 YEARS','15 DAYS','15 YEARS','16 DAYS','16 YEARS','17 DAYS',
'17 YEARS','18 DAYS','18 YEARS','19 DAYS','19 YEARS','2 DAYS','2 MONTHS','2 YEARS','20 DAYS','20 YEARS','21 DAYS',
'21 YEARS','22 DAYS','22 YEARS','23 DAYS','23 YEARS','24 DAYS','24 YEARS','25 DAYS','25 YEARS','26 DAYS','26 YEARS',
'27 DAYS','27 YEARS','28 DAYS','28 YEARS','29 DAYS','29 YEARS','3 DAYS','3 MONTHS','3 YEARS','30 DAYS','30 YEARS',
'31 DAYS','31 YEARS','32 YEARS','33 YEARS','34 YEARS','35 YEARS','36 YEARS','37 YEARS','38 YEARS','39 YEARS','4 DAYS','4 MONTHS',
'4 YEARS','40 YEARS','41 YEARS','42 YEARS','43 YEARS','44 YEARS','45 YEARS','46 YEARS','47 YEARS','48 YEARS','49 YEARS',
'5 DAYS','5 MONTHS','5 YEARS','50 YEARS','51 YEARS','52 YEARS','53 YEARS','54 YEARS','55 YEARS','56 YEARS','57 YEARS',
'58 YEARS','59 YEARS','6 DAYS','6 MONTHS','6 YEARS','60 YEARS','61 YEARS','62 YEARS','63 YEARS','64 YEARS','65 YEARS',
'66 YEARS','67 YEARS','68 YEARS','69 YEARS','7 DAYS','7 MONTHS','7 YEARS','70 YEARS','71 YEARS','72 YEARS','73 YEARS',
'74 YEARS','75 YEARS','76 YEARS','77 YEARS','78 YEARS','79 YEARS','8 DAYS','8 MONTHS','8 YEARS','80 YEARS','81 YEARS',
'82 YEARS','83 YEARS','84 YEARS','85 YEARS','86 YEARS','87 YEARS','88 YEARS','89 YEARS','9 DAYS','9 MONTHS','9 YEARS',
'90 YEARS','91 YEARS','92 YEARS','93 YEARS','94 YEARS','95 YEARS','96 YEARS','97 YEARS','98 YEARS','99 YEARS');

-- 37-Pregnancy = Male
insert into #report_table 
SELECT 'Pregnancy = Male',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Call_Type LIKE '%pregnancy%' AND Gender='Male';

-- 38-Pickup reach in less than 1 Min. (EM)
insert into #report_table   
SELECT 'Pickup reach in less than 1 Min. (EM)',incident_id,ambulance_assignment_time,Cluster,is_mci,  
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time  
FROM #temp_table  
WHERE case_type_name = 'EMERGENCY'
and DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time) < 60;

-- 39-Destination reach in less than 1 Min from pickup
insert into #report_table 
SELECT 'Destination reach in less than 1 Min from pickup',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE DATEDIFF(SECOND,Ambulance_pickup_point_departure_time,Ambulance_destination_reach_time) < 60
and scene_to_hsptl_gps_km >= 0.3;

-- 41-Call duration >= 30 Min
insert into #report_table 
SELECT 'Call duration >= 30 Min',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE DATEDIFF(SECOND,creation_date,Level1_end_call_time) >= 1800;

-- 43-Pickup reach before call end
insert into #report_table 
SELECT 'Pickup reach before call end',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_reach_time < Level1_end_call_time;

-- 44-Destination Hospital is null
insert into #report_table 
SELECT 'Destination Hospital is null',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Destination_hospital IS NULL or TRIM(Destination_hospital)='';

-- 45-B2S > 0 KM but travel time = 0
insert into #report_table 
SELECT 'B2S > 0 KM but travel time = 0',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE base_to_scene_gps_km > 0 AND DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time) = 0;

-- 46-S2H > 0 KM but travel time = 0
insert into #report_table 
SELECT 'S2H > 0 KM but travel time = 0',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE scene_to_hsptl_gps_km > 0 AND DATEDIFF(SECOND,Ambulance_pickup_point_departure_time,Ambulance_destination_reach_time) = 0;

-- 47-H2B > 0 KM but travel time = 0
insert into #report_table 
SELECT 'H2B > 0 KM but travel time = 0',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE hsptl_to_base_gps_km > 0 AND DATEDIFF(SECOND,Ambulance_destination_depart_time,Ambulance_base_reach_time) = 0;

-- 48-GPS & Lat-Long not available
insert into #report_table 
SELECT 'GPS & Lat-Long not available',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE latitude IS NULL OR longitude IS NULL;

-- 49-S2H < 0.3 KM
insert into #report_table 
SELECT 'S2H < 0.3 KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE scene_to_hsptl_gps_km < 0.3;

-- 50-B2S = 0 KM (EM)
insert into #report_table   
SELECT 'B2S = 0 KM (EM)',incident_id,ambulance_assignment_time,Cluster,  
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time  
FROM #temp_table
WHERE ((base_to_scene_gps_km < 0.3) or (base_to_scene_gps_km < 1 and CONVERT(float,map_distance)>1))
and case_type_name = 'EMERGENCY';

-- 53-Date variation in and after Assignment
insert into #report_table 
select 'Date variation in and after Assignment',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where ambulance_assignment_time not between @start_date_time and @end_date_time
or Ambulance_base_start_time not between @start_date_time and @DV_end_Date
or Ambulance_pickup_point_reach_time not between @start_date_time and @DV_end_Date
or Ambulance_pickup_point_departure_time not between @start_date_time and @DV_end_Date
or Ambulance_destination_reach_time not between @start_date_time and @DV_end_Date
or Ambulance_destination_depart_time not between @start_date_time and @DV_end_Date
or Ambulance_base_reach_time not between @start_date_time and @DV_end_Date;

-- 56-Total KM Mismatch
insert into #report_table 
select 'Total KM Mismatch',incident_id,ambulance_assignment_time,Cluster,is_mci,
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where round(base_to_scene_gps_km + scene_to_hsptl_gps_km + hsptl_to_base_gps_km,2) <> round(Total_gps_trip_kms,2);

-- 59-GPS link missing in GPS trip
insert into #report_table
select 'GPS link missing in GPS trip',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Hyperlink_tab is null or LEN(TRIM(Hyperlink_tab)) < 90;

-- 60-PCR not uploaded
insert into #report_table 
select 'PCR not uploaded',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where pcr_upload is Null or LEN(TRIM(pcr_upload)) < 30;

-- 66-Negative KMs
insert into #report_table
select 'Negative KMs',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where base_to_scene_gps_km<0 or scene_to_hsptl_gps_km<0 or hsptl_to_base_gps_km<0 or Total_gps_trip_kms<0 or [Total Trip Kilometer]<0;

-- 69-Call Start = Assignment
insert into #report_table
select 'Call Start = Assignment',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where creation_date=ambulance_assignment_time and is_mci=0;

-- 71-S2B GPS KM is available
insert into #report_table
SELECT 'S2B GPS KM is available',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where scene_to_base_gps_km>0 or scene_to_base_gps_km<0;

-- 78-RTNM > 0 Min
insert into #report_table
select 'RTNM > 0 Min',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where DelayResponsetimeMinute>0;

-- 83-Call Reference ID Null
insert into #report_table
SELECT 'Call Reference ID Null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where callreferenceid is null or LEN(TRIM(callreferenceid))<>20;

-- 85-Improper District Name
insert into #report_table
select 'Improper District Name',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where vehicle_base_district is null
or vehicle_base_district not in
(
	'Agra','Aligarh','Ambedkar Nagar','Amethi','Amroha','Auraiya','Ayodhya','Azamgarh','Baghpat','Bahraich','Ballia',
	'Balrampur','Banda','Barabanki','Bareilly','Basti','Bhadohi','Bijnor','Budaun','Bulandshahr','Chandauli','Chitrakoot',
	'Deoria','Etah','Etawah','Farrukhabad','Fatehpur','Firozabad','Gautam Buddha Nagar','Ghaziabad','Ghazipur','Gonda',
	'Gorakhpur','Hamirpur','Hapur','Hardoi','Hathras','Jalaun','Jaunpur','Jhansi','Kannauj','Kanpur Dehat','Kanpur Nagar',
	'Kasganj','Kaushambi','Kushinagar','Lakhimpur Kheri','Lalitpur','Lucknow','Mahoba','Maharajganj','Mainpuri','Mathura',
	'Mau','Meerut','Mirzapur','Moradabad','Muzaffarnagar','Pilibhit','Pratapgarh','Prayagraj','Rae Bareli','Rampur',
	'Saharanpur','Sambhal','Sant Kabeer Nagar','Shahjahanpur','Shamli','Shravasti','Siddharthnagar','Sitapur','Sonbhadra',
	'Sultanpur','Unnao','Varanasi'
);

-- 86-Total Trip KM < 1
insert into #report_table
SELECT 'Total Trip KM < 1',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Total_gps_trip_kms<1;

-- 87-Male Patient Admitted in Female Hospital
insert into #report_table
SELECT 'Male Patient Admitted in Female Hospital',incident_id,ambulance_assignment_time,
Cluster,is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Gender='MALE' AND 
(
	Destination_hospital LIKE '%women%'
	OR Destination_hospital LIKE '%woman%'
	OR Destination_hospital LIKE '%mahila%'
	OR Destination_hospital LIKE '%dwh%'
	OR Destination_hospital LIKE '%female%'
	OR Destination_hospital LIKE '%maternity%'
	OR Destination_hospital LIKE '%mch%'
	OR Destination_hospital LIKE 'RLB Hospital Lucknow'
	OR Destination_hospital LIKE 'Dufferin Hospital'
	OR Destination_hospital LIKE 'MEDICAL COLLEGE ( QUEEN MARRY ) LUCKNOW'
)
AND Destination_hospital NOT LIKE '%CHC%' and Destination_hospital NOT LIKE '%PHC%' 
AND (Age NOT IN('1 YEARS','2 YEARS','3 YEARS','4 YEARS'))
AND Age NOT LIKE '%Day%' AND Age NOT LIKE '%Month%';

-- 89-Response time Null
insert into #report_table
SELECT 'Response time Null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where [Response time] is null or LEN([Response time]) not between 8 and 9;

-- 90-Delay in Response time Null
insert into #report_table
SELECT 'Delay in Response time Null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where [Delay in Response time] is null or TRIM([Delay in Response time])='';

-- 91-DelayResponsetimeMinute Null
insert into #report_table
SELECT 'DelayResponsetimeMinute Null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DelayResponsetimeMinute is null or DelayResponsetimeMinute < 0;

-- 92-Call Duration < 40 seconds
insert into #report_table
SELECT 'Call Duration < 40 seconds',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATEDIFF(second,creation_date,Level1_end_call_time)<40 and is_mci=0;

-- 93-Improper Beneficiary Contact Number
insert into #report_table
SELECT 'Improper Beneficiary Contact Number',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where LEN(CONVERT(bigint,beneficary_contact_number)) not in (1,10) or beneficary_contact_number is null 
or (TRIM(beneficary_contact_number) <> '' AND TRIM(beneficary_contact_number) NOT LIKE '%[^0]%');

-- 94-Improper Caller Number
insert into #report_table
SELECT 'Improper Caller Number',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where LEN(CONVERT(bigint,Phone_no_of_the_Caller)) <> 10 or Phone_no_of_the_Caller is null;

-- 95-PHC/Sub Center Hospital
insert into #report_table
select 'PHC/Sub Center Hospital',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where Destination_hospital like '%sub%cen%' or Destination_hospital like '%PHC%';

-- 96-PHC Hospital Category
insert into #report_table
select 'PHC Hospital Category',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where Hospital_category like '%PHC%';

-- 97-Adult Admitted in Children Hospital
insert into #report_table
select 'Adult Admitted in Children Hospital',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where Destination_hospital='Sarojini Naidu Children Hospital'
and (Age is null or Age not in(
'1 DAYS','1 MONTHS','1 YEARS','10 DAYS','10 MONTHS','10 YEARS','11 DAYS','11 MONTHS','11 YEARS','12 DAYS',
'12 MONTHS','12 YEARS','13 DAYS','14 DAYS','15 DAYS','16 DAYS','17 DAYS','18 DAYS','19 DAYS','2 DAYS','2 MONTHS',
'2 YEARS','20 DAYS','21 DAYS','22 DAYS','23 DAYS','24 DAYS','25 DAYS','26 DAYS','27 DAYS','28 DAYS','29 DAYS',
'3 DAYS','3 MONTHS','3 YEARS','30 DAYS','4 DAYS','4 MONTHS','4 YEARS','5 DAYS','5 MONTHS','5 YEARS','6 DAYS',
'6 MONTHS','6 YEARS','7 DAYS','7 MONTHS','7 YEARS','8 DAYS','8 MONTHS','8 YEARS','9 DAYS','9 MONTHS','9 YEARS'));

-- 101-IFT H2B < 1 KM
insert into #report_table
SELECT 'IFT H2B < 1 KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE hsptl_to_base_gps_km < 1 and case_type_name = 'IFT';

-- 102-At Scene > 2 KM
insert into #report_table
select 'At Scene > 2 KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where [Source of Distance]='Gps' and at_scene_gps_km > 2;

-- 103-At Hospital > 2 KM
insert into #report_table
select 'At Hospital > 2 KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where [Source of Distance]='Gps' and at_hospital_gps_km > 2;

-- 104-UAD from mobile app
insert into #report_table
select 'UAD from mobile app',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from #temp_table
where beneficary_trip_uad > 0;

-- 105-S2H >= 0.3 to <=1 KM
insert into #report_table 
SELECT 'S2H >= 0.3 to <=1 KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE scene_to_hsptl_gps_km >= 0.3 and scene_to_hsptl_gps_km <= 1;

-- 106-Status on PCR = Unavailed
insert into #report_table 
SELECT 'Status on PCR = Unavailed',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE avail_status_on_pcr='UNAVAILED';

-- 107-S2H speed < 5 KM/h
insert into #report_table 
SELECT 'S2H speed < 5 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE Ambulance_pickup_point_departure_time <> Ambulance_destination_reach_time and
(scene_to_hsptl_gps_km/(DATEDIFF(SECOND,Ambulance_pickup_point_departure_time,Ambulance_destination_reach_time)/3600.0)) < 5;

-- 108-IFT(B2S 0) High Response Time
insert into #report_table 
SELECT 'IFT(B2S 0) High Response Time',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
WHERE case_type_name='IFT'
and base_to_scene_gps_km<0.3
and DATEDIFF(SECOND,Level1_end_call_time,Ambulance_pickup_point_reach_time)>300;

-- 109-Pickup reach in less than 29 Sec. (IFT)
insert into #report_table   
SELECT 'Pickup reach in less than 29 Sec. (IFT)',incident_id,ambulance_assignment_time,Cluster,is_mci,  
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time  
FROM #temp_table  
WHERE case_type_name = 'IFT'
and base_to_scene_gps_km < 0.3
and DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time) < 29;

-- 110-At Scene Duration < 29 seconds
insert into #report_table   
SELECT 'At Scene Duration < 29 seconds',incident_id,ambulance_assignment_time,Cluster,is_mci,  
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time  
FROM #temp_table  
WHERE DATEDIFF(second,Ambulance_pickup_point_reach_time,Ambulance_pickup_point_departure_time) < 29;

-- 111-At Hospital Duration < 29 seconds
insert into #report_table   
SELECT 'At Hospital Duration < 29 seconds',incident_id,ambulance_assignment_time,Cluster,is_mci,  
[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time  
FROM #temp_table  
WHERE DATEDIFF(second,Ambulance_destination_reach_time,Ambulance_destination_depart_time) < 29;

-- 112-Call Duration < 10 seconds
insert into #report_table
SELECT 'Call Duration < 10 seconds',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATEDIFF(second,creation_date,Level1_end_call_time)<10 and is_mci=0;

-- 118-B2S speed < 5 KM/h
insert into #report_table
SELECT 'B2S speed < 5 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Ambulance_base_start_time <> Ambulance_pickup_point_reach_time 
and (base_to_scene_gps_km/(DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time)/3600.0)) < 5
and DATEDIFF(SECOND,Ambulance_base_start_time,Ambulance_pickup_point_reach_time) > 1200;

-- 120-Call Start > Assignment
insert into #report_table
SELECT 'Call Start > Assignment',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where creation_date > ambulance_assignment_time;

-- 122-Total Case Duration <= 10 Min
insert into #report_table
SELECT 'Total Case Duration <= 10 Min',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATEDIFF(second,ambulance_assignment_time,Ambulance_base_reach_time)<=600;

-- 123-Total Trip KM >=1 and <=3
insert into #report_table
SELECT 'Total Trip KM >=1 and <=3',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Total_gps_trip_kms between 1 and 3;

-- 124-Improper Beneficiary Name
insert into #report_table
SELECT 'Improper Beneficiary Name',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Beneficiary_name is null or TRIM(Beneficiary_name)='' or Beneficiary_name like '%uad' or Beneficiary_name like '%snr%' 
or Beneficiary_name like '%s n r%' or Beneficiary_name like '%dead%' or Beneficiary_name like '%shift%' or Beneficiary_name like '%found%';

-- 125-Total Trip KM >= 70
insert into #report_table
SELECT 'Total Trip KM >= 70',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Total_gps_trip_kms>=70 and case_type_name='EMERGENCY';

 -- 126-Total Trip KM <= 10
insert into #report_table
SELECT 'Total Trip KM <= 10',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Total_gps_trip_kms<=10
and vehicle_base_district<>Destination_district;

-- 127-EMT Name Null
insert into #report_table
SELECT 'EMT Name Null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where emt_name is null or TRIM(emt_name)='' or emt_name not like '%[a-z]%';

-- 128-Pickup Location = Destination Hospital(IFT)
insert into #report_table
SELECT 'Pickup Location = Destination Hospital(IFT)',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where case_type_name='IFT'
and Pickup_Location=Destination_hospital;

-- 129-H2B KMs > 130% of B2S + S2H KMs
insert into #report_table
SELECT 'H2B KMs > 130% of B2S + S2H KMs',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where hsptl_to_base_gps_km > ((base_to_scene_gps_km + scene_to_hsptl_gps_km)*1.3)
and (Total_gps_trip_kms>25 or hsptl_to_base_gps_km>20);
--where hsptl_to_base_gps_km > ((base_to_scene_gps_km + scene_to_hsptl_gps_km)*2.0)
--and (Total_gps_trip_kms>25 or hsptl_to_base_gps_km>20);

-- 130-IFT H2B KM < 80% of S2H KM
insert into #report_table
SELECT 'IFT H2B KM < 80% of S2H KM',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where case_type_name = 'IFT'
and hsptl_to_base_gps_km < scene_to_hsptl_gps_km*0.8;

-- 131-Vehicle Number Issue
insert into #report_table
SELECT 'Vehicle Number Issue',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where vehicle_number is null or DATALENGTH(vehicle_number)<>10;

-- 132-Improper Gender
insert into #report_table
SELECT 'Improper Gender',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Gender is null or Gender not in ('FEMALE','MALE','TRANSGENDER');

-- 133-IFT Total Trip KM <=2
--insert into #report_table
--SELECT 'IFT Total Trip KM <=2',incident_id,ambulance_assignment_time,Cluster,
--is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
--FROM #temp_table
--where case_type_name='IFT' and Total_gps_trip_kms<=2;

-- 134-MCI case call type <> Mass Causality
insert into #report_table
SELECT 'MCI case call type <> Mass Causality',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where is_mci=1 and Call_Type <> 'Mass Causality';

-- 135-Improper Caller Name
insert into #report_table
SELECT 'Improper Caller Name',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATALENGTH(Name_of_the_caller) > 50;

-- 136-Assignment - Creation < 30 seconds
insert into #report_table
SELECT 'Assignment - Creation < 30 seconds',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATEDIFF(SECOND,creation_date,ambulance_assignment_time) < 30;

-- 137-Vehicle Base Location null
insert into #report_table
SELECT 'Vehicle Base Location null',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Vehicle_base_location is null or TRIM(Vehicle_base_location)='';

-- 138-H2B speed < 5 KM/h
insert into #report_table
SELECT 'H2B speed < 5 KM/h',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Ambulance_destination_depart_time <> Ambulance_base_reach_time 
and (hsptl_to_base_gps_km/(DATEDIFF(SECOND,Ambulance_destination_depart_time,Ambulance_base_reach_time)/3600.0)) < 5
and DATEDIFF(SECOND,Ambulance_destination_depart_time,Ambulance_base_reach_time) > 1800;

-- 139-Backup Vehicle Number Issue
insert into #report_table
SELECT 'Backup Vehicle Number Issue',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where DATALENGTH(backup_vehicle_number)<>10;

-- 140-Caller Number is Employee Phone Number
insert into #report_table
SELECT 'Caller Number is Employee Phone Number',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Phone_no_of_the_Caller in (select [Employee Phone Number] from [Billing108].[dbo].[Employee_Phone_Number]);

-- 141-Missing Pickup Location
insert into #report_table
SELECT 'Missing Pickup Location',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where Pickup_Location is null or TRIM(Pickup_Location)='';

-- 142-Vehicle District <> Hospital District (EM)
insert into #report_table
SELECT 'Vehicle District <> Hospital District (EM)',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where case_type_name = 'EMERGENCY' and vehicle_base_district <> Destination_district;

-- 143-Improper Pilot Mobile Number
insert into #report_table
SELECT 'Improper Pilot Mobile Number',incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
FROM #temp_table
where LEN(CONVERT(bigint,pilot_mobile_number)) not in (1,10) or pilot_mobile_number is null 
or (TRIM(pilot_mobile_number) <> '' AND TRIM(pilot_mobile_number) NOT LIKE '%[^0]%');

-- 30-Benef. Contact No. in more than 2 Districts
insert into #report_table
select Observation,incident_id,ambulance_assignment_time,Cluster,
is_mci,[Source of Distance],case_type_name,map_distance,update_from,Level1_end_call_time
from contact_number();

-- 42-Overlapping Case
drop table if exists #OC_temp_table;

create table #OC_temp_table
(
	"Overlapping ID" bigint, "Overlapping AT" datetime, "Overlapping BRT" datetime, update_from varchar(30), 
	"Overlapped ID" bigint, "Overlapped AT" datetime, "Overlapped BRT" datetime
);

set @query = '
	insert into #OC_temp_table
	select a.incident_id as ''Overlapping ID'',
	a.ambulance_assignment_time as ''Overlapping AT'',a.Ambulance_base_reach_time as ''Overlapping BRT'',a.update_from,
	b.incident_id as ''Overlapped ID'',b.ambulance_assignment_time as ''Overlapped AT'',b.Ambulance_base_reach_time as ''Overlapped BRT''
	from [Billing108].[dbo].' + QUOTENAME(@table_name) + ' a
	join [Billing108].[dbo].' + QUOTENAME(@table_name) + ' b
	on a.vehicle_number = b.vehicle_number
	where a.incident_id < b.incident_id
	and a.ambulance_assignment_time < b.Ambulance_base_reach_time
	and a.Ambulance_base_reach_time > b.ambulance_assignment_time;
';
Exec (@query);

insert into #report_table
select 'Overlapping Case',[Overlapping ID],[Overlapping AT],null,null,null,null,null,update_from,null
from #OC_temp_table;

-- 62-VIP Overlapping Case
drop table if exists #VOC_temp_table;

create table #VOC_temp_table
(
	incident_id bigint, Cluster varchar(4),vehicle_number varchar(10),ambulance_assignment_time datetime,Ambulance_base_reach_time datetime,
	update_from varchar(30),id int, "Start_Date" datetime, "End_Date" datetime
);

set @query = '
	insert into #VOC_temp_table
	select crd.incident_id,crd.Cluster,crd.vehicle_number,crd.ambulance_assignment_time,
	crd.Ambulance_base_reach_time,crd.update_from,vd.id,vd.[Start_Date],vd.[End_Date]
	from #temp_table crd
	inner join [Billing108].[dbo].[vip_duties] vd
	on crd.vehicle_number = vd.vehicle_number
	where crd.ambulance_assignment_time <= vd.[End_Date] 
	and crd.Ambulance_base_reach_time >= vd.[Start_Date];
';
Exec (@query);

insert into #report_table
select 'VIP Overlapping Case',incident_id,ambulance_assignment_time,null,null,null,null,null,update_from,null
from #VOC_temp_table;

-- 119-Vehicle offroad case overlap
drop table if exists #VOCO_temp_table;

create table #VOCO_temp_table
(
	incident_id bigint,vehicle_number varchar(10),ambulance_assignment_time datetime,Ambulance_base_reach_time datetime,
	update_from varchar(30),off_road_time datetime,on_road_time datetime
);

set @query = '
	insert into #VOCO_temp_table
	SELECT crd.incident_id,crd.vehicle_number,crd.ambulance_assignment_time,crd.Ambulance_base_reach_time,
	crd.update_from,od.off_road_time,od.Custom_on_road_time
	from #temp_table crd
	inner join [Billing108].[dbo].[offroad] od
	on crd.vehicle_number = od.vehicle_number
	where crd.backup_vehicle_number is null
	and crd.ambulance_assignment_time <= od.Custom_on_road_time 
	and crd.Ambulance_base_reach_time >= od.off_road_time;
';
Exec (@query);

insert into #report_table
select 'Vehicle offroad case overlap',incident_id,ambulance_assignment_time,null,null,null,null,null,update_from,null
from #VOCO_temp_table;

-- Exceptional Cases Table Creation
drop table if exists #exceptional_cases;
select * 
into #exceptional_cases
from [Billing108].[dbo].exceptional_cases(@start_date_time, @end_date_time);

-- Insert Data from Temporary Report Table to [Billing108].[dbo].[cad_raw_data_anomaly] Table
insert into [Billing108].[dbo].[cad_raw_data_anomaly]
select GETDATE() as [Insert Date],rt.Observation,rt.[Incident ID],rt.[Update From],
rt.[Ambulance Assignment Time],ql.Scope,rt.[Call End],ec.[Standard Remarks]
from #report_table rt
left join [Billing108].[dbo].[Billing Process Queries List] ql
on rt.Observation=ql.Observation collate SQL_Latin1_General_CP1_CI_AS
left join #exceptional_cases ec	
on rt.Observation=ec.Observation collate SQL_Latin1_General_CP1_CI_AS and rt.[Incident ID]=ec.[Incident Id];

-- Exporting data for Analysis Report
if @process_type='Manual'
begin
	-- Fetch Data from Report Table
	select rt.*,ql.Scope,ec.[Standard Remarks]
	from #report_table rt
	left join [Billing108].[dbo].[Billing Process Queries List] ql 
	on rt.Observation=ql.Observation
	left join #exceptional_cases ec	
	on rt.Observation=ec.Observation collate SQL_Latin1_General_CP1_CI_AS and rt.[Incident ID]=ec.[Incident Id]
	where rt.Observation not in ('Benef. Contact No. in more than 2 Districts','VIP Overlapping Case','Overlapping Case','Vehicle offroad case overlap')
	order by rt.Observation,rt.[Ambulance Assignment Time];

	-- Fetch Data from Overlapping Table
	select oc.[Overlapping ID],oc.[Overlapping AT],oc.[Overlapping BRT],oc.update_from,
	ec.[Standard Remarks],oc.[Overlapped ID],oc.[Overlapped AT],oc.[Overlapped BRT]
	from #OC_temp_table oc
	left join #exceptional_cases ec	
	on ec.Observation='Overlapping Case' and oc.[Overlapping ID]=ec.[Incident Id]
	where oc.[Overlapping AT] between @start_date_time and @end_date_time
	order by [Overlapping AT];

	-- Fetch Data from VIP Overlapping Table
	select voc.incident_id,voc.Cluster,voc.vehicle_number,voc.ambulance_assignment_time,voc.Ambulance_base_reach_time,
	voc.update_from,ec.[Standard Remarks],voc.id,voc.[Start_Date],voc.[End_Date]
	from #VOC_temp_table voc
	left join #exceptional_cases ec
	on ec.Observation='VIP Overlapping Case' and voc.incident_id=ec.[Incident Id]
	where voc.ambulance_assignment_time between @start_date_time and @end_date_time
	order by voc.ambulance_assignment_time;

	-- Fetch Data from Vehicle Offroad Case Overlap Table
	select voco.incident_id,voco.vehicle_number,voco.ambulance_assignment_time,voco.Ambulance_base_reach_time,
	voco.update_from,ec.[Standard Remarks],voco.off_road_time,voco.on_road_time
	from #VOCO_temp_table voco
	left join #exceptional_cases ec
	on ec.Observation='Vehicle offroad case overlap' and voco.incident_id=ec.[Incident Id]
	where voco.ambulance_assignment_time between @start_date_time and @end_date_time
	order by voco.ambulance_assignment_time;

end;
end;