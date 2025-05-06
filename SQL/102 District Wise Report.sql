-- 102 District Wise Report
/**************************/
--Availed Cases

--Temporary Table Creation
declare @start_date as datetime,@end_date as datetime
set @start_date='2024-10-01 00:00:00'
set @end_date='2024-10-31 23:59:59'

drop table if exists #Availed
select d.[New District Name], m.[Call ID], m.[Call Types], m.[RTNM], m.[gps_kms], m.[B2S_GPS],
m.[Call Time], m.[TripId], m.[Trip No], m.[Hsp-District], m.[Benef District], m.[Destination-District]
into #Availed
from [102_Billing_Raw].[dbo].[118_Oct23_WD] m
left join [analytics].[dbo].[District] d
on m.[Amby-District]=d.[Old District Name] collate SQL_Latin1_General_CP1_CI_AS
where m.[Ambulance Assignment Time] between @start_date and @end_date
GO

declare @pickup_calltype as varchar(10),@dropback_calltype as varchar(10)
set @pickup_calltype='PICKUP'
set @dropback_calltype='DROPBACK';

--Main Query
with Main_Data as
(
	select [New District Name] as 'District',
	ROUND(SUM(gps_kms),2) as 'KM of Availed Trips',
	SUM(RTNM) as 'RTNM Minutes'
	from #Availed
	where [Trip No]=1
	group by [New District Name]
), Trip_Count as
(
	select [New District Name] as 'District',
	COUNT([Call ID]) as 'Trip Count'
	from #Availed
	where [Trip No]=1
	group by [New District Name]
), PU_RFP as
(
	select [New District Name] as 'District',
	COUNT([Call ID]) as 'Pick Up RFP'
	from #Availed
	where [Call Types]=@pickup_calltype
	group by [New District Name]
), DB_RFP as
(
	select alias.[New District Name] as 'District',
	sum(alias.[Drop Back RFP Trip Sum]) as 'Drop Back RFP'
	from
	(
		select [New District Name],
		coalesce(
		(case 
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=1 then 0.8
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=2 then 1.5
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=3 then 2
		end),0) 
		+
		coalesce(
		(case
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=1 then 1.6
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=2 then 3
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=3 then 4
		end),0) as 'Drop Back RFP Trip Sum'
		from #Availed
		where [Call Types]=@dropback_calltype
		group by [New District Name],[TripId]
	) alias
	group by alias.[New District Name]
), DB_1B_RFP as
(
	select alias.[New District Name] as 'District',
	sum(alias.[1B Drop Back RFP Trip Sum]) as '1B RFP'
	from
	(
		select [New District Name],
		coalesce((case when sum(case when [Hsp-District]=[Benef District] then 1 end)=1 then 0.8 end),0) +
		coalesce((case when sum(case when [Hsp-District]<>[Benef District] then 1 end)=1 then 1.6 end),0)
		as '1B Drop Back RFP Trip Sum'
		from #Availed
		where [Call Types]=@dropback_calltype
		group by [New District Name],[TripId]
	) alias
	group by alias.[New District Name]
), DB_2B_RFP as
(
	select alias.[New District Name] as 'District',
	sum(alias.[2B Drop Back RFP Trip Sum]) as '2B RFP'
	from
	(
		select [New District Name],
		coalesce((case when sum(case when [Hsp-District]=[Benef District] then 1 end)=2 then 1.5 end),0) +
		coalesce((case when sum(case when [Hsp-District]<>[Benef District] then 1 end)=2 then 3 end),0)
		as '2B Drop Back RFP Trip Sum'
		from #Availed
		where [Call Types]=@dropback_calltype
		group by [New District Name],[TripId]
	) alias
	group by alias.[New District Name]
), DB_3B_RFP as
(
	select alias.[New District Name] as 'District',
	sum(alias.[3B Drop Back RFP Trip Sum]) as '3B RFP'
	from
	(
		select [New District Name],
		coalesce((case when sum(case when [Hsp-District]=[Benef District] then 1 end)=3 then 2 end),0) +
		coalesce((case when sum(case when [Hsp-District]<>[Benef District] then 1 end)=3 then 4 end),0)
		as '3B Drop Back RFP Trip Sum'
		from #Availed
		where [Call Types]=@dropback_calltype
		group by [New District Name],[TripId]
	) alias
	group by alias.[New District Name]
), IFT_RFP as
(
	select alias.[New District Name] as 'District',
	SUM(alias.[IFT RFP Trip Sum]) as 'IFT RFP'
	from
	(
		select [New District Name],
			case 
				when [Hsp-District]=[Destination-District] then 1
				when [Hsp-District]<>[Destination-District] then 2
			end as 'IFT RFP Trip Sum'
		from #Availed
		where [Call Types]='IFT'
	) alias
	group by alias.[New District Name]
), B2S_10 as
(
	select [New District Name] as 'District', 
	COUNT([Call ID]) as 'B2S <= 10KM'
	from #Availed
	where [Trip No]=1 and (B2S_GPS<=10 or B2S_GPS is null)
	group by [New District Name]
), B2S_10_15 as
(
	select [New District Name] as 'District', 
	COUNT([Call ID]) as 'B2S (10-15KM)'
	from #Availed
	where [Trip No]=1 and B2S_GPS>10 and B2S_GPS<=15
	group by [New District Name]
), B2S_15 as
(
	select [New District Name] as 'District', 
	COUNT([Call ID]) as 'B2S > 15KM)'
	from #Availed
	where [Trip No]=1 and B2S_GPS>15
	group by [New District Name]
), RTNM_PU_RFP as 
(
	select [New District Name] as 'District',
	COUNT([Call ID]) as 'RTNM Pick Up RFP'
	from #Availed
	where [RTNM]>0 and [Call Types]=@pickup_calltype
	group by [New District Name]
), RTNM_DB_RFP as 
(
	select alias.[New District Name] as 'District',
	sum(alias.[RTNM Drop Back RFP Trip Sum]) as 'RTNM Drop Back RFP'
	from
	(
		select [New District Name],
		coalesce(
		(case 
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=1 then 0.8
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=2 then 1.5
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=3 then 2
		end),0) 
		+
		coalesce(
		(case
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=1 then 1.6
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=2 then 3
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=3 then 4
		end),0) as 'RTNM Drop Back RFP Trip Sum'
		from #Availed
		where [RTNM]>0 and [Call Types]=@dropback_calltype
		group by [New District Name],[TripId]
	) alias
	group by alias.[New District Name]
), RTNM_IFT_RFP as
(
	select alias.[New District Name] as 'District',
	SUM(alias.[IFT RFP Trip Sum]) as 'RTNM IFT RFP'
	from
	(
		select [New District Name],
			case 
				when [Hsp-District]=[Destination-District] then 1
				when [Hsp-District]<>[Destination-District] then 2
			end as 'IFT RFP Trip Sum'
		from #Availed
		where [RTNM]>0 and [Call Types]='IFT'
	) alias
	group by alias.[New District Name]
), RTNM_Trips_10 as
(
	select [New District Name] as 'District',
	COUNT(RTNM) as 'RTNM Trips in B2S <= 10KM' 
	from #Availed
	where [Trip No]=1 and RTNM>0 and B2S_GPS<=10
	group by [New District Name]
), RTNM_Trips_10_15 as
(
	select [New District Name] as 'District',
	COUNT(RTNM) as 'RTNM Trips in B2S (10-15KM)' 
	from #Availed
	where [Trip No]=1 and RTNM>0 and B2S_GPS>10 and B2S_GPS<=15
	group by [New District Name]
), RTNM_Trips_15 as
(
	select [New District Name] as 'District',
	COUNT(RTNM) as 'RTNM Trips in B2S > 15KM' 
	from #Availed
	where [Trip No]=1 and RTNM>0 and B2S_GPS>15
	group by [New District Name]
)
select 
md.District,
tc.[Trip Count],
COALESCE(pr.[Pick Up RFP],0) + COALESCE(dr.[Drop Back RFP],0) + COALESCE(ir.[IFT RFP],0) as 'Availed RFP Trip Count',
md.[KM of Availed Trips],
pr.[Pick Up RFP],
dr.[Drop Back RFP],
db_1b.[1B RFP],
db_2b.[2B RFP],
db_3b.[3B RFP],
ir.[IFT RFP],
rt_pr.[RTNM Pick Up RFP],
rt_dr.[RTNM Drop Back RFP],
rt_ir.[RTNM IFT RFP],
COALESCE(rt_pr.[RTNM Pick Up RFP],0) + COALESCE(rt_dr.[RTNM Drop Back RFP],0) + COALESCE(rt_ir.[RTNM IFT RFP],0) as 'RTNM RFP Trip Count',
b_10.[B2S <= 10KM],
rt_10.[RTNM Trips in B2S <= 10KM],
b_10_15.[B2S (10-15KM)],
rt_10_15.[RTNM Trips in B2S (10-15KM)],
b_15.[B2S > 15KM)],
rt_15.[RTNM Trips in B2S > 15KM],
md.[RTNM Minutes]
from Main_Data md
left join Trip_Count tc on md.District=tc.District
left join PU_RFP pr on md.District=pr.District
left join DB_RFP dr on md.District=dr.District
left join DB_1B_RFP db_1b on md.District=db_1b.District
left join DB_2B_RFP db_2b on md.District=db_2b.District
left join DB_3B_RFP db_3b on md.District=db_3b.District
left join IFT_RFP ir on md.District=ir.District
left join B2S_10 b_10 on md.District=b_10.District 
left join B2S_10_15 b_10_15 on md.District=b_10_15.District 
left join B2S_15 b_15 on md.District=b_15.District
left join RTNM_PU_RFP rt_pr on md.District=rt_pr.District
left join RTNM_DB_RFP rt_dr on md.District=rt_dr.District
left join RTNM_IFT_RFP rt_ir on md.District=rt_ir.District
left join RTNM_Trips_10 rt_10 on md.District=rt_10.District
left join RTNM_Trips_10_15 rt_10_15 on md.District=rt_10_15.District
left join RTNM_Trips_15 rt_15 on md.District=rt_15.District
order by md.District;
GO

--Unavailed Cases

--Temporary Table Creation
declare @start_date as datetime,@end_date as datetime
set @start_date='2023-10-01 00:00:00'
set @end_date='2023-10-31 23:59:59'

drop table if exists #UnAvailed
select d.[New District Name], m.[New_Reg#No#_RO], m.[Trip No], m.[Call ID], m.[Call Types], 
m.[Standard Remarks], m.[TripId], m.[Hsp-District], m.[Benef District], m.[Destination-District]
into #UnAvailed
from [DB102].[dbo].[102Raw_Closed] m
left join [analytics].[dbo].[District] d
on m.[Amby-District]=d.[Old District Name] collate SQL_Latin1_General_CP1_CI_AS
where (m.[Standard Remarks] not in ('Closed First Aid','Closed(Home to Hospital)','Closed(IFT)','Victim dropped to home') or 
m.[Standard Remarks] is null)
GO

--Main Query
with Main_Data as
(
	select [New District Name] as 'District'
	from #Unavailed
	where [Trip No]=1
	group by [New District Name]
), PU_RFP as
(
	select [New District Name] as 'District',
	COUNT([Call ID]) as 'Pick Up RFP'
	from #Unavailed
	where [Call Types]='PICK UP'
	group by [New District Name]
), DB_RFP as
(
	select alias.District,
	sum(alias.[Drop Back RFP Trip Sum]) as 'Drop Back RFP'
	from
	(
		select [New District Name] as 'District',
		coalesce(
		(case 
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=1 then 0.8
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=2 then 1.5
			when sum(case when [Hsp-District]=[Benef District] then 1 end)=3 then 2
		end),0) 
		+
		coalesce(
		(case
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=1 then 1.6
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=2 then 3
			when sum(case when [Hsp-District]<>[Benef District] then 1 end)=3 then 4
		end),0) as 'Drop Back RFP Trip Sum'
		from #Unavailed
		where [Call Types]='DROP BACK'
		group by [New District Name],[TripId]
	) alias
	group by alias.[District]
), IFT_RFP as
(
	select alias.[District],
	SUM(alias.[IFT RFP Trip Sum]) as 'IFT RFP'
	from
	(
		select [New District Name] as 'District',
		case 
			when [Hsp-District]=[Destination-District] then 1
			when [Hsp-District]<>[Destination-District] then 2
		end as 'IFT RFP Trip Sum'
		from #Unavailed
		where [Call Types]='IFT'
	) alias
	group by alias.[District]
)
select md.[District],
pr.[Pick Up RFP],
dr.[Drop Back RFP],
ir.[IFT RFP],
COALESCE(pr.[Pick Up RFP],0)+COALESCE(dr.[Drop Back RFP],0)+COALESCE(ir.[IFT RFP],0) as 'Unavailed RFP Trip Count'
from Main_Data md
left join PU_RFP pr on md.[District]=pr.[District]
left join DB_RFP dr on md.[District]=dr.[District]
left join IFT_RFP ir on md.[District]=ir.[District]
order by md.[District];
GO