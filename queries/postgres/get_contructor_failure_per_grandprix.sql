-- get average pit_stop time per grand prix winner
with join_constructors as (
	SELECT 
		re.resultid,
		re.raceid, 
		re.driverid, 
		re.constructorid, 
		co.name as constructor_name,
		re."number", 
		re.grid, 
		re."position", 
		re.positiontext, 
		re.positionorder, 
		re.points, 
		re.laps, 
		re."time", 
		re.milliseconds, 
		re.fastestlap, 
		re."rank", 
		re.fastestlaptime, 
		re.fastestlapspeed, 
		re.statusid
	FROM public.results as re
		left join public.constructors as co
		on co.constructorid = re.constructorid
),

join_drivers_race as (
	select 
		constructor_name,
		grid,
		points,
		jc."time",
		jc.position,
		laps,
		dr.forename,
		dr.surname,
		ra."name" as race_name,
		ra."date" as race_date,
		jc.raceid,
		jc.statusid,
		jc.driverid
	from join_constructors as jc
		left join public.drivers as dr
		on jc.driverid = dr.driverid
		left join public.races as ra
		on ra.raceid = jc.raceid 
),

join_status as (
	select 
		jdr.constructor_name,
		jdr.grid,
		jdr.points,
		jdr."time",
		jdr.position,
		jdr.laps,
		jdr.forename,
		jdr.surname,
		jdr.race_name,
		jdr.race_date,
		st.status
	from join_drivers_race as jdr
		left join public.status as st
		on st.statusid = jdr.statusid
),

filter_for_damage as (
	select
		constructor_name,
		grid,
		points,
		"time",
		"position",
		laps,
		forename || surname as driver,
		surname,
		race_name,
		race_date,
		status
	from join_status
	where "status" in ('Engine', 'Transmission', 'Hydraulics')
),

group_data_by_race_constructor as (
	select 
		count(constructor_name) as num_constructor_failure,
		constructor_name,
		race_name,
		status,
		driver,
		laps as lap
	from filter_for_damage
	group by (
		constructor_name,
		race_name,
		status,
		laps,
		driver
	)
)

select * from group_data_by_race_constructor