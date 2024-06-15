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
		jc.raceid
	from join_constructors as jc
		left join public.drivers as dr
		on jc.driverid = dr.driverid
		left join public.races as ra
		on ra.raceid = jc.raceid 
)

select * from join_drivers_race