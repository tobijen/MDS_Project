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
		jc.driverid
	from join_constructors as jc
		left join public.drivers as dr
		on jc.driverid = dr.driverid
		left join public.races as ra
		on ra.raceid = jc.raceid 
),

join_pit_stops as (
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
		ps.stop as num_pitstop,
		ps.lap as pitstop_at_lap,
		ps.milliseconds as duration_pitstop
	from join_drivers_race as jdr
		left join public.pit_stops as ps
		on
			ps.raceid = jdr.raceid
		and 
			ps.driverid = jdr.driverid
),

filter_for_winner as (
	select
		constructor_name,
		grid,
		points,
		"time",
		"position",
		laps,
		forename,
		surname,
		race_name,
		race_date,
		num_pitstop,
		pitstop_at_lap,
		duration_pitstop
	from join_pit_stops
	where "position" = '1'
),

get_avg_pitstop_time as (
	select
		race_name,
		race_date,
		avg(duration_pitstop::float) 
	from filter_for_winner
	group by (
		race_name,
		race_date
	)
	order by race_date desc
)

select * from get_avg_pitstop_time