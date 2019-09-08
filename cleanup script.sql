
select
	--avg(value)
	datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch'),
	startdate,
	*
from HeartRate
limit 100
	
-- Create a datatime value containing only the hour for Heart Rate


/**********************************************************
-- Create a date dimension table at 5 minute intervals
**********************************************************/

-- Determine the start and end of the data set using heart rate data
drop table if exists drange
create table drange as 
select 
		max(date(substr(startDate,1,20))) maxDate,
	   	min(date(substr(startDate,1,20))) minDate,
		(julianday(max(date(substr(startDate,1,20))))-
	   	julianday(min(date(substr(startDate,1,20))))) * 288 increments
FROM HeartRate
where startDate is not null

-- Create the date dim
DROP TABLE IF EXISTS DateDimension;
CREATE TABLE DateDimension AS
WITH RECURSIVE
rDateDimensionMinute (CalendarDateInterval)
AS
(SELECT datetime((select min(minDate) from drange))
UNION ALL
SELECT datetime(CalendarDateInterval, '+5 minute') FROM rDateDimensionMinute
LIMIT cast((select max(increments) from drange) as integer)
)
SELECT CalendarDateInterval, 
	   datetime(CalendarDateInterval, '+299 second') CalendarDateIntervalEnd,
	   	datetime(strftime('%s', CalendarDateInterval) - strftime('%s', CalendarDateInterval) % 3600, 'unixepoch') CalendarDateHour,
		datetime(strftime('%s', CalendarDateInterval) - strftime('%s', CalendarDateInterval) % (3600*24), 'unixepoch')	CalendarDate,
		strftime('%w',CalendarDateInterval)	DayNumber,
		case cast (strftime('%w', CalendarDateInterval) as integer)
		when 0 then 'Sunday'
		when 1 then 'Monday'
		when 2 then 'Tuesday'
		when 3 then 'Wednesday'
		when 4 then 'Thursday'
		when 5 then 'Friday'
		when 6 then 'Saturday' end DayOfWeek,
		case cast (strftime('%w', CalendarDateInterval) as integer)
		when 0 then '00-Sunday'
		when 1 then '01-Monday'
		when 2 then '02-Tuesday'
		when 3 then '03-Wednesday'
		when 4 then '04-Thursday'
		when 5 then '05-Friday'
		when 6 then '06-Saturday' end DayOfWeekNum,
		substr('SunMonTueWedThuFriSat', 1 + 3*strftime('%w', CalendarDateInterval), 3) DayOfWeekAbbr,
		strftime('%H',CalendarDateInterval)	HourNumber,
		strftime('%M',CalendarDateInterval)	MinuteNumber,
		strftime('%d',CalendarDateInterval)	DayOfMonth,
		case cast (strftime('%w', CalendarDateInterval) as integer)
		when 0 then 1
		when 6 then 1
		else 0 end IsWeekend,
		case cast (strftime('%w', CalendarDateInterval) as integer)
		when 0 then 0
		when 6 then 0
		else 1 end IsWeekday,
		strftime('%m',CalendarDateInterval)	MonthNumber,
		case strftime('%m', date(CalendarDateInterval)) 
			when '01' then 'January' 
			when '02' then 'Febuary' 
			when '03' then 'March' 
			when '04' then 'April' 
			when '05' then 'May' 
			when '06' then 'June' 
			when '07' then 'July' 
			when '08' then 'August' 
			when '09' then 'September' 
			when '10' then 'October' 
			when '11' then 'November' 
			when '12' then 'December' else '' end MonthName,
		case strftime('%m', date(CalendarDateInterval)) 
			when '01' then 'Jan' 
			when '02' then 'Feb' 
			when '03' then 'Mar' 
			when '04' then 'Apr' 
			when '05' then 'May' 
			when '06' then 'Jun' 
			when '07' then 'Jul' 
			when '08' then 'Aug' 
			when '09' then 'Sep' 
			when '10' then 'Oct' 
			when '11' then 'Nov' 
			when '12' then 'Dec' else '' end MonthAbbr,
		strftime('%Y',CalendarDateInterval)	YearNumber,
		strftime('%Y',CalendarDateInterval) || '-' || strftime('%m',CalendarDateInterval) YearMonth,
		strftime('%Y',CalendarDateInterval) || '-' || strftime('%W',CalendarDateInterval) YearWeek
FROM rDateDimensionMinute;

CREATE INDEX IF NOT EXISTS ix_DateDimension_date on DateDimension (CalendarDateInterval, CalendarDateIntervalEnd)

select * from DateDimension
limit 1000

/* Create a reconstituted view of HeartRate */
DROP VIEW IF EXISTS vHourlyHeartRate
CREATE VIEW vHourlyHeartRate AS
select
	dd.CalendarDateHour AS CalendarDateInterval,
	avg(value) AverageRate,
	min(value) MinRate,
	max(value) MaxRate,
	avg(case when motionContext = 2 then value end) as AverageActiveRate, 
	avg(case when motionContext = 1 then value end) as AverageSedentaryRate
from
	(
		SELECT	
			vd.CalendarDateInterval,
			hr.motionContext,
			avg(hr.value) value
		FROM
			HeartRate hr inner JOIN
			DateDimension vd on 
				datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 300, 'unixepoch') = vd.CalendarDateInterval
		group by
			vd.CalendarDateInterval,
			hr.motionContext
	) t left outer join
	DateDimension dd on t.CalendarDateInterval = dd.CalendarDateInterval
group by 
	dd.CalendarDateHour

/* Create a reconstituted view of Active/Basal Energy */
DROP VIEW IF EXISTS vHourlyEnergy
CREATE VIEW vHourlyEnergy AS
select
	dd.CalendarDateHour AS CalendarDateInterval,
	sum(value) totalEnergyBurned,
	sum(case when burnType = 'Active' then value end) as totalActiveBurned, 
	sum(case when burnType = 'Basal' then value end) as totalBasalBurned
from
	(
		SELECT	
			vd.CalendarDateInterval,
			'Active' as burnType,
			sum(hr.value) value
		FROM
			activeEnergyBurned hr inner JOIN
			DateDimension vd on 
				datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 300, 'unixepoch') = vd.CalendarDateInterval
		group by
			vd.CalendarDateInterval
		union
		SELECT	
			vd.CalendarDateInterval,
			'Basal' as burnType,
			sum(hr.value) value
		FROM
			basalEnergyBurned hr inner JOIN
			DateDimension vd on 
				datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 300, 'unixepoch') = vd.CalendarDateInterval
		group by
			vd.CalendarDateInterval
	) t left outer join
	DateDimension dd on t.CalendarDateInterval = dd.CalendarDateInterval
group by 
	dd.CalendarDateHour
having sum(value) < 1000 --exclude instrument errors

/* Create a reconstituted view of Workout */
DROP VIEW IF EXISTS vHourlyWorkout
CREATE VIEW vHourlyWorkout AS
select
	dd.CalendarDateHour AS CalendarDateInterval,
	t.workoutType,
	sum(t.totalEnergyBurned) totalEnergyBurned,
	sum(t.duration) duration,
	sum(t.distance) distance
from
	(
		SELECT	
			vd.CalendarDateInterval,
			replace(zt.name,'HKWorkoutActivityType','') as workoutType,
			sum(hr.totalEnergyBurned) totalEnergyBurned,
			sum(hr.duration) duration,
			sum(hr.totalDistance) distance
		FROM
			Workout hr left outer join
			ztype zt on hr.workoutActivityType = zt.value inner JOIN
			DateDimension vd on 
				datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 300, 'unixepoch') = vd.CalendarDateInterval
		group by
			vd.CalendarDateInterval,
			replace(zt.name,'HKWorkoutActivityType','')
	) t left outer join
	DateDimension dd on t.CalendarDateInterval = dd.CalendarDateInterval
group by 
	dd.CalendarDateHour,
	t.workoutType

/* Create a view of daily activity summaries */
DROP VIEW IF EXISTS vActivitySummary
CREATE VIEW vActivitySummary AS
select
	CalendarDateInterval,
	t.*,
	case when activeEnergyBurned >= activeEnergyBurnedGoal then 1 else 0 end activeGoalMet,
	case when appleExerciseTime >= appleExerciseTimeGoal then 1 else 0 end exerciseGoalMet,
	case when appleStandHours >= appleStandHoursGoal then 1 else 0 end standGoalMet,
	case when activeEnergyBurned >= activeEnergyBurnedGoal and 
			  appleExerciseTime >= appleExerciseTimeGoal and 
			  appleStandHours >= appleStandHoursGoal then 1 else 0 end allGoalMet,
	1 as denominator
from
	ActivitySummary t inner join
	DateDimension dd on datetime(t.dateComponents) = dd.CalendarDateInterval

/* Gaps and islands to find longest streak */

DROP table IF EXISTS vActivityStreaks
CREATE table tActivityStreaks AS
select max(streaks) streaks, streakStart, streakEnd
from
(
select
	julianday(max(CalendarDateInterval))-
	julianday(min(CalendarDateInterval))+1 streaks,
	min(CalendarDateInterval) streakStart,
	max(CalendarDateInterval) streakEnd
from
	(
		select
			*
			, row_number() OVER w1 - row_number() OVER w2 AS diff
		from
			vHourlyWorkout
		WINDOW w1 AS (ORDER BY CalendarDateInterval)
			, w2 AS (PARTITION BY allGoalMet = 1 ORDER BY CalendarDateInterval)
	) t
where
	allGoalMet = 1
group by
	diff
)t2
group by streakStart, streakEnd
order by max(streaks) desc
limit 5

-- IDEAS
-- Average calories per minute per type of workout
-- longest unbroken activity streak
			
SELECT	
			*
		FROM
			basalEnergyBurned hr 
where startDate >= '2018-11-30' and startDate <= '2018-12-03'