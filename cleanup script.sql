
select
	--avg(value)
	datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch'),
	startdate,
	*
from HeartRate
limit 100
	
-- Create a datatime value containing only the hour for Heart Rate

--ALTER TABLE HeartRate ADD startTimeHour TEXT

--UPDATE HeartRate
--SET startTimeHour = datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch')

DROP VIEW IF EXISTS vHourlyHeartRate
CREATE VIEW vHourlyHeartRate AS
SELECT 
	datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch') startTimeHour,
	avg(value) AverageRate,
	min(value) MinRate,
	max(value) MaxRate,
	avg(case when motionContext = 2 then value end) as AverageActiveRate, 
	avg(case when motionContext = 1 then value end) as AverageSedentaryRate
FROM 
	HeartRate
GROUP BY
	datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch')
	
select * from vHourlyHeartRate
-- Create a date dimension table at 5 minute intervals
DROP TABLE IF EXISTS DateDimensionMinute;
CREATE TABLE DateDimensionMinute AS
WITH RECURSIVE
rDateDimensionMinute (CalendarDateInterval)
AS
(SELECT datetime('2015-04-01 00:00:00')
UNION ALL
SELECT datetime(CalendarDateInterval, '+5 minute') FROM rDateDimensionMinute
LIMIT 1000000 
)
SELECT CalendarDateInterval FROM rDateDimensionMinute;

--Create View vDateDimensionMinute as
select 
	CalendarDateInterval,
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
	strftime('%Y',CalendarDateInterval)	YearNumber
from 
	DateDimensionMinute

