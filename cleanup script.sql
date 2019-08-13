
select
	--avg(value)
	datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch'),
	startdate,
	*
from HeartRate
limit 100
	
-- Create a datatime value containing only the hour for Heart Rate

ALTER TABLE HeartRate ADD startTimeHour TEXT

UPDATE HeartRate
SET startTimeHour = datetime(strftime('%s',datetime(substr(startDate,1,20))) - strftime('%s',datetime(substr(startDate,1,20))) % 3600, 'unixepoch')

DROP TABLE HourlyHeartRate
CREATE TABLE HourlyHeartRate AS
SELECT 
	startTimeHour,
	avg(value) AverageRate,
	min(value) MinRate,
	max(value) MaxRate
FROM 
	HeartRate
GROUP BY
	startTimeHour