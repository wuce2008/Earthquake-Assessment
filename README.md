# Earthquake Assessment

## Table of contents:

- [Background](#Background)
	* [Data source](#Data Source)
	* [Technology](#Technology)

Tasks

	Database Design
	
	ETL Architecture 
	
		Extract
		
		Transform
		
		Load
		
	Analysis
	
Data modeling evaluation

Furthermore

## Background

When the local surface plates move and collide with each other, they will cause earthquakes. Earthquakes are often difficult to predict and it is impossible to prevent them from occurring. However, the U.S. Geological Survey (USGS) can map the future high-risk areas of earthquakes in the United States based on data derived from the locations and times of earthquakes in the past.

This Assessment exhibits earthquake relational database designing, ETL metadata into MySQL, and data analyzing. 

## Data Source
https://earthquake.usgs.gov/fdsnws/event/1/

## Technology
Python 3.9, MySQL, Tableau

## Tasks
Database Design

Exploring GeoJSON Summary from https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php

[Type], [metadata] and [bbox] show the general data collection information when we send data request to the website. In this project, I do not load this part of data.

[Features]:
Properties
Contains earthquake main information, like [id], [mag], [place], [time], etc. Main load.

Column [sources], [types], [ids] contains multi value in one cell. For database design performance concerning, it should follow 3NF.

[Geometry]
Contains earthquake geo information, [latitude], [longitude], [depth]. Main load.

ER Diagram
![alt text](http://url/to/img.png)
 
ETL Architecture 

1. Extract

The request query to usgs.gov url cannot download the whole year 2017 earthquake events at one time. So, I used incremental load month by month. 

Query:

usgs_request_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
usgs_request_url += "&starttime=" + startdate + "&endtime=" + enddate
print(usgs_request_url)

Results:

1.1. Generate url which used to send request to usgs api to download metadata.

 
1.2. Extract metadata using json format.

Query:

geojsonrecord = requests.get(usgs_request_url).json()


1.3. Parse json file and insert it into pandas dataframe.

Query:

json_norm = jn(geojsonrecord)

2. Transform

2.1. Change time data type from bigint to readable datetime.
2.2. Remove comma in categorical columns.
2.3. Split multi-value column into separate columns in different tables.
2.4. Generate business key [pid] auto increment every month. (for next month, start from the biggest [pid] to auto increment ).
2.5. Change column names.

(Query in python file function incrementalLoadDF)

3. Load

	3.1. Generate table structure in MySQL database.
[properties], [geometry], [types], [types_properties], [sources], [sources_properties]
	3.2. Add FK into tables.
3.3. Remove unused columns

(Query in python file line 217-340)

Analysis

1. biggest earthquake of 2017

Use [mag] to measure if it is the biggest or not. Because from mag description, it indicates that “The magnitude reported is that which the U.S. Geological Survey considers official for this earthquake, and was the best available estimate of the earthquake’s size, at the time that this page was created.”

(After checking the dataset there is NO TIE of biggest mag in 2017. So will using order by with limit. If there is a tie, will change to rank() window function.)

Query:

select
*
from properties p
inner join geometry g on p.pid = g.pid
order by mag desc
limit 1;


Result:
The biggest mag earthquake of 2017 information is
Place: 101km SSW of Tres Picos, Mexico
Happen time: 2017-09-08 04:49:19.
Magnitude is 8.2
depth: 47.39


2. give most probable hour of the day for the earthquakes bucketed by the range of magnitude (0-1,1-2,2-3,3-4,4-5,5-6,>6   For border values in the bucket, include them in the bucket where the value is a lower limit so for 1 include it in 1-2 bucket)

Query:

with cte
as (
select
*,
case 
when mag >= 0 and mag < 1 then '0-1'
when mag >= 1 and mag < 2 then '1-2'
when mag >= 2 and mag < 3 then '2-3'
when mag >= 3 and mag < 4 then '3-4'
when mag >= 4 and mag < 5 then '4-5'
when mag >= 5 and mag < 6 then '5-6'
when mag >= 6 then '>6'
end as mag_bin
from usgs.properties
)
select
mag_bin,
event_hour,
event_times
from (
	select
	mag_bin,
	hour(time) as event_hour,
    count(*) as event_times,
	rank() over (partition by mag_bin order by count(*) desc) as rk
    -- count(*) as event_times
	from cte 
	where mag_bin is not null
	group by mag_bin, hour(time)
) sub
where rk = 1
order by mag_bin, event_hour

result:

# mag_bin	event_hour	event_times
0-1	10	2209
1-2	19	2370
2-3	14	754
3-4	20	235
4-5	15	568
5-6	14	90
>6	2	10
>6	22	10
		
3. More Analysis

 

I found some interesting results from earthquake analyzing. 

From the ‘All events in 2017’ we can see the most earthquake In U.S occurred in west coast. That is make sense due to North American plate and the Pacific plate interacting with fault systems crisscrossing above and other active systems.

From ‘Event monthly freq by mag type’ report we can see the ml mag type earthquake is the most frequency in every month in 2017. The most events occurred in May 2017 and there are 10k+ ml type earthquake in the world.

 After set mag_bin feature, from report mag with depth we can find the most depth earthquake centralized to mag_bin [3-4], [4-5], [5-6]. But when mag greater than 6, the earthquake depth is lower than preview mag_bin. Maybe due to the earthquake source observation method.

“Event freq per mag_bin per hour” shows how many events occurred group by mag_bin and hour of the day. The result is as same as the SQL query I provide above. 


Data modeling evaluation
Advantage

Database

Due to many-to-many relationship performance issue, I added connection tables between 2 entities ([sources_properties], [types_properties]) to following 3NF to make processing more efficiency. 

ETL

In this assessment, I used ETL model. It was load data first into staging dataframe then into the target MySQL database. ETL model is good used for on-premises, relational and structure data and small number of metadata. Following with incremental load.

Another option would be ELT model. ELT would load data into target MySQL database first then process data transformation. ELT is good used for scalable cloud structured and unstructured data source. Meanwhile, when process data transformation ELT was flexible than ETL. 

In this assessment, the total number of row is 134,620 and contains multi-value in on column. So, I choose ETL strategy.

Disadvantage 

Transformation is complex. Could optimize it in the future.

Furthermore

1. When I analyzed on this earthquake dataset, there is no country – state – city level geo hierarchy. If I can get this geo information, I could build more accuracy analysis report. 

2. This assessment only show 2017 earthquake information. But after the research, from the sources id I found there are some relationship between earthquake and other type of disasters. So if I added more types of disasters data I could generate more relational analysis reports.

Appendix

Amtrak – Customer data mart

 


