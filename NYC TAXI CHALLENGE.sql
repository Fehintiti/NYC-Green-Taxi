
------APENDING ALL THE DATASET------------


select VendorID ,lpep_pickup_datetime, lpep_dropoff_datetime,store_and_fwd_flag, RatecodeID, PULocationID, 
DOLocationID, 
passenger_count, trip_distance,fare_amount, extra, mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
payment_type,trip_type into NYC_Taxi
FROM MAVEN..[2017]
	UNION ALL 
	SELECT VendorID ,lpep_pickup_datetime, lpep_dropoff_datetime,store_and_fwd_flag, RatecodeID, PULocationID, 
DOLocationID, 
passenger_count, trip_distance,fare_amount, extra, mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
payment_type,trip_type FROM Maven..[2018]
	UNION ALL
		SELECT VendorID ,lpep_pickup_datetime, lpep_dropoff_datetime,store_and_fwd_flag, RatecodeID, PULocationID, 
DOLocationID, 
passenger_count, trip_distance,fare_amount, extra, mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
payment_type,trip_type FROM Maven..[2019]
			UNION ALL
			SELECT VendorID ,lpep_pickup_datetime, lpep_dropoff_datetime,store_and_fwd_flag, RatecodeID, PULocationID, 
DOLocationID, 
passenger_count, trip_distance,fare_amount, extra, mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
payment_type,trip_type FROM Maven..[2020]


-----------Trips not sent by "Stored an forward", trip_type= "streethailed" ,payment type by 'card or cash' and ratecodeID is standard rate-------------

SELECT * INTO NYC_Taxi2
 FROM
 (
 SELECT * FROM NYC_Taxi
 WHERE store_and_fwd_flag ='N'
	and trip_type = 1
		and payment_type in (1,2)
			and RatecodeID =1) as a


-------GET DATE BETWEEN 2017 TO 2020 AND LOCATION FOR BOTH DROP OFF AND PICK UP THAT IS NOT NULL
		
SELECT * INTO NYC_Taxi3
FROM 
(
	SELECT * FROM NYC_Taxi2
	WHERE   lpep_dropoff_datetime   between '2017-1-01' and '2020-12-31'
		and lpep_pickup_datetime  between '2017-1-01' and '2020-12-31'
			and PULocationID IS NOT NULL 
				AND DOLocationID IS NOT NULL
			 ) AS B

	 ----REPLACE WHERE NO RECOREDED PASSENGER HAD 1 PASSENGER---
SELECT * INTO NYC_TAXI4
FROM
(
SELECT *,REPLACE(passenger_count,('0'),('1') ) AS passengers_count from NYC_Taxi3) AS C



 ------IF PICKUP DATE/TIME IS AFTER THE DROP-OFF TIME or VICE-VERSA,LETS SWAP THEM ---

 SELECT * , 
	CASE WHEN lpep_pickup_datetime >lpep_dropoff_datetime THEN lpep_dropoff_datetime ELSE lpep_pickup_datetime
		end as lpep_pickup
				from NYC_TAXI4
	------ADDED THE NEW COLUMN TO THE TABLE----
	ALTER TABLE NYC_TAXI4
	 ADD lpep_pickup datetime;

	 UPDATE NYC_TAXI4
	 SET lpep_pickup = CASE WHEN lpep_pickup_datetime >lpep_dropoff_datetime THEN lpep_dropoff_datetime ELSE lpep_pickup_datetime
		end 

  

  SELECT * , 
	CASE WHEN lpep_dropoff_datetime >lpep_pickup_datetime THEN lpep_dropoff_datetime ELSE lpep_pickup_datetime
		end as lpep_dropoff
				from NYC_TAXI4
	
	ALTER TABLE NYC_TAXI4
	 ADD lpep_dropoffs datetime;

	 UPDATE NYC_TAXI4
	 SET lpep_dropoffs = CASE WHEN lpep_dropoff_datetime >lpep_pickup_datetime THEN lpep_dropoff_datetime ELSE lpep_pickup_datetime
		end 
	
	-----REMOVE DISTANCE GRATER THAN 24 HOURS----
 
 Select *, CAST((lpep_dropoffs  -lpep_pickup) as time(0)) as Trip_timedistance44
 from NYC_TAXI4 

 
	ALTER TABLE NYC_TAXI4
	 ADD Trip_timedistances44 time;

	 UPDATE NYC_TAXI4
	 SET Trip_timedistances44 =  CAST((lpep_dropoffs  -lpep_pickup) as time(0))


   --( JUMPED FROM NYC_TAXI4 - NYC_Taxi8, BECAUSE I MADE ERROS INBETWEEN)
 select * into NYC_Taxi8
 from NYC_TAXI4
 where CAST (Trip_timedistances44 AS nvarchar) BETWEEN '00:00:01' AND '23:59:59' 
 ORDER BY Trip_timedistances44 DESC



-- select * from NYC_Taxi8
  --
  --select fare_amount,mta_tax,improvement_surcharge ,abs(fare_amount) as Fareamount1,ABS(mta_tax) as mtatax1, ABS( improvement_surcharge) as improvementsurcharge1
  ----from NYC_Taxi8
  --where fare_amount<0 and mta_tax< 0 and improvement_surcharge<0

   ---- CHANGING  FARE AMOUNT FROM POSIITVE TO NEGATIVE

    select *,abs(fare_amount) as Fareamount1
  from NYC_Taxi8
  where fare_amount<0 

   
	ALTER TABLE NYC_Taxi8
	 ADD Fareamount1 float 

	 UPDATE NYC_Taxi8
	 SET Fareamount1 = abs(fare_amount) 

	 --CHANGING mta_tax FROM POSITIVE TO NEGATIVE---


    select *,abs(mta_tax) as Mtatax1
  from NYC_Taxi8
  where mta_tax<0 

   
	ALTER TABLE NYC_Taxi8
	 ADD Mtatax1 float 

	 UPDATE NYC_Taxi8
	 SET Mtatax1 = abs(mta_tax) 

	 select * from NYC_Taxi7

	---- CHANGING improvement_surcharge FROM POSITIVE TO NEGATIVE-------

    select *,abs(improvement_surcharge) as improvementsurcharge1
  from NYC_Taxi8
  where improvement_surcharge<0 

   
	ALTER TABLE NYC_Taxi8
	 ADD improvementsurcharge1 float 

	 UPDATE NYC_Taxi8
	 SET improvementsurcharge1  = abs(improvement_surcharge)

	 select * from NYC_Taxi7


	
	------Trip that have fare amount but have a trip distance of 0, calculatte the distance as (fareamount-2.5)/2.5

	 select Fareamount1, trip_distance from  NYC_Taxi8
		where trip_distance =0 

			 UPDATE NYC_Taxi8
SET 
    trip_distance = ( Fareamount1-2.5)/2.5
WHERE Fareamount1>0 AND trip_distance=0


	------Trip that have a trip distance but have a fare amount of 0, calculate fare amount : 2.5+(trip *2.5)
	 UPDATE NYC_Taxi8
SET 
    Fareamount1 = ( trip_distance*2.5)+2.5
WHERE Fareamount1=0 AND trip_distance>0

 select Fareamount1, trip_distance from  NYC_Taxi8
		order by trip_distance asc


	-------------deleted where both trip_distance and Fareamount1 =0
	delete from NYC_Taxi8
		where trip_distance <0
	

 ----TO SEPERATE THE DROPOFF AND PICKUP TIME AND DATE INDIVIDUALLY...
 
 SELECT *, CONVERT(DATE,lpep_pickup) as Pickupdates from NYC_Taxi8

	ALTER TABLE NYC_Taxi8
	 ADD Pickupdatee date

	 UPDATE NYC_Taxi8
	 SET Pickupdatee = CONVERT(DATE,lpep_pickup)
 
 

  SELECT *, CONVERT(DATE,lpep_dropoffs) as Dropoffdate from NYC_Taxi8

	ALTER TABLE NYC_Taxi8
	 ADD Dropoffdate date

	 UPDATE NYC_Taxi8
	 SET Dropoffdate  = CONVERT(DATE,lpep_dropoffs)


	 SELECT *, CONVERT(time(0),lpep_dropoffs) as Dropofftimes from NYC_Taxi8

	 ALTER TABLE NYC_Taxi8
	 ADD Dropofftimes time(0)

	 UPDATE NYC_Taxi8
	 SET Dropofftimes  = CONVERT(time(0),lpep_dropoffs)


	 SELECT *, CONVERT(time(0),lpep_pickup) as Pickuptime from NYC_Taxi8

	 ALTER TABLE NYC_Taxi8
	 ADD Pickuptime time(0)

	 UPDATE NYC_Taxi8
	 SET Pickuptime = CONVERT(time(0),lpep_pickup) 

	 select * from NYC_Taxi8

	------ JOINED THE NYC_Taxi 8 with the calender table ------( did some calcuations before importing with powerBI)
	 select * into Nyc_Taxi9
	from
	(

select * from NYC_Taxi8  ny join calenderr cal
	on ny.Pickupdatee = cal.Date)  e



------CALCULATED AVERAGE FARE AMOUNT ------
select AVG(Fareamount1) from Nyc_Taxi9


----AVERAGE DIFFERNCE OF RIDES PER WEEK----
with cte as 
( 
select count(*) as totaltrips,FiscalWeekOfYear as Weekno from Nyc_Taxi9
group by FiscalWeekOfYear )  ,
 cte2 as
 (
SELECT  Weekno, AVG(totaltrips) as avg1,lag( AVG(totaltrips)) over (order by( Weekno)) as prevweek from cte
group by Weekno)

 select Weekno,avg1,prevweek, (avg1-prevweek) as weekdiffernce from cte2

 --------------RELATIVE WEEK OVER WEEK CHANGE----
 with cte as 
( 
select count(*) as totaltrips,datepart(week,cal.Date) as Weekno,FiscalYear from NYC_Taxi8 ny inner join
	calenderr cal on  ny.Pickupdatee = cal.Date 
		group by cal.FiscalWeekOfYear, FiscalYear),
 cte2 as
 (
 select Weekno, FiscalYear,sum(totaltrips) as trial , lag (sum(totaltrips)) over (order by (Weekno)) as prevweek,
	(ROW_NUMBER() over (order by Weekno)) as rn1 from cte 
	group by Weekno,FiscalYear )

	select  distinct Weekno,FiscalYear,trial, prevweek,round((trial-prevweek)*1.0/trial*100,2) as dest from cte2
	   
	   -----Another way ----(still correct)
 with cte as 
( 
select count(*) as totaltrips,cal.FiscalWeekofYear as Weekno from NYC_Taxi8 ny 
		join calenderr cal 
				on ny.Pickupdatee= cal.Date
group by cal.FiscalWeekofYear),
 cte2 as
 (
 select Weekno,avg(totaltrips) as trial , lag (avg(totaltrips)) over (order by (Weekno)) as prevweek,
	(ROW_NUMBER() over (order by Weekno)) as rn1 from cte 
	group by Weekno )

	select t1.Weekno,t1.trial, t1.prevweek,round((t1.trial-t1.prevweek)*1.0/t1.trial*100,2) as dest from cte2 t1
		left join cte2 t2 on t1.prevweek=t2.prevweek
	
	

	--------MOST BUSY PICK UP AND DROP OFF STATION -----


	SELECT  distinct PULocationID, Borough ,taxz.Zone, COUNT(*) AS Totaltrips from Nyc_Taxi9 ny  JOIN Taxizones taxz
		on ny.PULocationID = taxz.LocationID
	GROUP BY PULocationID,taxz.Zone,Borough
	order by Totaltrips desc

	  -------Most Busy Pickup station by Zone-----------

	SELECT  DOLocationID, Borough ,taxz.Zone, COUNT(*) AS Totaltrips from Nyc_Taxi9 ny  JOIN Taxizones taxz
		on ny.DOLocationID = taxz.LocationID
	GROUP BY DOLocationID,taxz.Zone,Borough
	order by Totaltrips desc




 















	 with cte as 
( 
select count(*) as totaltrips,datepart(week,Date) as Weekno from Nyc_Taxi9
	group by datepart(week,Date)),
 cte2 as
 (
 select Weekno,sum(totaltrips) as trial , lag (sum(totaltrips)) over (order by (Weekno)) as prevweek,
	(ROW_NUMBER() over (order by Weekno)) as rn1 from cte 
	group by Weekno)

	select  distinct Weekno,trial, prevweek,round((trial-prevweek)*1.0/trial*100,2) as dest from cte2
	   