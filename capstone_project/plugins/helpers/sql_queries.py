import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('configs.cfg')

# STAGING TABLES
staging_flights_copy = None
staging_weather_copy = None
staging_airports_copy = None


class SqlQueries:
    f_flights = ("""
        INSERT INTO f_flights (
            flight_detail_id,
            schdld_depart_time_id,
            airport_depart_id,
            airport_arrival_id,
            weather_airport_depart_id,
            weather_airport_arrival_id,
            schdld_flight_time_min,
            flight_time_min,
            depart_delay_min,
            arrival_delay_min
        ) 
        SELECT
            fd.flight_detail_id,
            dt.datetime,
            da_depart.airport_id AS airport_depart_id,
            da_arrive.airport_id AS airport_arrival_id,
            dw_depart.weather_id AS weather_airport_depart_id,
            dw_arrive.weather_id AS weather_airport_arrival_id,
            DATEDIFF(MINS, fd.schdld_arvl_time, fd.schdld_dprt_time) as schdld_flight_time_min,
            CASE WHEN fd.arvl_time = NULL or fd.dprt_time = NULL THEN NULL
            	ELSE DATEDIFF(MINS, fd.arvl_time, fd.dprt_time) END as flight_time_min,
            CASE WHEN fd.arvl_time = NULL or fd.dprt_time = NULL THEN NULL
            	ELSE DATEDIFF(MINS, fd.dprt_time, fd.schdld_dprt_time) END as depart_delay_min,
            CASE WHEN fd.arvl_time = NULL or fd.dprt_time = NULL THEN NULL
            	ELSE DATEDIFF(MINS, fd.arvl_time, fd.schdld_arvl_time) END as arrival_delay_min
        FROM d_flight_detail fd
        JOIN d_time dt ON dt.datetime = fd.schdld_dprt_time
        JOIN d_airport da_depart ON da_depart.airport_code = fd.origin
        JOIN d_airport da_arrive ON da_arrive.airport_code = fd.dest
        JOIN d_weather dw_depart ON LOWER(REPLACE(dw_depart.airport_name, 'INTERNATIONAL AIRPORT', 'Intl')) = LOWER(da_depart.name)
        JOIN d_weather dw_arrive ON LOWER(REPLACE(dw_arrive.airport_name, 'INTERNATIONAL AIRPORT', 'Intl')) = LOWER(da_arrive.name);
    """)

    d_flight_detail = ("""
        INSERT INTO d_flight_detail (
            carrier,
            origin,
            dest,
            distance,
            schdld_dprt_time,
            schdld_arvl_time,
  			dprt_time,
            arvl_time,
            cancelled,
            diverted
        )
        SELECT
            UniqueCarrier AS carrier,
            Origin AS origin,
            Dest AS dest,
            CASE WHEN Distance = 'NA' THEN 0
            	ELSE Distance::int2
            END AS distance,
            CAST(Year || '-' ||
                 CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
                 CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END  || ' ' ||
            	CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
	 				WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
			        WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     				WHEN LEN(CRSDepTime) = 4 THEN 
     				 CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     				 || ':' || SUBSTRING(CRSDepTime, 3, 2)
				END AS timestamp),
            CAST(Year || '-' ||
                 CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
                 CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END|| ' ' ||
	            CASE WHEN LEN(CRSArrTime) = 1 THEN ('00:0' || CRSArrTime)
					 WHEN LEN(CRSArrTime) = 2 THEN '00:' || CRSArrTime
     				 WHEN LEN(CRSArrTime) = 3 THEN '0' || SUBSTRING(CRSArrTime, 0, 2) || ':' || SUBSTRING(CRSArrTime, 2, 2)
     				 WHEN LEN(CRSArrTime) = 4 THEN 
                         CASE WHEN SUBSTRING(CRSArrTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSArrTime, 1, 2) END
                          || ':' || SUBSTRING(CRSArrTime, 3, 2)
				END AS timestamp),
            CASE WHEN Arrtime = 'NA' then NULL
			ELSE    
                CAST(Year || '-' ||
                 CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
                 CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END  || ' ' ||
            	CASE WHEN LEN(DepTime) = 1 THEN ('00:0' || DepTime)
	 				WHEN LEN(DepTime) = 2 THEN '00:' || DepTime
			        WHEN LEN(DepTime) = 3 THEN '0' || SUBSTRING(DepTime, 0, 2) || ':' || SUBSTRING(DepTime, 2, 2)
     				WHEN LEN(DepTime) = 4 THEN 
     				 CASE WHEN SUBSTRING(DepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(DepTime, 1, 2) END
     				 || ':' || SUBSTRING(DepTime, 3, 2)
				END AS timestamp)
            END,
         CASE WHEN Arrtime = 'NA' then NULL
		 ELSE CAST(Year || '-' ||
                 CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
                 CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END|| ' ' ||
	            CASE WHEN LEN(ArrTime) = 1 THEN ('00:0' || ArrTime)
					 WHEN LEN(ArrTime) = 2 THEN '00:' || ArrTime
     				 WHEN LEN(ArrTime) = 3 THEN '0' || SUBSTRING(ArrTime, 0, 2) || ':' || SUBSTRING(ArrTime, 2, 2)
     				 WHEN LEN(ArrTime) = 4 THEN 
                         CASE WHEN SUBSTRING(ArrTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(ArrTime, 1, 2) END
                          || ':' || SUBSTRING(ArrTime, 3, 2)
				END AS timestamp) 
         END,
            Cancelled::int2 AS cancelled,
            Diverted::int2 AS diverted
        FROM stage_flight_details
        ORDER BY
        CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END|| ' ' ||
	        CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
			     WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
     			 WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     			 WHEN LEN(CRSDepTime) = 4 THEN 
     			  CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     			  || ':' || SUBSTRING(CRSDepTime, 3, 2)
		    END AS timestamp);
    """)

    d_time = ("""
        INSERT INTO {} (
            "date",
            "datetime",
            "year",
            quarter,
            month,
            day,
            hour,
            minute
        )
        SELECT DISTINCT
            CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END AS DATE),

            CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END || ' ' ||
	        CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
			     WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
     			 WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     			 WHEN LEN(CRSDepTime) = 4 THEN
     			  CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     			  || ':' || SUBSTRING(CRSDepTime, 3, 2)
		    END AS timestamp),

            "Year"::int2 AS "year",
            CASE WHEN Month in (1, 2, 3) THEN 'Q1'
                 WHEN Month in (4, 5, 6) THEN 'Q2'
                 WHEN Month in (7, 8, 9) THEN 'Q3'
                 WHEN Month in (10, 11, 12) THEN 'Q4'
                 ELSE NULL
            END AS quarter,
            Month::int2 AS month,
            DayofMonth::int2 AS day,
            extract(hour from CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END || ' ' ||
	        CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
			     WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
     			 WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     			 WHEN LEN(CRSDepTime) = 4 THEN
     			  CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     			  || ':' || SUBSTRING(CRSDepTime, 3, 2)
		    END AS timestamp)) AS hour,
            extract(minute from CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END|| ' ' ||
	        CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
			     WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
     			 WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     			 WHEN LEN(CRSDepTime) = 4 THEN
     			  CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     			  || ':' || SUBSTRING(CRSDepTime, 3, 2)
		    END AS timestamp)) AS minute
        FROM stage_flight_details
        ORDER BY CAST(Year || '-' ||
            CASE WHEN LEN(Month) = 1 THEN '0' || MONTH ELSE MONTH END || '-' ||
            CASE WHEN LEN(DayofMonth) = 1 THEN '0' || DayofMonth ELSE DayofMonth END|| ' ' ||
	        CASE WHEN LEN(CRSDepTime) = 1 THEN ('00:0' || CRSDepTime)
			     WHEN LEN(CRSDepTime) = 2 THEN '00:' || CRSDepTime
     			 WHEN LEN(CRSDepTime) = 3 THEN '0' || SUBSTRING(CRSDepTime, 0, 2) || ':' || SUBSTRING(CRSDepTime, 2, 2)
     			 WHEN LEN(CRSDepTime) = 4 THEN
     			  CASE WHEN SUBSTRING(CRSDepTime, 1, 2) = '24' THEN '00' ELSE SUBSTRING(CRSDepTime, 1, 2) END
     			  || ':' || SUBSTRING(CRSDepTime, 3, 2)
		    END AS timestamp);
    """)

    d_weather = ("""
        INSERT INTO d_weather (
            airport_name,
            "date",
            max_temp,
            min_temp,
            avg_temp,
            precipitation_in,
            snow_fall_in,
            snow_depth_in
        )
        SELECT 
            INITCAP(LOWER(REPLACE(name, 'INTERNATIONAL AIRPORT', 'Intl'))) as name,
            "date",
            CASE WHEN max_temp in  ('M', 'T') THEN 0 
                 ELSE max_temp::int2 END as max_temp,
            CASE WHEN min_temp in  ('M', 'T') THEN 0 
                 ELSE min_temp::int2 END as min_temp,
            CASE WHEN avg_temp in  ('M', 'T') THEN 0 
                 ELSE avg_temp::numeric(4,1) END as avg_temp,
            CASE WHEN precipitation_in in  ('M', 'T') THEN 0 
                 ELSE precipitation_in::numeric(5,2)
            END as precipitation_in,
            CASE WHEN snow_fall_in in  ('M', 'T') THEN 0
                 ELSE snow_fall_in::numeric(5,2)
            END as snow_fall_in,
            CASE WHEN snow_depth_in in  ('M', 'T') THEN 0
                 ELSE snow_depth_in::numeric(5,2)
            END as snow_depth_in
        FROM stage_weather
        ORDER BY date;        
    """)

    d_airport = ("""
        INSERT INTO {} (
            name,
            airport_code,
            city,
            state,
            country,
            latitude,
            longitude
        ) 
        SELECT
            TRIM(airport) as name,
            TRIM(iata) as airport_code,
            TRIM(city),
            TRIM(state),
            TRIM(country),
            TRIM(lat) as latitude,
            TRIM(long) as longitude 
        FROM stage_airports
        WHERE city IS NOT NULL AND country = 'USA'
        ORDER BY TRIM(iata);
    """)

    create_staging_flights = ("""CREATE TABLE IF NOT EXISTS 
        public.stage_flight_details (
            Year varchar(10),
            Month varchar(10),
            DayofMonth varchar(10),
            DayOfWeek varchar(10),
            DepTime varchar(10),
            CRSDepTime varchar(10),
            ArrTime varchar(10),
            CRSArrTime varchar(10),
            UniqueCarrier varchar(10),
            FlightNum varchar(10),
            TailNum varchar(10),
            ActualElapsedTime varchar(10),
            CRSElapsedTime varchar(10),
            AirTime varchar(10),
            ArrDelay varchar(10),
            DepDelay varchar(10),
            Origin varchar(10),
            Dest varchar(10),
            Distance varchar(10),
            TaxiIn varchar(10),
            TaxiOut varchar(10),
            Cancelled varchar(10),
            CancellationCode varchar(10),
            Diverted varchar(10),
            CarrierDelay varchar(10),
            WeatherDelay varchar(10),
            NASDelay varchar(10),
            SecurityDelay varchar(10),
            LateAircraftDelay varchar(10)
            );""")

    create_staging_airports = ("""CREATE TABLE IF NOT EXISTS
         public.stage_airports (
            iata varchar(10),
            airport varchar(100) NOT NULL,
            city varchar(50),
            state varchar(10),
            country varchar(50),
            lat numeric(13,8),
            long numeric(13,8)
            );""")

    create_staging_weather = ("""CREATE TABLE IF NOT EXISTS
         public.stage_weather (
            name varchar(100),
            "date" date,
            max_temp varchar(10),
            min_temp varchar(10),
            avg_temp varchar(10),
            precipitation_in varchar(10),
            snow_fall_in varchar(10),
            snow_depth_in varchar(10)
            );""")

    create_f_flights = ("""CREATE TABLE IF NOT EXISTS
         public.f_flights (
            flights_id int identity(0,1) NOT NULL PRIMARY KEY,
            flight_detail_id int4,
            schdld_depart_time_id timestamp,
            airport_depart_id int4,
            airport_arrival_id int4,
            weather_airport_depart_id int4,
            weather_airport_arrival_id int4,
            schdld_flight_time_min int2,
            flight_time_min int2,
            depart_delay_min int2,
            arrival_delay_min int2
            );""")

    create_d_flight_detail = ("""CREATE TABLE IF NOT EXISTS
         public.d_flight_detail (
            flight_detail_id int identity(0,1) NOT NULL PRIMARY KEY,
            carrier varchar(6),
            origin varchar(3),
            dest varchar(3),
            distance int2,
            schdld_dprt_time timestamp without time zone SORTKEY DISTKEY,
            schdld_arvl_time timestamp without time zone,
            dprt_time timestamp without time zone,
            arvl_time timestamp without time zone,
            cancelled int2,
            diverted int2
            );""")

    create_d_time = ("""CREATE TABLE IF NOT EXISTS public.d_time (
            time_id int identity(0,1) NOT NULL PRIMARY KEY,
            "date" date,
            datetime timestamp without time zone SORTKEY DISTKEY,
            year int2,
            quarter varchar(2),
            month int2,
            day int2,
            hour int2,
            minute int2
            );""")

    create_d_weather = ("""CREATE TABLE IF NOT EXISTS public.d_weather (
            weather_id int identity(0,1) NOT NULL PRIMARY KEY,
            airport_name varchar(100),
            "date" date SORTKEY DISTKEY,
            max_temp int2,
            min_temp int2,
            avg_temp numeric(4,1),
            precipitation_in numeric(5,2),
            snow_fall_in numeric(5,2),
            snow_depth_in numeric(5,2)
            );""")

    create_d_airport = ("""CREATE TABLE IF NOT EXISTS public.d_airport (
            airport_id int identity(0,1) NOT NULL PRIMARY KEY,
            name varchar(100) NOT NULL,
            airport_code varchar(4),
            city varchar(50),
            state varchar(2),
            country varchar(3),
            latitude numeric(13,8),
            longitude numeric(13,8)
            );""")

    staging_flights_copy = ("""
            copy stage_flight_details from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            timeformat 'auto'
            dateformat 'auto'
            compupdate off
            statupdate off
            delimiter ',' ignoreheader as 1 bzip2;""")

    staging_weather_copy = ("""
            copy stage_weather from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            compupdate off
            statupdate off
            timeformat 'auto'
            dateformat 'auto'
            format as JSON 'auto';""")

    staging_airports_copy = ("""
            copy stage_airports from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            compupdate off
            statupdate off
            timeformat 'auto'
            dateformat 'auto'
            delimiter ',' removequotes escape ignoreheader as 1;""")
