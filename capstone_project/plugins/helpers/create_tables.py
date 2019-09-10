class CreateTables:

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

    # todo validate sortkeys and distkeys
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
