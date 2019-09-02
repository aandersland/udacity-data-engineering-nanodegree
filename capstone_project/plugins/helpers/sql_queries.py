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
        INSERT INTO {} (
            flights_id,
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
            da_depart.airport_depart_id,
            da_arrive.airport_arrival_id,
            dw_depart.weather_airport_depart_id,
            dw_arrive.weather_airport_arrival_id,
            DATEDIFF(MINS, dt.schdld_arvl_time, dt.schdld_dprt_time) as schdld_flight_time_min,
            DATEDIFF(MINS, dt.arvl_time, dt.dprt_time) as flight_time_min,
            DATEDIFF(MINS, dt.prt_time, dt.schdld_dprt_time) as depart_delay_min,
            DATEDIFF(MINS, dt.arvl_time, dt.schdld_arvl_time) as arrival_delay_min
        FROM d_flight_detail fd
        JOIN d_time dt ON dt.datetime = fd.schdld_dprt
        JOIN d_airport da_depart ON da_depart.airport_code = fd.origin
        JOIN d_airport da_arrive ON da_arrive.airport_code = fd.dest
        JOIN d_weather dw_depart ON dw_depart.airport_name = da_depart.name
        JOIN d_weather dw_arrive ON dw_arrive.airport_name = da_arrive.name 
    """)
    # todo need to create source to target mapping document
    # todo need to validate the joins
    # todo valicate subtract two times and get minutes

    d_flight_detail = ("""
        INSERT INTO {} (
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
            Distance AS distance,
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer)) AS schdld_dprt_time,
            
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSArrTime AS integer)) AS schdld_arvl_time,
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(DepTime AS integer)) AS dprt_time,
            
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(ArrTime AS integer)) AS arvl_time,
            Cancelled AS cancelled,
            Diverted AS diverted 
        FROM stage_flight_details
        ORDER BY CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer))
    """)

    d_time = ("""
        INSERT INTO {} (
            time_id,
            date,
            datetime,
            timezone,
            year,
            quarter,
            month,
            day,
            day_of_week,
            hour,
            minute,
            second
        ) 
        SELECT DISTINCT 
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek AS date,
            CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer)) AS datetime,
            Year AS year,
            CASE WHEN Month in (1, 2, 3) THEN 'Q1',
                 WHEN Month in (4, 5, 6) THEN 'Q2',
                 WHEN Month in (7, 8, 9) THEN 'Q3',
                 WHEN Month in (10, 11, 12) THEN 'Q4'
                 ELSE NULL AS quarter,
            Month AS month,
            DayofMonth AS day,
            extract(hour from CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer))) AS hour,
            extract(minute from CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer))) AS minute
        FROM stage_flight_details
        ORDER BY CAST(Year || '-' || DayofMonth || '-' || DayofWeek  || ' ' || 
            CAST(CRSDepTime AS integer))
    """)

    d_weather = ("""
        INSERT INTO {} (
            airport_name,
            date,
            max_temp,
            min_temp,
            avg_temp,
            precipitation_in,
            snow_fall_in,
            snow_depth_in
        ) 
        SELECT 
            airport_name,
            date,
            max_temp,
            min_temp,
            avg_temp,
            CASE WHEN precipitation_in in  ('M', 'T') THEN 0 
                 ELSE precipitation_in,
            CASE WHEN snow_fall_in in  ('M', 'T') THEN 0
                 ELSE snow_fall_in,
            CASE WHEN snow_depth_in in  ('M', 'T') THEN 0
                 ELSE snow_depth_in
        FROM stage_weather
        ORDER BY date
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
        ORDER BY TRIM(iata)
    """)

    @staticmethod
    def set_staging_copy_params(_config):
        """
        Updates the copy statements with the correct ARN
        :param _config: Configuration file
        :return: Two copy statements: flights, weather, airports
        """
        _staging_flights_copy = ("""
            copy stage_flight_details from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            timeformat as 'epochmillisecs'
            compupdate off
            statupdate off
            delimiter ',' bzip2;""").format(_config.get('S3', 'FLIGHT_DATA'),
                                            _config.get('WFLY',
                                                        'WFLY_ROLE_ARN'),
                                            _config.get('AWS', 'REGION'))

        _staging_weather_copy = ("""
            copy stage_weather from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            compupdate off
            statupdate off
            format as JSON 'auto';""").format(_config.get('S3', 'WEATHER_DATA'),
                                              _config.get('WFLY',
                                                          'WFLY_ROLE_ARN'),
                                              _config.get('AWS', 'REGION'))

        _staging_airports_copy = ("""
            copy stage_airports from '{}'
            credentials 'aws_iam_role={}'
            region '{}'
            compupdate off
            statupdate off
            delimiter ',' removequotes escape ignoreheader as 1;""").format(
            _config.get('S3', 'AIRPORT_DATA'),
            _config.get('WFLY', 'WFLY_ROLE_ARN'),
            _config.get('AWS', 'REGION'))

        return _staging_flights_copy, _staging_weather_copy, \
               _staging_airports_copy

