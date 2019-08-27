class SqlQueries:
    f_flights = ("""
        INSERT INTO {} (
            flights_id,
            flight_detail_id,
            airport_depart_id,
            airport_depart_weather_id,
            depart_time_id,
            airport_arrival_id,
            airport_arrival_weather_id,
            arrival_time_id,
            schdld_flight_time_sec,
            schdld_flight_time_min,
            flight_time_sec,
            flight_time_min,
            depart_delay_sec,
            depart_delay_min,
            arrival_delay_sec,
            arrival_delay_min
        ) 
        SELECT
        FROM 
        WHERE 
    """)

    d_flight_detail = ("""
        INSERT INTO {} (
        flight_detail_id,
        carrier,
        origin,
        dest,
        distance,
        schdld_depart_time,
        schdld_arrival_time,
        cancelled,
        diverted
        ) 
        SELECT 
        FROM stage_flight_details
        WHERE 
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
        SELECT 
        FROM 
    """)

    d_weather = ("""
        INSERT INTO {} (
        weather_id,
        date,
        max_temp,
        min_temp,
        avg_temp,
        precipitation_in,
        snow_fall_in,
        snow_depth_in
        ) 
        SELECT 
        FROM stage_weather
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
        FROM stage_airports
    """)
