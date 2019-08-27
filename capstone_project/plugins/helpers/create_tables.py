class CreateTables:
    # todo determine not nulls, pks, distributions, partitions
    create_f_flights = ("""CREATE TABLE weatherfly.f_flights (
        flights_id int4 NOT NULL,
        flight_detail_id int4,
        airport_depart_id int4,
        airport_depart_weather_id int4,
        depart_time_id int4,
        airport_arrival_id int4,
        airport_arrival_weather_id int4,
        arrival_time_id int4,
        schdld_flight_time_sec int4,
        schdld_flight_time_min int2,
        flight_time_sec int4,
        flight_time_min int2,
        depart_delay_sec int4,
        depart_delay_min int2,
        arrival_delay_sec int4,
        arrival_delay_min int2,
        CONSTRAINT f_flights_pk PRIMARY KEY (flights_id)
        );""")

    create_d_flight_detail = ("""CREATE TABLE weatherfly.d_flight_detail (
        flight_detail_id int4 NOT NULL,
        carrier varchar(5),
        origin varchar(3),
        dest varchar(3),
        distance int2,
        schdld_depart_time timestamp without time zone,
        schdld_arrival_time timestamp without time zone,
        cancelled int2,
        diverted int2,
        CONSTRAINT d_flight_detail_pk PRIMARY KEY (d_flight_detail)
        );""")

    create_d_time = ("""CREATE TABLE weatherfly.d_time (
        time_id int NOT NULL,
        'date' date,
        datetime timestamp without time zone,
        timezone varchar(5),
        year int2,
        quarter int2,
        month int2,
        day int2,
        day_of_week int2,
        hour int2,
        minute int2,
        second int2,
        CONSTRAINT d_time_detail_pk PRIMARY KEY (d_time)
        );""")

    create_d_weather = ("""CREATE TABLE weatherfly.d_weather (
        weather_id int NOT NULL,
        'date' date,
        max_temp int2,
        min_temp int2,
        avg_temp numeric(4,1),
        precipitation_in numeric(5,2),
        snow_fall_in numeric(5,2),
        snow_depth_in numeric(5,2),
        CONSTRAINT d_weather_detail_pk PRIMARY KEY (d_weather)
        );""")

    create_d_airport = ("""CREATE TABLE weatherfly.d_airport (
        name varchar(100) NOT NULL,
        airport_code varchar(3),
        city varchar(50),
        state varchar(2),
        country varchar(3),
        latitude numeric(13,8),
        longitude numeric(13,8),
        CONSTRAINT d_airport_detail_pk PRIMARY KEY (d_airport)
        );""")
