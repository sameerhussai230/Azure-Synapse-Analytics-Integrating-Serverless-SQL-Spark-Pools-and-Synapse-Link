USE nyc_taxi_ldw;

Select DISTINCT
    year,
    month
    From bronze.vw_trip_data_green_csv;
