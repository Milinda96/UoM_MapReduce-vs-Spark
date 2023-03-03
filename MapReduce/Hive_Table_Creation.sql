-- Create table in Hadoop using hiveQL
CREATE TABLE delay_flights (
    Id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay DOUBLE,
    DepDelay DOUBLE,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted DOUBLE,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://airline-delay/hiveSession/';

-- Describe the table
DESCRIBE delay_flights;

-- Load the data from s3 bukect
LOAD DATA INPATH 's3://airline-delay/DelayedFlights-updated.csv' OVERWRITE INTO TABLE delay_flights;

-- Remove the table
DROP TABLE IF EXISTS delay_flights;
