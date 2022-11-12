-- ALTER ROLE trajectory; -- to allow superuser to store data

COPY trajectory(Track_ID, Type, Traveled_Dis, AVg_Speed, Longuited, Latitude, Speed, Lon_Acc, Lat_Acc, Time)
FROM './cleanData.csv'
DELIMITER ','
CSV HEADER;

