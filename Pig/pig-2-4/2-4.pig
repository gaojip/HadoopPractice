-- userId, movieId, rating, timestamp	ratings.txt
-- movieId, title, genres		movies.csv
-- movieId, genres, rating, title	Needed

REGISTER /home/cloudera/Desktop/Pig/jars/PigUDF.jar; -- jar file name must same with package name, will not error with 1070
REGISTER /usr/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

a = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING CSVLoader() AS (movieId:int, title:chararray, genres:chararray);
b = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating: int, timestamp:int);
c = FILTER b BY rating == 5;
d = FOREACH c GENERATE movieId, rating;
e = FILTER a BY genres MATCHES '.*Adventure.*';
f = JOIN e BY movieId, d BY movieId;
g = FOREACH f GENERATE e::movieId AS movieId, PigUDF.HandleGenres(e::genres) AS genres, d::rating AS rating, e::title AS title;
h = DISTINCT g;
i = LIMIT h 20;
STORE i INTO '2-4-output';



