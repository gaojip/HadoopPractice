REGISTER /usr/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

a = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING CSVLoader() AS (movieId:int, title:chararray, genres:chararray);
b = FILTER a BY title MATCHES '(?i)a.*';
c = FOREACH b GENERATE FLATTEN(TOKENIZE(genres, '|')) AS genre;
d = FOREACH (GROUP c BY genre) GENERATE group AS genre, COUNT(c) AS genreCount;
e = ORDER d BY genre;
STORE e INTO '2-3-output';



