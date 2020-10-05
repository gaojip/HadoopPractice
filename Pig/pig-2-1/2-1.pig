a = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
dump a;
b = GROUP a BY (gender, occupation);
c = FILTER b BY (group.occupation == 'lawyer') AND (group.gender == 'M');
counts = FOREACH c GENERATE group, COUNT(a);
STORE counts INTO '2-1-output';
-- dump counts;
-- ((M,lawyer),10)

