a = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
dump a;
b = GROUP a BY (gender, occupation);
c = FILTER b BY (group.occupation == 'lawyer') AND (group.gender == 'M');
d = FOREACH c GENERATE a;
e = FOREACH d GENERATE flatten($0);
f = ORDER e BY a::age DESC;
g = LIMIT f 1;
STORE g INTO '2-2-output';
-- dump g;
-- (10,53,M,lawyer,90703)
-- userId: 10

