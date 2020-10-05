a = LOAD 'input/pig/InputForWC.txt' USING PigStorage('\n') AS (sen:chararray);
b = FOREACH a GENERATE TOKENIZE(sen, ' \n\t') AS wordBag;
c = FOREACH b GENERATE flatten($0) AS word;
d = GROUP c BY word;
-- describe d;
counts = FOREACH d GENERATE group, COUNT(c);
dump counts;
STORE counts INTO '1-1-output';
