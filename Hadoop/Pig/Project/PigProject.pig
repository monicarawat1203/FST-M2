Script1 = LOAD 'hdfs:///user/AKS/episodeIV_dialogues.txt' USING PigStorage('\t') AS (Actor:chararray,Dialogue:chararray);
Script2 = LOAD 'hdfs:///user/AKS/episodeVI_dialogues.txt' USING PigStorage('\t') AS (Actor:chararray,Dialogue:chararray);
Script3 = LOAD 'hdfs:///user/AKS/episodeV_dialogues.txt' USING PigStorage('\t') AS (Actor:chararray,Dialogue:chararray);
allScript= Union Script1,Script2,Script3;
GroupByActor = GROUP allScript BY Actor;
CountByActor = FOREACH GroupByActor GENERATE $0 AS Actor,COUNT($1) AS Dialogue;
orderbydesc= order CountByActor by Dialogue desc;
STORE orderbydesc INTO 'hdfs:///user/AKS/pig_project_result/DialogueOutput_updated' USING PigStorage('\t');
