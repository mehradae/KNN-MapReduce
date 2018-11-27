points = load '$Inputpath' using PigStorage(',') as (id:long, x:double, y:double);
distance = FOREACH points {
GENERATE SQRT((x-$X)*(x-$X)+(y-$Y)*(y-$Y)) as dist:double ,(x),(y);}
limit_distance = ORDER distance BY dist ASC;
limit_distance_asc = LIMIT limit_distance (long)$k;
dump limit_distance_asc;
/* if we want to store it into HDFS*/
/*STORE limit_distance_asc INTO ' hdfs://localhost/ ' USING PigStorage (',');*/
