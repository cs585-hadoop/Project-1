
cus = LOAD 'hdfs://localhost:8020/user/hadoop/cache/customer.csv' USING PigStorage(',') as (ID:chararray,name:chararray,age:int,cc:int,salary:float);

cc_group = GROUP cus BY cc;

temp_result = FOREACH cc_group GENERATE group, COUNT(cus.ID);

result = FILTER temp_result BY $1 > 5000 OR $1 < 2000;

STORE result INTO 'pigresult' USING PigStorage(',');





