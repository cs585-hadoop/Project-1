cus = LOAD 'hdfs://localhost:8020/user/hadoop/customer/customer.csv' USING PigStorage(',') as (ID:chararray,name:chararray,age:int,cc:int,salary:float);
trans = LOAD 'hdfs://localhost:8020/user/hadoop/transaction/transaction.csv' USING PigStorage(',') as (transID:chararray,cusID:chararray,transTotal:float,transNum:int,transDesc:chararray);
trans_group = GROUP trans BY cusID;
trans_stat = FOREACH trans_group GENERATE group, COUNT(trans),SUM(trans.transTotal),MIN(trans.transNum); 
result = JOIN cus BY ID, trans_stat by $0 USING 'replicated';
final = FOREACH result generate $0,$1,$4,$6,$7,$8;
STORE final INTO 'pigresult' USING PigStorage(',');




