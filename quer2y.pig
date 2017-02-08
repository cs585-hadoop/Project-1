
cus = LOAD 'hdfs://localhost:8020/user/hadoop/customer/customer.csv' USING PigStorage(',') as (ID:chararray,name:chararray,age:int,cc:int,salary:float);

trans = LOAD 'hdfs://localhost:8020/user/hadoop/transaction/transaction.csv' USING PigStorage(',') as (transID:chararray,cusID:chararray,transTotal:float,transNum:int,transDesc:chararray);

cus_tran_join = JOIN cus BY ID, trans BY cusID USING 'replicated';

cusid_group = group cus_tran_join by (cus.ID,cus.name,cus.salary);

result = FOREACH cusid_group GENERATE group, COUNT(*),SUM(trans.transTotal),MIN(trans.transNum);  

STORE result INTO 'pigresult' USING PigStorage(',');





