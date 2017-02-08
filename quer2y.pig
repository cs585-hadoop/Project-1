
cus = LOAD 'hdfs://localhost:8020/user/hadoop/cache/customer.csv' USING PigStorage(',') as (ID:chararray,name:chararray,age:int,cc:int,salary:float);

trans = LOAD 'hdfs://localhost:8020/user/hadoop/p_input/transaction.csv' USING PigStorage(',') as (transID:chararray,cusID:chararray,transTotal:float,transNum:int,transDesc:chararray);

cus_tran_join = JOIN cus BY ID, trans BY cusID USING 'replicated';

cusid_group = group cus_tran_join by cus.ID;

result = LIMIT cusid_group 5; 

STORE result INTO 'pigresult' USING PigStorage(',');





