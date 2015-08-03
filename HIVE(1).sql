
 


Create table lineitem_lab2 (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING) PARTITIONED BY(UDF STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/lineitem_lab2';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.05,SHIPMODE='MAIL') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT<0.05 and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.05,SHIPMODE='SHIP') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT<0.05 and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.05,SHIPMODE='OTHER') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT<0.05 and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.07,SHIPMODE='MAIL') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.07,SHIPMODE='SHIP') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.07,SHIPMODE='OTHRT') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.09,SHIPMODE='MAIL') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT>=0.07 and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.09,SHIPMODE='SHIP') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT>=0.07 and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.09,SHIPMODE='OTHER') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT>=0.07 and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );






hive直接读取hdfs上的数据 进行查询不是放到表里面  load 

cpsmodel hadoop mv 

load tmptable 





Create external table lineitem_tmp (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/lineitem_gch';

load data inpath '/tpch/lineitem/lineitem.tbl' into table lineitem_tmp;



insert overwrite table lineitem_lab partition (SHIPDATE,DISCOUNT,SHIPMODE) select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_COMMENT, L_SHIPINSTRUCT, L_SHIPMODE FROM lineitem_tmp where L_SHIPDATE>'1997-09-02' and L_DISCOUNT<0.05 and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1994-01-01',DISCOUNT=0.5,SHIPMODE='MAIL') select * FROM lineitem_tmp where L_SHIPDATE<'1994-01-01' and L_DISCOUNT<0.5 and L_SHIPMODE='MAIL';


insert overwrite table lineitem_partition partition (shipinstruct='NONE',shipmode='AIR')select * FROM lineitem_external where L_SHIPINSTRUCT='NONE' and L_SHIPMODE='AIR';

//动态分区 需要在最末尾加上分区表的属性信息才可以
//参见该文章

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;

insert overwrite table lineitem_partition partition (shipinstruct='NONE',shipmode)select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_COMMENT, L_SHIPINSTRUCT, L_SHIPMODE FROM lineitem_external where L_SHIPINSTRUCT='NONE';


添加分区

ALTER TABLE table_name ADD PARTITION (partCol = 'value1') location 'loc1'; //

示例
ALTER TABLE lineitem_lab ADD IF NOT EXISTS PARTITION (dt='newrail') LOCATION '/tpch/lineitem_lab/shipdate=1994-01-01/discount=0.05/shipmode=OTHER/dt=newrail'; //一


次添加一个分区

ALTER TABLE page_view ADD PARTITION (dt='2008-08-08', country='us') location '/path/to/us/part080808' PARTITION (dt='2008-08-09', country='us') location '/path/to/us/part080809';  //一次添加多个分区

删除分区

ALTER TABLE login DROP IF EXISTS PARTITION (dt='2008-08-08');
ALTER TABLE page_view DROP IF EXISTS PARTITION (dt='2008-08-08', country='us');