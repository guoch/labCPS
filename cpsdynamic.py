#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys 
reload(sys)
sys.setdefaultencoding('utf-8')
#from numpy import *
#from hive_service import ThriftHive  
#from hive_service.ttypes import HiveServerException  
#from thrift import Thrift  
#from thrift.transport import TSocket  
#from thrift.transport import TTransport  
#from thrift.protocol import TBinaryProtocol  
import pyhs2
conn=pyhs2.connect(host='ubuntu1',port=10000,authMechanism='PLAIN',user='scidb',password='hive',database='default',)

'''
class TestHandle(ContentHandler):
    def __init__(self, inlist):
        self.inlist = inlist
        
    def startElement(self,name,attrs):
        print 'name:',name, 'attrs:',attrs.keys()
        
    def endElement(self,name):
        print 'endname',name
        
    def characters(self,chars):
        print 'chars',chars
        self.inlist.append(chars)
'''

'''模型设计 冗余
如何切分 
两种渠道 一种为hive增加partition的方法
另外一种方法为 mv操作，但是有必要mv吗 移动究竟是否有价值 因为可以事先得到查询的列表
问题是优化器选择的直接作为mapreduce输入而不是用了mapreduce下的某个文件夹
mv是否可以将小表格load进memory里面
根据某个attribute得到所有属于该属性的数据

''' 

def encodePartition(shipdate_start,shipdate_end,discount_min,discount_max,shipmode):
# match  UDF='1971-01-01|1994-01-01|0.00|0.05|OTHER'
    #discount_min_str=
    #discount_max_str=
    return "UDF=\'"+shipdate_start+'|'+shipdate_end+'|'+discount_min+'|'+discount_max+'|'+shipmode+"\'"




def CPSModel():
    weight=0
    return 0

def loadDataset(fileName):
    dataMat=[]
    fr=open(fileName)
    for line in fr.readlines():
        curLine=line.strip().split('\t')
        fltLine=map(float,curLine)
        dataMat.append(fltLine)
    return dataMat



class HiveClient:
    def __init__(self, db_host, user, password, database, port=10000, authMechanism="PLAIN"):
        self.conn = pyhs2.connect(host=db_host,port=port,authMechanism=authMechanism,user=user,password=password,database=database,)
    def query(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetch()
    def close(self):
        self.conn.close()




#以下函数针对于hive server1版本，可能不适应于hive2版本
'''
def hiveExe(sql):  
    try:  
        transport = TSocket.TSocket('10.141.211.91', 10000)   
        transport = TTransport.TBufferedTransport(transport)  
        protocol = TBinaryProtocol.TBinaryProtocol(transport)  
        client = ThriftHive.Client(protocol)  
        transport.open()  
        client.execute(sql)  


        print "The return value is : "   
        print client.fetchAll()  
        print "............"  
        transport.close()  
    except Thrift.TException, tx:  
        print '%s' % (tx.message)  
'''


def renamePartition():
    #conn=pyhs2.connect(host='ubuntu1',port=10000,authMechanism='PLAIN',user='hive',password='hive',database='default',)
    cur=conn.cursor()
    cur.execute('show partitions lineitem_lab2')
    result2=cur.fetch()
    #hive_client=HiveClient(db_host='ubuntu1',port=10000,user='hive',password='hive',database='default')
    #result=hive_client.query('show tables')
    #print result
    #result2=hive_client.query('show partitions lineitem_lab2')
    '''
    for t in result2:
        oldsql=t[0][0:4]+"'"+t[0][4:]+"'"
        newsql=t[0][0:4]+"'"+t[0][4:40]+"|0'"
        try:
            hiveql="alter table lineitem_lab2 partition("+oldsql+") rename to partition("+newsql+")"
            cur2=conn.cursor()
            cur2.execute(hiveql)
            
        except Exception, e:
            print "this is an error"   
    '''

def createPartition():
    #cur=conn.cursor()
    #cur.execute('show partitions lineitem_lab2')
    #result=cur.fetch()
    result=[
'udf=1971-01-01|1994-01-01|0.00|0.05|MAIL|0',
'udf=1971-01-01|1994-01-01|0.00|0.05|OTHE|0',
'udf=1971-01-01|1994-01-01|0.00|0.05|SHIP|0',
'udf=1971-01-01|1994-01-01|0.05|0.07|MAIL|0',
'udf=1971-01-01|1994-01-01|0.05|0.07|OTHE|0',
'udf=1971-01-01|1994-01-01|0.05|0.07|SHIP|0',
'udf=1971-01-01|1994-01-01|0.07|1.00|MAIL|0',
'udf=1971-01-01|1994-01-01|0.07|1.00|OTHE|0',
'udf=1971-01-01|1994-01-01|0.07|1.00|SHIP|0',
'udf=1994-01-01|1997-09-02|0.00|0.05|MAIL|0',
'udf=1994-01-01|1997-09-02|0.00|0.05|OTHE|0',
'udf=1994-01-01|1997-09-02|0.00|0.05|SHIP|0',
'udf=1994-01-01|1997-09-02|0.05|0.07|MAIL|0',
'udf=1994-01-01|1997-09-02|0.05|0.07|OTHE|0',
'udf=1994-01-01|1997-09-02|0.05|0.07|SHIP|0',
'udf=1994-01-01|1997-09-02|0.07|1.00|MAIL|0',
'udf=1994-01-01|1997-09-02|0.07|1.00|OTHE|0',
'udf=1994-01-01|1997-09-02|0.07|1.00|SHIP|0',
'udf=1997-09-02|2015-08-01|0.00|0.05|MAIL|0',
'udf=1997-09-02|2015-08-01|0.00|0.05|OTHE|0',
'udf=1997-09-02|2015-08-01|0.00|0.05|SHIP|0',
'udf=1997-09-02|2015-08-01|0.05|0.07|MAIL|0',
'udf=1997-09-02|2015-08-01|0.05|0.07|OTHE|0',
'udf=1997-09-02|2015-08-01|0.05|0.07|SHIP|0',
'udf=1997-09-02|2015-08-01|0.07|1.00|MAIL|0',
'udf=1997-09-02|2015-08-01|0.07|1.00|OTHE|0',
'udf=1997-09-02|2015-08-01|0.07|1.00|SHIP|0']

    for t in result:
        getsql=decode(t)
        cur2=conn.cursor()
        cur2.execute(getsql)

def decode_attribute(udfstring):
    begin_time=udfstring[4:14]
    end_time=udfstring[15:25]
    low_discount=udfstring[26:30]
    high_discount=udfstring[31:35]
    shipmode=udfstring[36:40]
    flag=udfstring[41:42]
    atttibutes=[begin_time,end_time,low_discount,high_discount,shipmode,flag]
    return atttibutes

def decode(udfstring):
    oldsql=udfstring[0:4]+"'"+udfstring[4:]+"'"
    begin_time=udfstring[4:14]
    end_time=udfstring[15:25]
    low_discount=udfstring[26:30]
    high_discount=udfstring[31:35]
    shipmode=udfstring[36:40]
    flag=udfstring[41:42]
    #(L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02')
    sql_timeline="(L_SHIPDATE>='"+begin_time+"' and L_SHIPDATE<'"+end_time+"')"
    #(L_DISCOUNT>=0.05 and L_DISCOUNT<0.07)
    sql_discount="(L_DISCOUNT>="+low_discount+" and L_DISCOUNT<"+high_discount+")"
    #(L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL')   L_SHIPMODE='SHIP'
    if shipmode=='OTHE':
        sql_shipmode="(L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL')"
    else:
        sql_shipmode="L_SHIPMODE='"+shipmode+"'"
    #insert overwrite table lineitem_lab2 partition (UDF='1994-01-01|1997-09-02|0.05|0.07|OTHET') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );
    udfword=udfstring[0:4]+"'"+udfstring[4:]+"'"
    create_sql="insert overwrite table lineitem_lab2 partition ("+udfword+") select * FROM lineitem_tmp where "+sql_timeline+" and "+sql_discount+" and "+sql_shipmode
    return create_sql

def getborderString(udfstring):
    oldsql=udfstring[0:4]+"'"+udfstring[4:]+"'"
    begin_time=udfstring[4:14]
    end_time=udfstring[15:25]
    low_discount=udfstring[26:30]
    high_discount=udfstring[31:35]
    shipmode=udfstring[36:40]
    flag=udfstring[41:42]
    #(L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02')
    sql_timeline="(L_SHIPDATE>='"+begin_time+"' and L_SHIPDATE<'"+end_time+"')"
    #(L_DISCOUNT>=0.05 and L_DISCOUNT<0.07)
    sql_discount="(L_DISCOUNT>="+low_discount+" and L_DISCOUNT<"+high_discount+")"
    #(L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL')   L_SHIPMODE='SHIP'
    if shipmode=='OTHE':
        sql_shipmode="(L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL')"
    else:
        sql_shipmode="L_SHIPMODE='"+shipmode+"'"
    #insert overwrite table lineitem_lab2 partition (UDF='1994-01-01|1997-09-02|0.05|0.07|OTHET') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );
    udfword=udfstring[0:4]+"'"+udfstring[4:]+"'"
    borderstring=[udfword,sql_timeline,sql_discount,sql_shipmode]
    return borderstring





def judgeConflict(udfstring1,udfstring2):
    udflist1=decode_attribute(udfstring1)
    udflist2=decode_attribute(udfstring2)
    if udflist1[5]==udflist2[5]:
        return false
    else:
        if (udflist1[0]<udflist2[0] and udflist1[1]>udflist2[0]) or (udflist2[0]<udflist1[0] and udflist2[1]<udflist1[0]):
            return true
        else:
            return false      
    # This is by wuzhigang
    
    return
'''
#include <iostream>
 using namespace std;

 #define MAX(a, b) ((a >= b) ? a : b)
 #define MIN(a, b) ((a <= b) ? a : b)

 struct INTERVAL //区间
{
float left;
 float right;
 };

 struct RECT //矩形
{
float left;
 float right;
 float top;
 float bottom;
 };

 bool interval_intersect(INTERVAL t1, INTERVAL t2, INTERVAL &t3) //求区间t1和t2的交（一维）
{
if (t1.right < t2.left || t1.left > t2.right)
 return false; 
 else 
 {
 t3.left = MAX(t1.left, t2.left);
 t3.right = MIN(t1.right, t2.right);
 return true; 
 }
 }

'''
def intersect(t1_left,t1_right,t2_left,t2_right):
	if(t1_right<t2_left or t1_left>t2_right):
		return false  #no conflict
	else:
		t3_left=MAX(t1_left,t2_left)
		t3_right=MIN(t1_right,t2_right)
		return true


def rect_intersect(t1_left,t1_right,t1_low,t1_high,t2_left,t2_right,t2_low,t2_high):
	if intersect(t1_left,t1_right,t2_left,t2_right) and intersect(t1_low,t1_high,t2_low,t2_high):
		return true
	else:
		return false

#该函数用于判断两个能否覆盖一个矩形
def proper_intersect(t1_left,t1_right,t2_left,t2_right,query_left,query_right):
	if(t1_right==t2_left and t1_left<=query_left and t2_right>=query_right) or (t1_left==t2_right and t2_left<=query_left and t1_right>=query_right):
		retun true
	else:
		return false




'''
 bool rect_intersect(RECT r1, RECT r2, RECT &r3) //矩形求交（二维）
{
INTERVAL t1, t2, t3, t4, t5, t6;

 t1.left = r1.left;
 t1.right = r1.right;
 t2.left = r2.left;
 t2.right = r2.right;
 t4.left = r1.top;
 t4.right = r1.bottom;
 t5.left = r2.top;
 t5.right = r2.bottom;
 if (interval_intersect(t1, t2, t3) && interval_intersect(t4, t5, t6))
 {
 r3.left = t3.left;
 r3.right = t3.right;
 r3.top = t6.left;
 r3.bottom = t6.right;
 return true;
 }
 return false;
 }



'''



'''

def coverxy(x,y):
	return
	

def calrate(table1,table2,timebegin1,timeend1,timebegin2,timeend2):
    string1="select count(*) from "+table1
    string2="select count(*) from "+table2
    ratecur=conn.cursor()
    rs=ratecur.execute(getsql)
    






    return

#由于end_time为不可到达的上界小于等于即可
def scanPartition(start_time,end_time):
    cur_scan=conn.cursor()
    cur_scan.execute('show partitions lineitem_lab2')
    result2=cur_scan.fetch()
    scanlist=[]
    for singlelist in result2:
        partname=decode_attribute(singlelist[0])
        if partname[0]>=start_time and partname[1]<=end_time:
            scanlist.append(singlelist)
        else:
            pass
    return scanlist 

def getScanSql(scanlist):
    scanPartition('1994-02-06','1995-07-08')







    



if __name__ == '__main__':  
    mytest=decode('udf=1971-01-01|1994-01-01|0.00|0.05|MAIL|0')
    print mytest
    mytest2=decode('udf=1994-01-01|1997-09-02|0.00|0.05|OTHE|0')
    print mytest2
    createPartition()
    


            

        




    





