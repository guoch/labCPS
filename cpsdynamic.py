#!/usr/bin/env python
# -*- coding: utf-8 -*-
from numpy import *
from xml.sax.handler import ContentHandler
from xml.sax import parse
         
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


def decode(udfstring):
    

            
if __name__ == '__main__':
    lt = []
    parse('test.xml', TestHandle(lt))
    print lt

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



