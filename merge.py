# -*- coding: utf-8 -*-
import math
import datetime

def mergePart(start,mergenum,lists):
	end=min(start+mergenum,len(lists))
	singeldict=lists[0]
	listDepth=len(singeldict)
	wc={}
	#count=0
	mergedList=[]
	for k in singeldict:
		count=0
		d={}
		for j in range(start,end):
			count=count+lists[j][k]
		d[k]=count
		mergedList.append(d)
	return mergedList


def merge(lists,mergenum):
	 

	numofMergedList=int(math.ceil(len(lists)/float(mergenum))) 
	#print numofMergedList
	mergedList=[]
	for i in range(0,numofMergedList):
		mergedList.append(mergePart(i*mergenum,mergenum,lists))	
	return mergedList


if __name__ == '__main__':
	#testlists=[{'aa':12,'bb':14,'cc':19},{'aa':19,'bb':17,'cc':21},{'aa':5,'bb':1,'cc':11},{'aa':6,'bb':7,'cc':1}]
	OriginLists=eval(open("D:\\lists200.txt").read())

	merge200=[100, 67, 50, 40, 34, 29, 25, 23, 20, 19, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2]
	for partnum in merge200:
		starttime = datetime.datetime.now()  
		merge(OriginLists,partnum)
		endtime = datetime.datetime.now() 
		print (endtime - starttime).microseconds





	#testlists=[{'aa':12,'bb':14,'cc':19},{'aa':19,'bb':17,'cc':21},{'aa':5,'bb':1,'cc':11}]



	#mylistsPart=mergePart(0,2,testlists)
	#print mylistsPart
	#mylistsAll=merge(testlists,2)
	#print mylistsAll




