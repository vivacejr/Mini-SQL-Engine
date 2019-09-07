import csv
import sys
import re
from collections import OrderedDict

METADATA_FILE = "../files/metadata.txt"
agg_flag = 0;

def main():
	dictionary = {}
	dictionary2 = {}
	getMetadata(dictionary)
	getCsvdata(dictionary, dictionary2)
	if len(sys.argv) != 2:
		print("ERROR : invalid args")
		exit(-1)

	query = str(sys.argv[1])
	solve(query,dictionary,dictionary2)

def getMetadata(dictionary):
	f = open(METADATA_FILE,'r')
	cur_table = " "
	for line in f:
		if line.strip() == "<begin_table>":
			cur_table = next(f)
			cur_table=cur_table.strip()
			dictionary[cur_table] = []
		elif line.strip() != "<end_table>" :
			str = cur_table + '.' + line.strip()
			dictionary[cur_table].append(str)

def getCsvdata(dictionary,dictionary2):
	for key in dictionary:
		cur_table = key
		dictionary2[cur_table]=[]
		c = 0
		for x in dictionary[key]:
			ary = []
			tableName = cur_table + '.csv'
			tableContent = readCsv(tableName) 
			for data in tableContent:
				ary.append(data[c])
			c = c + 1
			dictionary2[cur_table].append(ary)

	# print(dictionary2)

def getTables(str, dictionary):
	ret = str.split(',')
	#check if all tables are present
	j=0
	for i in ret:
		ret[j]=ret[j].strip()
		if ret[j] not in dictionary.keys():
			sys.exit("Table not found")
		j = j + 1
			#change it to "tablename" not found
	return ret

def getColoumns(str, dictionary):
	#removing select
	str = str[7:]
	ret = str.split(',');
	j=0
	for i in ret:
		ret[j]=ret[j].strip()
		j = j + 1
	return ret

def join(tables, dictionary,dictionary2,qdict,qdict2):
	f = 0
	for tab in tables:
		if f == 0:
			for x in dictionary[tab]:
				qdict.append(x)
			qdict2=dictionary2[tab]
			f = 1
		else :
			tdict = []
			tdict2 = []
			for x in dictionary[tab]:
				tdict.append(x)
				qdict.append(x)
			tdict2=dictionary2[tab]
			n = len(qdict2[0])
			m = len(tdict2[1])
			cnt = 0
			for x in qdict2:
				for i in range (n*m):
					if i >= n:
						qdict2[cnt].append(qdict2[cnt][i%n])
				cnt = cnt + 1
			cnt= 0
			for x in tdict2:
				ary = []
				for i in range(n*m):
					ary.append(tdict2[cnt][i/n])
				qdict2.append(ary)
				cnt = cnt+1
	return qdict2

def solve_columns(columns,qdict,qdict2):
	req = []
	req2 = []

	cnt = 0
	for x in columns :
		if '(' in x : 
			agg_flag=1

	#change all columns to tablename.columnname
	for x in columns:
		if agg_flag == 1:
			if '(' not in x :
				sys.exit("Invalid column names")	
			else:
				col = ((x.split('('))[1].split(')'))[0]
				if '.' not in col :
					for val in qdict:
						z = (val.split('.'))[1]
						if z == col :
							col = val
				if '.' not in col :
					sys.exit("Invalid column names")
				columns[cnt]=(x.split('('))[0] + '(' + col + ')' 
				req.append(col)
		else :
			col = x
			if '.' not in col :
				for val in qdict:
					z = (val.split('.'))[1]
					if z == col :
						col = val
			if '.' not in col :
				sys.exit("Invalid column names")			
			req.append(col)
			columns[cnt]=col
		cnt = cnt + 1

	#remove columns which are not required
	tdict = []
	tdict2 = []
	cnt = 0 
	for x in qdict:
		if x in req:
			tdict.append(x)
			tdict2.append(qdict2[cnt])
		cnt = cnt +1 
	qdict = tdict
	qdict2 = tdict2
	return qdict2

def solve_where(where_object, qdict, qdict2):
	and_flag = 0
	or_flag = 0
	cond1 = ""
	cond2 = ""
	if 'AND' in where_object:
		and_flag = 1
		temp = where_object.split('AND')
		cond1 = temp[0].strip()
		cond2 = temp[1].strip()
	elif 'OR' in where_object:
		or_flag = 1
		temp = where_object.split('AND')
		cond1 = temp[0].strip()
		cond2 = temp[1].strip()
	else:
		cond1 = where_object.strip()

	colid1lhs=-1
	colid1rhs=-1
	col1lhs=""
	col1rhs=""
	num_flag1=0
	temp1 = []
	if '>' in cond1:
		temp1=cond1.split('>')
	elif '>=' in cond1:
		temp1=cond1.split('>=')
	elif '<' in cond1:
		temp1=cond1.split('<')
	elif '<' in cond1:
		temp1=cond1.split('<=')
	elif '=' in cond1:
		temp1=cond1.split('=')
	
	col1lhs=temp1[0].strip()
	col1rhs=temp1[1].strip()
	if col1rhs[0].isdigit():
		num_flag1=1
		colid1rhs =  int(col1rhs)

	cnt = 0  
	for x in qdict:
		y = (x.split('.'))[1]
		if col1lhs == x or col1lhs == y:
			colid1lhs = cnt
		if num_flag1 == 0:
			if col1rhs == x or col1rhs == y:
				colid1rhs = cnt			
		cnt = cnt + 1

	list1 = []
	cnt = 0
	for i in qdict2[0]:
		if '>' in cond1:
			if num_flag1 == 1:
				if int(qdict2[colid1lhs][cnt]) > colid1rhs:
					list1.append(cnt)
			else :	
				if int(qdict2[colid1lhs][cnt]) > int(qdict2[colid1rhs][cnt]):
					list1.append(cnt)		
		elif '>=' in cond1:
			if num_flag1 == 1:
				if int(qdict2[colid1lhs][cnt]) >= colid1rhs:
					list1.append(cnt)
			else :	
				if int(qdict2[colid1lhs][cnt]) >= int(qdict2[colid1rhs][cnt]):
					list1.append(cnt)		
		elif '<' in cond1:
			if num_flag1 == 1:
				if int(qdict2[colid1lhs][cnt]) < colid1rhs:
					list1.append(cnt)
			else :	
				if int(qdict2[colid1lhs][cnt]) < int(qdict2[colid1rhs][cnt]):
					list1.append(cnt)		
		elif '<=' in cond1:
			if num_flag1 == 1:
				if int(qdict2[colid1lhs][cnt]) <= colid1rhs:
					list1.append(cnt)
			else :	
				if int(qdict2[colid1lhs][cnt]) <= int(qdict2[colid1rhs][cnt]):
					list1.append(cnt)		
		elif '=' in cond1:
			if num_flag1 == 1:
				if int(qdict2[colid1lhs][cnt]) == colid1rhs:
					list1.append(cnt)
			else :	
				if qdict2[colid1lhs][cnt] == qdict2[colid1rhs][cnt]:
					list1.append(cnt)				
		cnt = cnt + 1

	if and_flag == 0 and or_flag == 0:
		tdict2 = []
		for x in qdict2:
			ary = []
			for y in list1:
				ary.append(x[y])
			tdict2.append(ary)
		qdict2=tdict2
		return qdict2

	colid2lhs=0
	colid2rhs=0
	col2lhs=""
	col2rhs=""
	num_flag2=0	
	temp2 = []
	if '>' in cond2:
		temp2=cond2.split('>')
	elif '>=' in cond2:
		temp2=cond2.split('>=')
	elif '<' in cond2:
		temp2=cond2.split('<')
	elif '<' in cond2:
		temp2=cond2.split('<=')
	elif '=' in cond2:
		temp2=cond2.split('=')

	col2lhs=temp2[0].strip()
	col2rhs=temp2[1].strip()
	if col2rhs[0].isdigit():
		num_flag2 = 1
		colid2rhs =  int(col2rhs)
	cnt = 0  
	for x in qdict:
		y = (x.split('.'))[1]
		if col2lhs == x or col2lhs == y:
			colid2lhs = cnt
		if num_flag2 == 0:
			if col2rhs == x or col2rhs == y:
				colid2rhs = cnt			
		cnt = cnt + 1

	list2 = []
	cnt = 0
	for i in qdict2[0]:
		if '>' in cond2:
			if num_flag2 == 1:
				if int(qdict2[colid2lhs][cnt]) > colid2rhs:
					list2.append(cnt)
			else :	
				if int(qdict2[colid2lhs][cnt]) > int(qdict2[colid2rhs][cnt]):
					list2.append(cnt)		
		elif '>=' in cond2:
			if num_flag2 == 1:
				if int(qdict2[colid2lhs][cnt]) >= colid2rhs:
					list2.append(cnt)
			else :	
				if int(qdict2[colid2lhs][cnt]) >= int(qdict2[colid2rhs][cnt]):
					list2.append(cnt)		
		elif '<' in cond2:
			if num_flag2 == 1:
				if int(qdict2[colid2lhs][cnt]) < colid2rhs:
					list2.append(cnt)
			else :	
				if int(qdict2[colid2lhs][cnt]) < int(qdict2[colid2rhs][cnt]):
					list2.append(cnt)		
		elif '<=' in cond2:
			if num_flag2 == 1:
				if int(qdict2[colid2lhs][cnt]) <= colid2rhs:
					list2.append(cnt)
			else :	
				if int(qdict2[colid2lhs][cnt]) <= int(qdict2[colid2rhs][cnt]):
					list2.append(cnt)		
		elif '=' in cond2:
			if num_flag2 == 1:
				if int(qdict2[colid2lhs][cnt]) == colid2rhs:
					list2.append(cnt)
			else :	
				if int(qdict2[colid2lhs][cnt]) == int(qdict2[colid2rhs][cnt]):
					list2.append(cnt)
		cnt = cnt + 1
	
	list = []
	if and_flag == 1:
		for x in list1:
			if x in list2:
				list.append(x)
	if or_flag == 1:
		list = list1
		for x in list2:
			if x not in list1:
				list.append(x)
	list.sort()
	tdict2 = []
	for x in qdict2:
		ary = []
		for y in list:
			ary.append(x[y])
		tdict2.append(ary)
	qdict2=tdict2
	return qdict2


def solve(query,dictionary, dictionary2):
	if "from" in query:
		temp1 = query.split('from');
	else:
		sys.exit("Incorrect Syntax")		
	column_object = temp1[0]
	columns = getColoumns(column_object, dictionary)
	
	temp2 = temp1[1].split('where')
	table_object = temp2[0]
	tables= getTables(table_object, dictionary)

	if len(tables)==0 :
		sys.exit("Zero Tables Provided")

	#join
	qdict = []
	qdict2 = []
	qdict2=join(tables,dictionary,dictionary2,qdict,qdict2)

	qdict2= solve_columns(columns, qdict, qdict2)


	where_object = temp2[1].strip()
	qdict2=solve_where(where_object,qdict,qdict2)
	print qdict
	print qdict2



def readCsv(tableName):
	tableContent = []
	tableName='../files/'+tableName
	with open(tableName,'rb') as f:
		file = csv.reader(f)
		for line in file:
			tableContent.append(line)

	return tableContent
if __name__ == "__main__":
	main()
