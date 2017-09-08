import random
import string
import sys
import os
import argparse
import csv 

def main():
	print("Start")
	########################################
	# parse arguments
	########################################
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", dest = "metadata_file", help="full path to metadata file")
	parser.add_argument("-f", dest = "file_name", default='data_feed.csv', help="output feed file name")
	parser.add_argument("-nrec", dest = "num_rec",default=10, help="number of records")
	parser.add_argument("-d", dest = "delimiter", default=',', help="field delimiter")
	parser.add_argument("-hdr", dest = "header_req", default='N', help="Y if header is required otherwise N")
	
	args = parser.parse_args()	
	metadata_file = args.metadata_file
	file_name = args.file_name
	num_rec = int(args.num_rec)
	delimiter = args.delimiter
	header_req = args.header_req.upper()
	
	#Raise error if metadata is not passed in Parameter
	if not (metadata_file):
		parser.print_help()
		sys.exit()
	
	#Open the file for writing
	feed_file=open(file_name,"w")
	record = ''
	record_type=[]
	field_type=[]
		
	with open(metadata_file) as metadata:
		reader = csv.reader(metadata)
		j=0
		for row in reader:
			field_type=[]
			if j>0:
				field_type.append(row[0])
				field_type.append(row[1])
				field_type.append(row[2])
				field_type.append(row[3])
				record_type.append(field_type)
			j+=1
			
	print("Generating the Feed File Named "+file_name+" With "+str(len(record_type))+" Columns and Delimiter = "+delimiter)	
	
	header=''
	if header_req == 'Y':
		for rec in record_type:
			header=header+rec[0]+delimiter
		header=header[:len(header)-1]+"\n"
		feed_file.write(header)
		
	for i in xrange(num_rec):
		record=''
		for rec in record_type:
			record= record+getData(rec[1],rec[2],rec[3])+delimiter
		record=record[:len(record)-1]+"\n"
		feed_file.write(record)

	feed_file.close()
	print("File "+file_name+" Generated Successfully")

def getData(p_dataType, p_length, p_type):
	value =''
	dataType = p_dataType.upper()
	length=int(p_length)
	type=p_type.upper()
	if 'INT' in dataType or 'NUM' in dataType:
		if type == 'FIXED':
			return ''.join(random.choice(string.digits) for _ in range(length))
		else:
			length=random.randint(1,length)
			return ''.join(random.choice(string.digits) for _ in range(length))			
	elif 'CHAR' in dataType or 'STR' in dataType:
		if type == 'FIXED':
			return ''.join(random.choice(string.ascii_uppercase))+''.join(random.choice(string.ascii_lowercase) for _ in range(length-1))
		elif type != 'FIXED' and len(type)>0:
			return ''.join(random.choice(p_type.split(",")))
		else:
			length=random.randint(0,length)
			return ''.join(random.choice(string.ascii_uppercase))+''.join(random.choice(string.ascii_lowercase) for _ in range(length-1))		
		
		
if __name__ == '__main__':
    main()