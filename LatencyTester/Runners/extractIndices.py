import os,sys

def getCSVFileName(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <csv_file>"
		exit()

	csvFileName = getCSVFileName(args)
	return csvFileName

def getCSVFilePath(csvFileName):
	return os.path.abspath(csvFileName) 

def parseCSV(csvFileName):
	csvFile = getCSVFilePath(csvFileName)
	with open(csvFile) as f:
		for line in f:
			fields = line.split(",")
			print fields[0] + "," + fields[3]



csvFileName = checkAndReturnArgs(sys.argv)
parseCSV(csvFileName)
	