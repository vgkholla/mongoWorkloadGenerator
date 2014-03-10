from pprint import pprint
import time
import random
class DistributionGenerator(object):

	def __init__(self, client, config, dbOpsHandler, recordPreloader, extraRecordGetter):
		self.client = client
		self.config = config
		self.dbOpsHandler = dbOpsHandler
		self.recordPreloader = recordPreloader
		self.extraRecordGetter = extraRecordGetter
		self.mapping = self.getRecordPreloader().initMapping()
		self.setNextLineNum(0);

	def getClient(self):
		return self.client

	def getConfig(self):
		return self.config

	def getDBOpsHandler(self):
		return self.dbOpsHandler

	def getRecordPreloader(self):
		return self.recordPreloader

	def getExtraRecordGetter(self):
		return self.extraRecordGetter

	def getMapping(self):
		return self.mapping

	def getKeyInMapping(self, key):
		mapping = self.getMapping()
		return mapping.get(key, None)

	def getNextLineNum(self):
		return self.nextLineNum

	def setNextLineNum(self, num):
		self.nextLineNum = num;

	def incrementNextLineNum(self):
		self.nextLineNum += 1

	def insertKeyInMapping(self, key, value):
		mapping = self.getMapping()
		mapping[key] = value

	def stripToIndices(self, record):
		config = self.getConfig()
		shardKey = config.getShardKey()
		reshardKey = config.getReshardKey()

		strippedRecord = dict()
		
		strippedRecord[shardKey] = record[shardKey]
		strippedRecord[reshardKey] = record[reshardKey]
		
		return strippedRecord

	def getNextRecordToInsert(self):
		record = self.getExtraRecordGetter().getRecordAtLine(self.getNextLineNum())
		self.incrementNextLineNum()
		return record

	def getQuery(self, record):
		return record


	def getOpAndRecord(self):
		recordNum = self.getNextIndex()
		op = ""
		record = None
		value = self.getKeyInMapping(recordNum)
		if value == None:
			op = "i"
			record = self.getNextRecordToInsert()
			self.insertKeyInMapping(recordNum, self.stripToIndices(record))
		else:
			tieBreaker = int(random.uniform(0,100000))
		 	if tieBreaker % 2 == 0:
		 		op = "r"
		 	else:
		 		op = "u"

		 	record = self.getQuery(value)

		return op, record

	def getUpdateQuery(self, record):
		change = dict(self.stripToIndices(record))
		change[self.getConfig().getChangeColumn()] = "10"
		return change

		#update = {"$set" : change}
		#return update

	def applyWorkload(self):
		config = self.getConfig()
		sleepTime = config.getSleepTime()

		while True:
			op, record = self.getOpAndRecord()
			print ("Op is " + op + ". Record is " + str(record))
			self.applyOperation(op, record)
			time.sleep(float(sleepTime/1000000))

	def applyOperation(self, op, record):
		dbOpsHandler = self.getDBOpsHandler()
		if op == "i":
			dbOpsHandler.insertRecord(record)
		elif op == "r":
			dbOpsHandler.readRecord(record)
		elif op == "u":
			update = self.getUpdateQuery(record)
			dbOpsHandler.updateRecord(record, update)