import time
import datetime
import pymongo

class DBOpsHandler:

	def __init__(self, client, config):
		self.client = client
		self.config = config

	def getClient(self):
		return self.client

	def getConfig(self):
		return self.config

	def getDB(self):
		client = self.getClient()
		config = self.getConfig()
		return client[config.getDbName()]

	def getCollection(self):
		client = self.getClient()
		config = self.getConfig()
		db = self.getDB()
		return db[config.getCollectionName()]

	def insertRecord(self, record):
		collection = self.getCollection()
		
		startTime = time.time()
		try:
			returnID = collection.insert(record)
			endTime = time.time()
		except pymongo.errors.OperationFailure:
			print "Operation Failure: Insert Failure"
			endTime = startTime - 100

		elapsedTime = endTime - startTime
		
		dt = datetime.datetime.fromtimestamp(time.time())
		print "Insert latency: " + str(dt) + ", " + str(elapsedTime)

	def readRecord(self, query):
		collection = self.getCollection()
		
		startTime = time.time()
		record = None
		try:
			record = collection.find_one(query)
			endTime = time.time()
		except pymongo.errors.OperationFailure:
			print "Operation Failure: Read Failure"
			endTime = startTime

		elapsedTime = endTime - startTime
		if record == None:
			elapsedTime = -100

		print "Read output: " + str(record)
		dt = datetime.datetime.fromtimestamp(time.time())
		print "Read latency: " + str(dt) + ", " + str(elapsedTime)

	def updateRecord(self, query, update):
		collection = self.getCollection()
		startTime = time.time()
		try:
			returnStatus = collection.update(query, update)
			if returnStatus.get("updatedExisting", False):
				endTime = time.time()
			else:
				print "Operation Failure: Update Failure (No record updated)"
				endTime = startTime - 100
		except pymongo.errors.OperationFailure:
			print "Operation Failure: Update Failure"
			endTime = startTime - 100

		elapsedTime = endTime - startTime
		
		dt = datetime.datetime.fromtimestamp(time.time())
		print "Update latency: " + str(dt) + ", " + str(elapsedTime)
