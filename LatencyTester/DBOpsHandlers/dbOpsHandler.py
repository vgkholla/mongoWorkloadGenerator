import time
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
			print "Insert Failure"
			endTime = startTime - 100

		elapsedTime = endTime - startTime
		
		print "Insert latency: " + str(elapsedTime)

	def readRecord(self, query):
		collection = self.getCollection()
		
		startTime = time.time()
		try:
			record = collection.find_one(query)
			endTime = time.time()
		except pymongo.errors.OperationFailure:
			print "Read Failure"
			endTime = startTime

		elapsedTime = endTime - startTime
		if record == None:
			elapsedTime = -100
		print "Read latency: " + str(elapsedTime)

	def updateRecord(self, query, update):
		collection = self.getCollection()
		startTime = time.time()
		try:
			returnStatus = collection.update(query, update)
			endTime = time.time()
		except pymongo.errors.OperationFailure:
			print "Update Failure"
			endTime = startTime - 100

		elapsedTime = endTime - startTime
		
		print "Update latency: " + str(elapsedTime)