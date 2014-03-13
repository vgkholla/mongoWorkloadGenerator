import os
class ConfigGetter:
	def __init__(self, configFilePath):
		self.routerIP = "127.0.0.1"
		self.routerPort = 27017
		self.dbName = ""
		self.collectionName = ""
		self.rangeStart = 0
		self.rangeEnd = 0
		self.recordPreloadStart = 0
		self.numOpsPerSec = 0
		self.shardKey = ""
		self.reshardKey = ""
		self.changeColumn = ""
		self.initRecordsFilePath = ""
		self.extraRecordsFilePath = ""
		self.distribution = "uniform"

		self.parseConfigFile(configFilePath)

	def parseConfigFile(self, configFilePath):
		with open(configFilePath) as f:
			for line in f:
				if line[0] == "#":
					continue
				
				words = line.split("=")
				key = words[0]
				value = words[1].replace("\n", "")
				
				if key == "routerIP":
					self.routerIP = value
				elif key == "routerPort":
					self.routerPort = value
				elif key == "dbName":
					self.dbName = value
				elif key == "collectionName":
					self.collectionName = value
				elif key == "rangeStart":
					self.rangeStart = value
				elif key == "rangeEnd":
					self.rangeEnd = value
				elif key == "recordPreloadStart":
					self.recordPreloadStart = value
				elif key == "numOpsPerSec":
					self.numOpsPerSec = value
				elif key == "shardKey":
					self.shardKey = value
				elif key == "reshardKey":
					self.reshardKey = value
				elif key == "changeColumn":
					self.changeColumn = value
				elif key == "initRecordsFilePath":
					self.initRecordsFilePath = os.path.abspath(value)
				elif key == "extraRecordsFilePath":
					self.extraRecordsFilePath = os.path.abspath(value)
				elif key == "distribution":
					self.distribution = value

	def getRouterIP(self):
		return self.routerIP

	def getRouterPort(self):
		return int(self.routerPort)

	def getDbName(self):
		return self.dbName

	def getCollectionName(self):
		return self.collectionName

	def getRangeStart(self):
		return int(self.rangeStart)

	def getRangeEnd(self):
		return int(self.rangeEnd)

	def getRecordPreloadStart(self):
		return int(self.recordPreloadStart)

	def getNumOpsPerSec(self):
		return int(self.numOpsPerSec)

	def getShardKey(self):
		return self.shardKey

	def getReshardKey(self):
		return self.reshardKey

	def getChangeColumn(self):
		return self.changeColumn

	def getInitRecordsFilePath(self):
		return self.initRecordsFilePath

	def getExtraRecordsFilePath(self):
		return self.extraRecordsFilePath

	def getDistribution(self):
		return self.distribution

	def printConfig(self):
		print "Config details:"
		print "Router IP : " + self.getRouterIP()
		print "Router port : " + str(self.getRouterPort())
		print "DB Name : " + self.getDbName()
		print "Collection Name : " + self.getCollectionName()
		print "Range start : " + str(self.getRangeStart())
		print "Range end : " + str(self.getRangeEnd())
		print "Record preload start: " + str(self.getRecordPreloadStart())
		print "Num Ops/Sec : " + str(self.getNumOpsPerSec())
		print "Shard key : " + self.getShardKey()
		print "Reshard key : " + self.getReshardKey()
		print "Change column : " + self.getChangeColumn()
		print "Init records file path : " + self.getInitRecordsFilePath()
		print "Extra records file path : " + self.getExtraRecordsFilePath()
		print "Distribution : " + self.getDistribution()