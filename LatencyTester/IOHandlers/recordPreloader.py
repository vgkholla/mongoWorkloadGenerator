import os
class RecordPreloader:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def initMapping(self):
		config = self.getConfig()
		initRecordsFile = config.getInitRecordsFilePath()
		insertedRecords = dict()
		recordNum = 0
		with open(initRecordsFile) as f:
			for line in f:
				fields = line.split(",")
				record = {config.getShardKey() : fields[0].replace("\n", ""), config.getReshardKey() : fields[1].replace("\n", "")}
				insertedRecords[recordNum] = record
				recordNum += 1

		return insertedRecords