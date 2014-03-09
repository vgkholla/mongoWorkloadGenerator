import os
class RecordGetter:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def getLine(self, lineNum):
		filePath = self.getConfig().getExtraRecordsFilePath()

		fp = open(filePath)
		for i, line in enumerate(fp):
		    if i == lineNum:
		   		return line
		fp.close() 

	def getRecordAtLine(self, lineNum):
		config = self.getConfig()
		line = self.getLine(lineNum)
		fields = line.split(",")

		record = {config.getShardKey() : fields[0], config.getReshardKey() : fields[3], config.getChangeColumn() : fields[2]}

		return record