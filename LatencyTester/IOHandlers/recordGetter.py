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
		if line == None:
			return None

		fields = line.split(",")

		shardField = fields[0].replace("\n", "").replace("\"", "")
		reshardField = fields[3].replace("\n", "").replace("\"", "")

		if shardField.isdigit():
			shardField = int(shardField)

		if reshardField.isdigit():
			reshardField = int(reshardField)

		record = {config.getShardKey() : shardField, config.getReshardKey() : reshardField, config.getChangeColumn() : fields[2].replace("\n", "").replace("\"", "")}

		return record
