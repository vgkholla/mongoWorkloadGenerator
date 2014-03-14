import random
from distributionGenerator import DistributionGenerator
class UniformDistributionGenerator(DistributionGenerator):

	def __init__(self, client, config, dbOpsHandler, recordPreloader, extraRecordGetter):
		super(UniformDistributionGenerator, self).__init__(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)

	def getNextIndex(self):
		config = self.getConfig()
		start = 0
		end = self.getNumKeysInMapping() - 1

		return int(random.uniform(start, end))

