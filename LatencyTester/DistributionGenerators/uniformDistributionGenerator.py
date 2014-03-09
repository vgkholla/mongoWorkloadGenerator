import random
from distribution import DistributionGenerator
class UniformDistributionGenerator(DistributionGenerator):

	def __init__(self, client, config, dbOpsHandler, recordPreloader, extraRecordGetter):
		super(UniformDistributionGenerator, self).__init__(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)

	def getNextIndex(self):
		config = self.getConfig()
		start = config.getRangeStart()
		end = config.getRangeEnd()

		return int(random.uniform(start, end))

