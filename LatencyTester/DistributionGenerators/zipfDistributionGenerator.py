import numpy
from distributionGenerator import DistributionGenerator
class ZipfDistributionGenerator(DistributionGenerator):

	def __init__(self, client, config, dbOpsHandler, recordPreloader, extraRecordGetter):
		self.currentIndex = 0
		self.values = None
		super(ZipfDistributionGenerator, self).__init__(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)

	def getCurrentIndex(self):
		return self.currentIndex

	def getValues(self):
		return self.values

	def incrementCurrentIndex(self, increment = 1):
		self.currentIndex += increment

	def setValues(self, values):
		self.values = values

	def getNextIndex(self):
		currentIndex = self.getCurrentIndex()

		if currentIndex == 0:
			lower = 0
			shape = 2   # the distribution shape parameter, also known as `a` or `alpha`
			size = 1000000
			upper = self.getConfig().getRangeEnd()

			x = numpy.random.zipf(shape, size) + lower
			x = x[x<upper]

			self.setValues(x)
		
		numKeysInMapping = self.getNumKeysInMapping()
		values = self.getValues()
		
		value = int(values[currentIndex])
		value = numKeysInMapping - value
		
		self.incrementCurrentIndex()
		return value


