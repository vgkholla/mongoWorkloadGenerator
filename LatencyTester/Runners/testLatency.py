import os,sys
import time
sys.path.append(os.path.abspath('../DistributionGenerators'))
sys.path.append(os.path.abspath('../IOHandlers'))
sys.path.append(os.path.abspath('../Configs'))
sys.path.append(os.path.abspath('../DBOpsHandlers'))

from pymongo import MongoClient

from configGetter import ConfigGetter
from dbOpsHandler import DBOpsHandler
from recordPreloader import RecordPreloader
from recordGetter import RecordGetter
from uniformDistributionGenerator import UniformDistributionGenerator
from latestDistributionGenerator import LatestDistributionGenerator
from zipfDistributionGenerator import ZipfDistributionGenerator

def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <config_file>"
		exit()

	configFile = getConfigFile(args)
	return configFile

def getConfigFilePath(configFile):
	return os.path.abspath("../Configs/" + configFile) 

def getConfig(configFile):
	configFilePath = getConfigFilePath(configFile)
	return ConfigGetter(configFilePath)

def getMongoClient(config):
	return MongoClient(config.getRouterIP(), config.getRouterPort())

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

config.printConfig()

client = getMongoClient(config)
dbOpsHandler = DBOpsHandler(client, config)
recordPreloader = RecordPreloader(config)
extraRecordGetter =  RecordGetter(config)

distribution = config.getDistribution()
workloadGenerator = None
if distribution == "uniform":
	workloadGenerator = UniformDistributionGenerator(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)
elif distribution == "latest":
	workloadGenerator = LatestDistributionGenerator(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)
elif distribution == "zipf":
	workloadGenerator = ZipfDistributionGenerator(client, config, dbOpsHandler, recordPreloader, extraRecordGetter)
else:
	print "Unrecognized distribution - " + distribution
	exit()

workloadGenerator.applyWorkload()