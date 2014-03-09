import os,sys
import time
sys.path.append(os.path.abspath('../DistributionGenerators'))
sys.path.append(os.path.abspath('../IOHandlers'))
sys.path.append(os.path.abspath('../Configs'))
sys.path.append(os.path.abspath('../DBOpsHandlers'))

from pymongo import MongoClient

from configGetter import ConfigGetter
from dbOpsHandler import DBOpsHandler
from recordGetter import RecordGetter
from uniformDistributionGenerator import UniformDistributionGenerator

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
extraRecordGetter =  RecordGetter(config)

distribution = config.getDistribution()
workloadGenerator = None
if distribution == "uniform":
	workloadGenerator = UniformDistributionGenerator(client, config, dbOpsHandler, extraRecordGetter)
else:
	print "Unrecognized distribution - " + distribution
	exit()

workloadGenerator.applyWorkload()