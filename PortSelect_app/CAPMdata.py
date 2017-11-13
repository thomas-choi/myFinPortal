import sys
import datetime as dt
from dateutil.relativedelta import *
import time
import numpy as np  # array operations
import pandas as pd

"""
Global variables
"""
DataPath = "Data"  		# data forlder from working directory

testdata = ['0027.HK','0700.HK','0005.HK','0939.HK','1299.HK','0941.HK','1398.HK','3988.HK']
HSI = ['0027.HK','0700.HK','0005.HK','0939.HK','1299.HK','0941.HK','1398.HK','3988.HK','0001.HK','2318.HK','0388.HK',
      '0386.HK','0002.HK','0883.HK','0016.HK','0823.HK','2388.HK','0003.HK','0011.HK','0857.HK','2628.HK',
      '0006.HK','0688.HK','1928.HK','0004.HK','0267.HK','0175.HK','0762.HK','0066.HK','1088.HK','1109.HK','2018.HK',
      '0012.HK','0017.HK','3328.HK','0023.HK','1038.HK','2319.HK','0083.HK','0101.HK','1044.HK','0019.HK',
      '0992.HK','0151.HK','0836.HK','0144.HK','0135.HK','0293.HK','3968.HK','1113.HK','0322.HK']

QQQ = ['AAPL', 'MSFT', 'AMZN', 'FB', 'GOOG', 'GOOGL', 'INTC', 'CMCSA', 'CSCO', 'AMGN', 'NVDA', 'AVGO',
		'KHC', 'TXN', 'GILD', 'QCOM', 'ADBE', 'PYPL', 'CHTR', 'NFLX', 'PCLN', 'SBUX', 'CELG', 'WBA',
        'COST','BIIB','BIDU','MDLZ','AMAT','TSLA','MU','ADP','ATV','TMUS','CSX','MAR','CTSH','SRG',
        'REGN','INTU','EBAY','VRTX','JD','EA','MNST']

def my_Read_csv(stocklist) :
	global  DataPath
	#  read csv from yahoo
	data = pd.DataFrame()
	for sym in stocklist:
		fpath = DataPath+"/"+sym+".csv"
		print("read file {}".format(fpath))
		data[sym] = pd.read_csv(fpath, index_col=0)['Adj Close']
		if data[sym].dtype !=  float :
			data = data[data[sym] != 'null']
	data = data.astype(float)
	return data

def Import_Data(portName):
	global  testdata
	global  HSI
	global  QQQ

	if portName == "test":
		gdata = my_Read_csv(testdata)
	elif portName == "HSI":
		gdata = my_Read_csv(HSI)
	elif portName == "QQQ":
		gdata = my_Read_csv(QQQ)
	else:
		gdata = pd.DataFrame()
	return gdata
