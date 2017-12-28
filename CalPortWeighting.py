import pandas_datareader.data as web
import datetime as dt
import time
from dateutil.relativedelta import *
import os, os.path

# import local python
import PortSelect_app.CAPMdata as cd
from PortSelect_app.CAPMdata import FinDB, PortSelectTbl, get_Index_StockList
from PortSelect_app.CAPMapi  import getBestWeightingDB

## set up global variables
DailyPlan = True
plist = cd.get_portName_List()

print("Request to select best stock weightings for : {}".format(plist))
endt = dt.date.today()
edt = endt.__str__()
print("Calculation Date {}".format(endt))
curpath = os.path.abspath(__file__)
curdir = os.path.abspath(os.path.join(curpath, os.pardir))
pardir = os.path.abspath(os.path.join(curdir, os.pardir))
datapath = os.path.join(pardir, "Data")
print("Data folder is {}".format(datapath))

print("Create DataBase")
mydb = FinDB(curdir)
print("Create Table")
psTbl = PortSelectTbl(mydb)

for pn in plist:
    stklist = get_Index_StockList(pn)
    hdate, pdate = psTbl.getUpdateDate(pn, stklist[0])
    print("Hdate={}, pdate={} for {}".format(hdate, pdate, pn))
    if hdate > pdate:
        rlist = getBestWeightingDB(mydb, pn, True, 'EEF.png', False, hdate)
        psTbl.Add(pn, hdate, rlist)
