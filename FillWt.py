import datetime as dt
import time
from dateutil.relativedelta import *
import os, os.path

# import local python
import PortSelect_app.CAPMdata as cd
from PortSelect_app.CAPMdata import FinDB, PortSelectTbl, get_Index_StockList
from PortSelect_app.CAPMapi  import getBestWeightingDB

## set up global variables
FillDaysCount = 3
plist = cd.get_portName_List()

print("Request to select best stock weightings for : {}".format(plist))
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
    dlist = psTbl.getMissingDate(pn, stklist[0])
#    print("Hdate={}, pdate={} for {}".format(hdate, pdate, pn))
    for i in range(0, FillDaysCount):
        rlist = getBestWeightingDB(mydb, pn, True, 'EEF.png', False, dlist[i])
        psTbl.Add(pn, dlist[i], rlist)
