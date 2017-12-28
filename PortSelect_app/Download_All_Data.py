import pandas_datareader.data as web
import datetime as dt
import time
from dateutil.relativedelta import *
import os, os.path

# import local python
import CAPMdata as cd
from CAPMdata import FinDB

## set up global variables
DailyPlan = True
plist = cd.get_portName_List()

if DailyPlan:
    datlenyr = 2
else:
    datlenyr=20

print("Request to down portfolio list: {}".format(plist))
endt = dt.date.today()
stdt = endt - relativedelta(years=datlenyr)
sdt = stdt.__str__()
edt = endt.__str__()
print("Extract Data from {} to {}".format(stdt, endt))
curpath = os.path.abspath(__file__)
curdir = os.path.abspath(os.path.join(curpath, os.pardir))
pardir = os.path.abspath(os.path.join(curdir, os.pardir))
datapath = os.path.join(pardir, "Data")
print("Data folder is {}".format(datapath))

print("Create DataBase")
myfindb = FinDB(pardir)

for pn in plist:
    stklist = cd.get_Index_StockList(pn)
    for sym in stklist :
        print("-- Start download {} for {}".format(sym, pn))
        cfile = sym+".csv"
        fpath = os.path.join(datapath, cfile)
        if os.path.exists(fpath):
            print("-- {} data is existed. Skip..".format(sym))
        else:
            try :
                stk = web.DataReader(sym, 'yahoo', stdt, endt)
                stk = stk.dropna(axis=0, how='any')
                print("-- Saving {} to {}".format(sym, fpath))
                stk.to_csv(fpath)
                try:
                    myfindb.Add(sym, stk)
                except:
                    print("Save {} to DB Error.".format(sym))
                    pass
            except Exception as e:
                if hasattr(e, 'message'):
                    print("Exception occurred {}.".format(e.message))
                else:
                    print("There is unknown error when download {} web.".format(sym))
                pass
            time.sleep(2)
