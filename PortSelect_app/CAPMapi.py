import datetime as dt
from dateutil.relativedelta import *
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os, os.path

"""
  Local  Class
"""
from PortSelect_app.CAPM import CAPM
from PortSelect_app.CAPMdata import Import_Data, FinDB, PortSelectTbl

def getHistPortWeighting(pName):
    curpath = os.path.abspath(".")
    print("Data folder is {}".format(curpath))
    mydb = FinDB(curpath)
    psTbl = PortSelectTbl(mydb)
    ddata = psTbl.Query(pName)
    ddata = ddata.loc[:, (ddata != 0).any(axis=0)]
    return ddata

def Rebalance(indata, drawChart, figfile, pName):

    aCapm = CAPM(indata)
    prets, pvols = aCapm.random_walk()
    Sflag, msrstat, msrpw = aCapm.Max_Sharpe()
    Mflag, mvstat, mvpw = aCapm.Min_Varian()
    SHRW = pd.DataFrame(index=indata.columns)
    SHRW['weight'] = msrpw.round(3)
    MVW = pd.DataFrame(index=indata.columns)
    MVW['weight'] = mvpw.round(3)

    if drawChart:
        ssdt = indata.index[0]
        eedt = indata.index[-1]
        retH = msrstat[0] * 1.1
        retL = mvstat[0] * 0.7
        print(" Calculate Frontier from return {} to {}".format(retL, retH))
        trets, tvols = aCapm.EF_Frontier(retH, retL)
        print(" Fronter lint has {} points".format(len(trets)))
        fig = plt.figure(figsize=(10, 6))
        print(" Plot random walk points.")
        plt.scatter(pvols, prets,
                        c=prets / pvols, marker='.')
                        # random portfolio composition
        print(" Plot EF frontier points.")
        plt.scatter(tvols, trets,
                        c=trets / tvols,
                        marker='X')
                        # efficient frontier
        plt.plot(msrstat[1], msrstat[0],
                    'r*', markersize=15.0, label="Max Sharpe Ratio")
                    # portfolio with highest Sharpe ratio
        plt.plot(mvstat[1], mvstat[0],
                    'y*', markersize=15.0, label="Min Variance")
                    # minimum variance portfolio
        plt.grid(True)
        plt.xlabel('expected volatility')
        plt.ylabel('expected return')
        plt.title(pName+': Min risk for given return level '+ssdt+' - '+eedt)
        plt.colorbar(label='Sharpe ratio')
        plt.legend()
        if len(figfile)>0:
            fig.savefig(figfile)

    return SHRW, MVW

def print_port_weighting(title, port, weights) :
    print('{} :'.format(title))
    for sym, w in zip(port, weights) :
        if (w > 0.0):
            print('{} : {}'.format(sym, w))

def getBestWeighting(portName, drFlag, drFile, refine):
    inname = portName
    print(" get best weighting of {}.".format(inname))
    endt = dt.date.today()
    stdt = endt - relativedelta(years=1)
    sdt = stdt.__str__()
    edt = endt.__str__()
    print(" Extract Data from {} to {}".format(sdt, edt ))

    bigdata = Import_Data(portName)
    print(" Import Data Info {}".format(bigdata.info()))
    flist = []
    if bigdata.shape[0] > 0 and bigdata.shape[1] > 0:
        d1 = bigdata[sdt:edt]
        ssdt = d1.index[0]
        eedt = d1.index[-1]
        print(" Actual Data from {} to {}.".format(ssdt, eedt))
        SRw, MVw = Rebalance(d1, False, "", inname)
        targetPortSize = 5
        tmp = SRw.sort_values(by=['weight'], ascending=False)
        if refine:
            finalPort = tmp.head(targetPortSize)
            print('  Porfolio first select by top {}\n {}'.format(targetPortSize, finalPort))
            d2 = pd.DataFrame()
            for col in finalPort.index:
                d2[col] = d1[col]
            # d2.info()
            ssdt = d2.index[0]
            eedt = d2.index[-1]
            SRw, MVw = Rebalance(d2, drFlag, drFile, inname)
            tmp = SRw.sort_values(by=['weight'], ascending=False)
        tmp = tmp[tmp > 0 ].dropna()
        flist = {}
        for index, row in tmp.iterrows():
            flist[index] = row['weight']
        print('  Final Selected Portfolio\n {}'.format(flist))
    return flist

def getBestWeightingDB(myDB, portName, drFlag, drFile, refine, tdate):
    inname = portName
    print(" get best weighting of {} on {}.".format(inname, tdate))
    if tdate != None:
        endt = tdate
    else:
        endt = dt.date.today()
    stdt = endt - relativedelta(years=2)
    sdt = stdt.__str__()
    edt = endt.__str__()
    print(" Extract Data from {} to {}".format(sdt, edt))

    bigdata = myDB.Import_Data(portName, sdt, edt)
    if bigdata.shape[0] > 0 and bigdata.shape[1] > 0:
        d1 = bigdata[sdt:edt].dropna(axis=1, how='any')
        ssdt = d1.index[0]
        eedt = d1.index[-1]
        print(" Actual Data from {} to {}.".format(ssdt, eedt))
        print(" Import Data Info {}".format(d1.info()))
        SRw, MVw = Rebalance(d1, False, "", inname)
        targetPortSize = 5
        tmp = SRw.sort_values(by=['weight'], ascending=False)
        if refine:
            finalPort = tmp.head(targetPortSize)
            print('  Final Porfolio selected by top {}\n {}'.format(targetPortSize, finalPort))
            d2 = pd.DataFrame()
            for col in finalPort.index:
                d2[col] = d1[col]
            # d2.info()
            ssdt = d2.index[0]
            eedt = d2.index[-1]
            SRw, MVw = Rebalance(d2, drFlag, drFile, inname)
            tmp = SRw.sort_values(by=['weight'], ascending=False)
        tmp = tmp[tmp > 0 ].dropna()
        print('  Final Porfolio selection\n {}'.format(tmp))
    return tmp

if __name__ == "__main__":
	getBestWeighting("^HSI", True, "")
