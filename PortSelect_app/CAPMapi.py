import sys
import datetime as dt
from dateutil.relativedelta import *
import time
import numpy as np  # array operations
import scipy.stats as scs
import scipy.optimize as sco
import scipy.interpolate as sci
import pandas as pd
import calendar
import concurrent.futures
import matplotlib as mpl
import matplotlib.pyplot as plt  # standard plotting library
from pylab import plt
plt.style.use('ggplot')
## %matplotlib inline
# put all plots in the notebook itself
"""
  Local  Class
"""
from PortSelect_app.CAPM import CAPM
from PortSelect_app.CAPMdata import Import_Data


def Rebalance(indata, drawChart):

    aCapm = CAPM(indata)
    prets, pvols = aCapm.random_walk()
    Sflag, msrstat, msrpw = aCapm.Max_Sharpe()
    Mflag, mvstat, mvpw = aCapm.Min_Varian()
    SHRW = pd.DataFrame(index=indata.columns)
    SHRW['weight'] = msrpw.round(3)
    MVW = pd.DataFrame(index=indata.columns)
    MVW['weight'] = mvpw.round(3)

    if drawChart:
        retH = msrstat[0] * 1.1
        retL = mvstat[0] * 0.7
        print(" Calculate Frontier from return {} to {}".format(retL, retH))
        trets, tvols = aCapm.EF_Frontier(retH, retL)
        print(" Fronter lint has {} points".format(len(trets)))
        plt.figure(figsize=(10, 6))
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
                    'r*', markersize=15.0)
                    # portfolio with highest Sharpe ratio
        plt.plot(mvstat[1], mvstat[0],
                    'y*', markersize=15.0)
                    # minimum variance portfolio
        plt.grid(True)
        plt.xlabel('expected volatility')
        plt.ylabel('expected return')
        plt.title('Minimum risk portfolios for given return level (crosses) '+sdt+' - '+edt)
        plt.colorbar(label='Sharpe ratio')

    return SHRW, MVW

def print_port_weighting(title, port, weights) :
    print('{} :'.format(title))
    for sym, w in zip(port, weights) :
        if (w > 0.0):
            print('{} : {}'.format(sym, w))

def getBestWeighting(portName):
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
        drawchart = False
        SRw, MVw = Rebalance(d1, drawchart)
        targetPortSize = 5
        tmp = SRw.sort_values(by=['weight'], ascending=False)
        finalPort = tmp.head(targetPortSize)
        print('  Final Porfolio select by top {}\n {}'.format(targetPortSize, finalPort))
        d2 = pd.DataFrame()
        for col in finalPort.index:
            d2[col] = d1[col]
        # d2.info()
        ssdt = d2.index[0]
        eedt = d2.index[-1]
        SRw, MVw = Rebalance(d2, drawchart)
        print("\n***  Final Portfolio Weighting by Sharpe Ratio: ***\n {}".format(SRw))
        
        for index, row in SRw.iterrows():
            flist.append("{} is {}".format(index, row['weight']))
        print('  Final Porfolio select by top {}\n {}'.format(targetPortSize, finalPort))
    return flist

if __name__ == "__main__":
	getBestWeighting("test")
