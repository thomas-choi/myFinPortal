import pandas as pd
import os, os.path
import sqlite3

"""
Global variables
"""
DataPath = "Data"  		# data forlder from working directory

testdata = ['0027.HK','0700.HK']

HSI = ['0027.HK','0700.HK','0005.HK','0939.HK','1299.HK','0941.HK','1398.HK','3988.HK','0001.HK',
      '2318.HK','0388.HK','0386.HK','0002.HK','0883.HK','0016.HK','0823.HK','2388.HK','0003.HK',
      '0011.HK','0857.HK','2628.HK','0006.HK','0688.HK','1928.HK','0004.HK','0267.HK','0175.HK',
      '0762.HK','0066.HK','1088.HK','1109.HK','2018.HK','0012.HK','0017.HK','3328.HK','0023.HK',
      '1038.HK','2319.HK','0083.HK','0101.HK','1044.HK','0019.HK','0992.HK','0151.HK','0836.HK',
      '0144.HK','0135.HK','0293.HK','3968.HK','1113.HK','0322.HK','0288.HK','0267.HK','0101.HK']

QQQ = ['AAPL', 'MSFT', 'AMZN', 'FB', 'GOOG', 'GOOGL', 'INTC', 'CMCSA', 'CSCO', 'AMGN', 'NVDA', 'AVGO',
		'KHC', 'TXN', 'GILD', 'QCOM', 'ADBE', 'PYPL', 'CHTR', 'NFLX', 'PCLN', 'SBUX', 'CELG', 'WBA',
        'COST','BIIB','BIDU','MDLZ','AMAT','TSLA','MU','ADP','ATV','TMUS','CSX','MAR','CTSH','SRG',
        'REGN','INTU','EBAY','VRTX','JD','EA','MNST']

SPY=['AAPL', 'MSFT','AMZN','FB','JNJ','BRK.B','JPM','XOM','GOOGL','GOOG','BAC',
        'WFC','PG','CVX','INTC','T','PFE','V','HD','C','VZ','CSCO','KO','CMCSA',
        'DWDP','PEP','DIS','PM','GE','MRK','ABBV','ORCL','BA','WMT','MA','MMM',
        'MCD','IBM','NVDA','MO','AMGN','HON','AVGO','MDT','BMY','QCOM','TXN',
        'GILD','UNP','ACN','ADBE','PYPL','UTX','GS','SLB','PCLN','NFLX','SBUX',
        'USB','LLY','CELG','CAT','UPS','LMT','TMO','NKE','COST','NEE','CRM','CVS',
        'CB','MS','AXP','CHTR','TWX','LOW','BIIB','CL','MDLZ','PNC','AMT','DUK',
        'WBA','AMAT','COP','BLK','AGN','EOG','ANTM','AET','GM','DHR','GD','MET',
        'AIG','BK','RTN','FDX']

ETF=['EEM','EFA','EWZ','GLD','HYG','IWM','SPY','DIA','IYR','QQQ','TLT','XLE',
        'XLV','XOP','USO','XRT','XME','XLK']

IndexList = {
    ('^HSI', 'Hang Seng Index(HSI)'),
    ('QQQ', 'Nasdaq 100 Index(QQQ)'),
    ('SPY', 'S&P 500 Index(SPY)'),
    ('ETF', 'US ETFs'),
    ('test', 'Testing Stock List(test)'),
}

# DataBase variables
#
database = "FINPORTAL.sqlite3"

addRecSQL = "INSERT INTO HISTORICAL (Symbol, Date, Open, High, Low, Close, AdjClose, Volume) VALUES ( \'{}\', \'{}\', {},{}, {}, {}, {}, {})"

createTableSQL= """CREATE TABLE if not exists HISTORICAL (
Symbol text NOT NULL,
Date text NOT NULL,
Open real, High real, Low real, Close real, AdjClose real, Volume integer,
PRIMARY KEY(Symbol, Date))"""

queryAllSQL = """Select Date, Open, High, Low, Close, AdjClose, Volume from HISTORICAL
where Symbol = \'{}\' order by Date"""

querybyDateSQL = """Select Date, Open, High, Low, Close, AdjClose, Volume from HISTORICAL
where Symbol = \'{}\' and Date >= \'{}\' and Date <= \'{}\' order by Date"""

cols = ['Date','Open','High','Low','Close','AdjClose','Volumn']

class FinDB :

    def __init__(self, inpath):
        dbpath = os.path.join(inpath, database)
        print(" Create DB-{}".format(dbpath))
        self.conn = sqlite3.connect(dbpath)
        self.curs = self.conn.cursor()
        self.curs.execute(createTableSQL)
        self.conn.commit()
        self.insertlimit = 3000

    def __del__(self):
        self.conn.close()

    def Add(self, sym, stk):
        count=0
        for i,r in stk.iterrows():
            # print(addRecSQL.format(sym, i, r['Open'], r['High'], r['Low'],r['Close'],r['Adj Close'],r['Volume']))
            self.curs.execute(addRecSQL.format(sym, i, r['Open'], r['High'], r['Low'],r['Close'],r['Adj Close'],r['Volume']))
            count += 1
            if count > self.insertlimit:
                print("** commit() after {} insert".format(count))
                self.conn.commit()
                count = 0
        self.conn.commit()

    def Query(self, sym, start, end):
        data = pd.DataFrame(columns=cols)
        if len(start)>0 and len(end)>0:
            self.curs.execute(querybyDateSQL.format(sym, start, end))
        else:
            self.curs.execute(queryAllSQL.format(sym))
        rows = self.curs.fetchall()
        for r in rows:
            data.loc[len(data)] = list(r)
        data = data.set_index('Date')
        return data

    def load_list(self, stocklist) :
        data = pd.DataFrame()
        for sym in stocklist:
            data[sym] = Query(sym, "", "")['AdjClose']
            if data[sym].dtype !=  float :
                data = data[data[sym] != 'null']
        data = data.astype(float)
        return data

    def Import_Data(self, portName):
        global  testdata
        global  HSI
        global  QQQ
        global  SPY

        if portName == "test":
            gdata = load_list(testdata)
        elif portName == "^HSI":
            gdata = load_list(HSI)
        elif portName == "QQQ":
            gdata = load_list(QQQ)
        elif portName == "SPY":
            gdata = load_list(SPY)
        elif portName == "ETF":
            gdata = load_list(ETF)
        else:
            gdata = pd.DataFrame()
        return gdata

def get_portName_List():
    pNList = []
    for idx, desc in IndexList:
        pNList.append(idx)
    return pNList

def get_portName_ManualList():
    return IndexList

def get_Index_StockList(portName) :
    if portName == "test":
        gdata = testdata
    elif portName == "^HSI":
        gdata = HSI
    elif portName == "QQQ":
        gdata = QQQ
    elif portName == "SPY":
        gdata = SPY
    elif portName == "ETF":
        gdata = ETF
    else:
        gdata = []
    return gdata

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
    global  SPY

    if portName == "test":
        gdata = my_Read_csv(testdata)
    elif portName == "^HSI":
        gdata = my_Read_csv(HSI)
    elif portName == "QQQ":
        gdata = my_Read_csv(QQQ)
    elif portName == "SPY":
        gdata = my_Read_csv(SPY)
    else:
        gdata = pd.DataFrame()
    return gdata
