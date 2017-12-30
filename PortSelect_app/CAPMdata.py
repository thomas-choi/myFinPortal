import pandas as pd
from dateutil import parser
import os, os.path
import sqlite3

"""
Global variables
"""
DataPath = "Data"  		# data forlder from working directory

testdata = ['^HSI','QQQ', 'SPY']

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

SPY=['AAPL', 'MSFT','AMZN','FB','JNJ','JPM','XOM','GOOGL','GOOG','BAC',
        'WFC','PG','CVX','INTC','T','PFE','V','HD','C','VZ','CSCO','KO','CMCSA',
        'DWDP','PEP','DIS','PM','GE','MRK','ABBV','ORCL','BA','WMT','MA','MMM',
        'MCD','IBM','NVDA','MO','AMGN','HON','AVGO','MDT','BMY','QCOM','TXN',
        'UNP','ACN','ADBE','PYPL','UTX','GS','SLB','PCLN','NFLX','SBUX',
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

createPStableSQL="""CREATE TABLE if not exists "HistPortSelect" (
`PortName` TEXT NOT NULL,
`Date` TEXT NOT NULL,
`Symbol` TEXT NOT NULL DEFAULT '',
`Weight` REAL NOT NULL DEFAULT 0.0,
PRIMARY KEY(`PortName`,`Date`) )"""

queryAllSQL = """Select Date, Open, High, Low, Close, AdjClose, Volume from HISTORICAL
where Symbol = \'{}\' order by Date"""

queryColsAllSQL = """Select {} from HISTORICAL
where Symbol = \'{}\' order by Date"""

querybyDateSQL = """Select Date, Open, High, Low, Close, AdjClose, Volume from HISTORICAL
where Symbol = \'{}\' and Date >= \'{}\' and Date <= \'{}\' order by Date"""

queryColsbyDateSQL = """Select {} from HISTORICAL
where Symbol = \'{}\' and Date >= \'{}\' and Date <= \'{}\' order by Date"""

queryLastRecordDateSQL = """Select Symbol, max(Date) from HISTORICAL group by Symbol"""

DBcolumns = ['Date','Open','High','Low','Close','AdjClose','Volumn']

class FinDB :

    def __init__(self, inpath):
        dbpath = os.path.join(inpath, database)
        print(" Create DB-{}".format(dbpath))
        self.conn = sqlite3.connect(dbpath)
        self.curs = self.conn.cursor()
        self.curs.execute(createTableSQL)
        self.conn.commit()
        self.insertlimit = 3000
        self.LastDate = pd.DataFrame(columns=['Symbol', 'LastDate'])
        self.curs.execute(queryLastRecordDateSQL)
        rows = self.curs.fetchall()
        for r in rows:
            # print(" r={}".format(r))
            self.LastDate.loc[len(self.LastDate)]= list(r)
        self.LastDate = self.LastDate.set_index('Symbol')
        print('LastDate DF is \n{}'.format(self.LastDate))

    def __del__(self):
        self.conn.close()

    def Add(self, sym, stk):
        count=0
        if (self.LastDate.index == sym).any():
            ldate = parser.parse(self.LastDate.loc[sym].LastDate)
        else:
            ldate = parser.parse('1000-01-01 00:00:00')
        print("{} has record up to {}".format(sym, ldate))
        for i,r in stk.iterrows():
            # print(" Check {}and{} on type {} {}".format(i,ldate, type(i), type(ldate)))
            if i > ldate:
                # print(addRecSQL.format(sym, i, r['Open'], r['High'], r['Low'],r['Close'],r['Adj Close'],r['Volume']))
                self.curs.execute(addRecSQL.format(sym, i, r['Open'], r['High'], r['Low'],r['Close'],r['Adj Close'],r['Volume']))
                count += 1
                if count > self.insertlimit:
                    print("** commit() after {} insert".format(count))
                    self.conn.commit()
                    count = 0
        if count > 0:
            print("** commit() after {} insert".format(count))
            self.conn.commit()

    def Query(self, sym, start, end):
        data = pd.DataFrame(columns=DBcolumns)
        if len(start)>0 and len(end)>0:
            self.curs.execute(querybyDateSQL.format(sym, start, end))
        else:
            self.curs.execute(queryAllSQL.format(sym))
        rows = self.curs.fetchall()
        for r in rows:
            data.loc[len(data)] = list(r)
        data = data.set_index('Date')
        return data

    def QuerybyCols(self, sym, start, end, clist):
        if len(clist) <= 0:
            clist = DBcolumns
        data = pd.DataFrame(columns=clist)
        colstr = clist[0]
        for i in range(1, len(clist)):
            colstr = colstr + ',' + clist[i]
        if len(start)>0 and len(end)>0:
            qstring = queryColsbyDateSQL.format(colstr, sym, start, end)
        else:
            qstring = queryColsAllSQL.format(colstr, sym)
        self.curs.execute(qstring)
        rows = self.curs.fetchall()
        for r in rows:
            data.loc[len(data)] = list(r)
        data = data.set_index('Date')
        return data

    def load_list(self, stocklist, start, end) :
        data = pd.DataFrame()
        for sym in stocklist:
            data[sym] = self.QuerybyCols(sym, start, end, ['Date','Close'])['Close']
            if data[sym].dtype !=  float :
                data = data[data[sym] != 'null']
        data = data.astype(float)
        return data

    def Import_Data(self, portName, start, end):
        global  testdata
        global  HSI
        global  QQQ
        global  SPY

        if portName == "test":
            gdata = self.load_list(testdata, start, end)
        elif portName == "^HSI":
            gdata = self.load_list(HSI, start, end)
        elif portName == "QQQ":
            gdata = self.load_list(QQQ, start, end)
        elif portName == "SPY":
            gdata = self.load_list(SPY, start, end)
        elif portName == "ETF":
            gdata = self.load_list(ETF, start, end)
        else:
            gdata = pd.DataFrame()
        return gdata

class DBTable:
    def __init__(self, db):
        self.mydb = db
        self.cmtlimit = 3000
        self.Icnt = 0
        self.Columns = []
        self.CreateTableSQL = """ """
        self.AddSQL = """ """
        self.QuerySQL = """ """

    def AddRec(self, queryString):
        self.mydb.curs.execute(queryString)
        self.Icnt += 1
        if self.Icnt > self.cmtlimit:
            print("** commit() after {} insert".format(self.Icnt))
            self.mydb.conn.commit()
            self.Icnt = 0
    def EndCommit(self):
        if self.Icnt > 0:
            print("** commit() after {} insert".format(self.Icnt))
            self.mydb.conn.commit()
            self.Icnt = 0

class PortSelectTbl(DBTable):
    def __init__(self, db):
        DBTable.__init__(self, db)
        self.Columns = ['PortName','Date','Symbol','Weight']
        self.CreateTableSQL = """CREATE TABLE if not exists "HistPortSelect" (`PortName` TEXT NOT NULL,`Date` TEXT NOT NULL,`Symbol` TEXT NOT NULL DEFAULT '',`Weight` REAL NOT NULL DEFAULT 0.0 )"""
        self.AddSQL = "INSERT INTO HistPortSelect (PortName,Date,Symbol,Weight) VALUES ( \'{}\', \'{}\', \'{}\',{})"
        self.checkUpdatesql1 = "select symbol, max(Date),min(Date) from HISTORICAL where symbol = \'{}\'"
        self.checkUpdatesql2 = "select PortName, max(Date) from HistPortSelect where PortName = \'{}\'"
        self.getMissingDatesql= "select Date from HISTORICAL where symbol = \'{}\' and Date not in ( select distinct Date from HistPortSelect where PortName = \'{}\' ) order by Date desc"
        self.mydb.curs.execute(self.CreateTableSQL)
        self.mydb.conn.commit()

    def Add(self, PName, Pdate, wgtlist):
        qstring = ''
        for i, r in wgtlist.iterrows():
            qstring = self.AddSQL.format(PName, Pdate, i, r.weight)
            print(qstring)
            DBTable.AddRec(self, qstring)
        DBTable.EndCommit(self)

    def getUpdateDate(self, PName, symbol):
        hdate = None
        pdate = None
        qstring = self.checkUpdatesql1.format(symbol)
        self.mydb.curs.execute(qstring)
        rows = self.mydb.curs.fetchall()
        for r in rows:
            if r[1] != None:
                hdate = parser.parse(r[1])
            if r[2] != None:
                pdate = parser.parse(r[2])
        qstring = self.checkUpdatesql2.format(PName)
        self.mydb.curs.execute(qstring)
        rows = self.mydb.curs.fetchall()
        for r in rows:
            if r[1] != None:
                pdate = parser.parse(r[1])
        return hdate, pdate

    def getMissingDate(self, PName, symbol):
        Dlist = []
        qstring = self.getMissingDatesql.format(symbol, PName)
        print(qstring)
        self.mydb.curs.execute(qstring)
        rows = self.mydb.curs.fetchall()
        for r in rows:
            if r != None:
                dd = parser.parse(r[0])
                Dlist.append(dd)
        return Dlist

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
