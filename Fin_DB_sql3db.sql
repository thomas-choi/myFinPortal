select Date from HISTORICAL
where symbol = 'AAPL'
and Date not in ( select distinct Date from HistPortSelect
where PortName = 'SPY' )
order by Date desc

select * from HistPortSelect
where PortName = '^HSI'

delete from HistPortSelect
commit

select * from HISTORICAL
where Date = '2017-12-28 00:00:00'

select symbol, min(Date) from HISTORICAL
where symbol like '%.HK'

where symbol in ('EEM','EFA','EWZ','GLD')

select symbol, min(Date) from HISTORICAL
group by symbol
order by min(Date) desc

select symbol, max(Date) from HISTORICAL
group by symbol
order by max(Date) desc

select distinct portname,Max(Date), min(Date) from HistPortSelect
group by portname

select HT.symbol, max(HT.Date), PortName, max(HP.Date) from HISTORICAL as HT, HistPortSelect as HP
where HT.symbol = '0027.HK' and HP.PortName = '^HSI'

select HT.symbol, max(HT.Date), PortName, max(HP.Date) from HISTORICAL as HT, HistPortSelect as HP
where HT.symbol = 'AAPL' and HP.PortName = 'SPY'

select HT.symbol, max(HT.Date), PortName, max(HP.Date) from HISTORICAL as HT, HistPortSelect as HP
where HT.symbol = 'AAPL' and HP.PortName = 'QQQ'