# -*- coding: utf-8 -*-
"""
Created on Sat Sep  2 13:48:48 2017

@author: Thomas

CAP/M Class
"""
import numpy as np  # array operations
import pandas as pd  # time series management
import scipy.optimize as sco
import scipy.interpolate as sci

class CAPM(object) :
    
    def  __init__(self, data) :
        self.noa = len(data.columns)
        ## vectorized calculation of the log returns
        self.logRets = np.log(data/data.shift(1))
        # Bound the parameter values(weights) within 0 and 1
        self.bnds = tuple((0, 1) for x in range(self.noa))
        #  constraint: all parameters (weights) add up to 1
        self.cons = ({'type': 'eq', 'fun': lambda x:  np.sum(np.absolute(x)) - 1})
        self.EEsampleNum = 50       # number of sample points of EE Frontier
        ##   annualized return
      #  print('Annualized Return : \n{}'.format(logRets.mean() * 252))
      #  print('Annualized Covariance : \n{}'.format(logRets.cov() * 252))
        
    def random_walk(self) :
        
        ## random walk of portfolio
        prets = []        
        pvols = []
        for p in range(2500):
            weights = np.random.random(self.noa)
            weights /= np.sum(weights)
            prets.append(np.sum(self.logRets.mean() * weights) * 252)
            pvols.append(np.sqrt(np.dot(weights.T, np.dot(self.logRets.cov() * 252, weights))))
        prets = np.array(prets)
        pvols = np.array(pvols)
      #  print('Returns: \n{}'.format(prets))
      #  print('Volatility : \n{}'.format(pvols))
        return prets, pvols

    def statistics(self, weights):
        ''' Return portfolio statistics.
        
        Dependences
        ===========
              logRets : log returns of portfolio in array
        
        Parameters
        ==========
        weights : array-like
            weights for different securities in portfolio
        logRets : array-like
            log returns of the portfolio
        
        Returns
        =======
        pret : float
            expected portfolio return
        pvol : float
            expected portfolio volatility
        pret / pvol : float
            Sharpe ratio for rf=0
        '''
        weights = np.array(weights)
        # print('lRet={} weights={}'.format(logRets, weights))
        llcret = np.sum(self.logRets.mean() * weights) * 252
        llcvol = np.sqrt(np.dot(weights.T, np.dot(self.logRets.cov() * 252, weights)))
        return np.array([llcret, llcvol, llcret / llcvol])
    def min_func_sharpe(self, weights):
        return -self.statistics(weights)[2]
    def min_func_variance(self, weights):
        return self.statistics(weights)[1] ** 2
    def min_func_port(self, weights):
        return self.statistics(weights)[1]
    
    def Max_Sharpe(self) :
        # min of neg Sharpe is the maximization of the Sharpe
        print('  Max_Sharpe.noa = {}'.format(self.noa))
        opts = sco.minimize(self.min_func_sharpe, self.noa * [1. / self.noa,], method='SLSQP', 
                            bounds=self.bnds, constraints=self.cons)
        print('  max func sharpe=\n{}\n'.format(opts))
        pt = self.statistics(opts['x'])  
        return opts['success'], [pt[0], pt[1]], opts['x'] 
 
    def Min_Varian(self) :
        #  find min variance of portfolio
        optv = sco.minimize(self.min_func_variance, self.noa * [1. / self.noa,], method='SLSQP',
                               bounds=self.bnds, constraints=self.cons)
        print('  min func variance=\n{}\n'.format(optv))
        pt = self.statistics(optv['x'])  
        return optv['success'], [pt[0], pt[1]], optv['x'] 
 
       
    def EF_Frontier(self, retH, retL) : 
        #   Efficient Frontier 
        trets = np.linspace(retL, retH, self.EEsampleNum)
        tvols = []
        for tret in trets:
            cons = ({'type': 'eq', 'fun': lambda x:  self.statistics(x)[0] - tret},
                    {'type': 'eq', 'fun': lambda x:  np.sum(np.absolute(x)) - 1})
            res = sco.minimize(self.min_func_port, self.noa * [1. / self.noa,], method='SLSQP',
                               bounds=self.bnds, constraints=cons)
            tfun = round(res['fun'],8)
            ## print(' res FUN={}'.format(tfun))
            if len(tvols) > 0 and tvols[-1] == tfun :
                print('--> TVOLS dupl value='.format(tvols[-1]))
                break            
            tvols.append(tfun)
        tvols = np.array(tvols)
        if len(trets) > len(tvols) :
            trets = np.resize(trets, len(tvols))
        ##  print('  TVOLS = {}\n   TRETS={}'.format(tvols, trets))         # duplicate TVOLS cuase problem
    
        return trets, tvols

    def Market_Line(self, itrets, itvols) : 
        #  Capital Market Line - add riskless asset
        ind = np.argmin(itvols)
        evols = itvols[ind:]
        erets = itrets[ind:]
        
        try :
            tck = sci.splrep(evols, erets)
        except ValueError:
            print(' Error on input data\n   eVOLS={}\n eRETS={}'.format(evols, erets))
            return erets, evols, 0, 0, 0, False, [0,0], 0
        
        print('  TCK of EFF={}'.format(tck))
   
        def f(x):
            ''' Efficient frontier function (splines approximation). '''
            return sci.splev(x, tck, der=0)
        def df(x):
            ''' First derivative of efficient frontier function. '''
            return sci.splev(x, tck, der=1)
        def equations(p, rf=0.01):
            eq1 = rf - p[0]
            eq2 = rf + p[1] * p[2] - f(p[2])
            eq3 = p[1] - df(p[2])
            return eq1, eq2, eq3
        
        MKTopt = sco.fsolve(equations, [0.01, 0.5, 0.15])
        MKTpt = [MKTopt[2], f(MKTopt[2])]      
        print(' EFF Market Line EQ: {}    Point:{}'.format(MKTopt, MKTpt))
        para = np.round(equations(MKTopt), 6)
        pSum = np.sum(para)
        # 3 equations should be zero
        print(' Market_Line function: All 3 parameters should be zero: \n{} sum(parameters)={}'.format(para, pSum))
        
        mcons = ({'type': 'eq', 'fun': lambda x:  self.statistics(x)[0] - f(MKTopt[2])},
                {'type': 'eq', 'fun': lambda x:  np.sum(np.absolute(x)) - 1})
        res = sco.minimize(self.min_func_port, self.noa * [1. / self.noa,], method='SLSQP',
                bounds=self.bnds, constraints=mcons)
        pt = self.statistics(res['x'])
        
        print(' Market_Line function: res={}'.format(res))
        return erets, evols, MKTopt, MKTpt, pSum, res['success'], [pt[0],pt[1]], res['x']

