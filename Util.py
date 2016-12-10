#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys,os.path,time,datetime,pytz
from ib.ext.Contract import Contract

bjTZ=pytz.timezone('Asia/Shanghai')
estTZ=pytz.timezone('US/Eastern')

class Util:
  @staticmethod
  def makeContract(contractSymbol='',secType='',exchange='',currency='USD',expiry='',strike='0.0',right=''):
    newContract = Contract()
    newContract.m_symbol = contractSymbol
    newContract.m_secType = secType
    newContract.m_exchange = exchange
    newContract.m_currency = currency
    newContract.m_expiry = expiry
    newContract.m_strike = strike
    newContract.m_right = right
    print 'Contract Parameters: [|symbol:%s,|secType:%s,|exchange:%s,|currency:%s,|expiry:%s,|strike:%s,|right:%s]' % (contractSymbol,secType,exchange,currency,expiry,strike,right)
    return newContract
  
  @staticmethod
  def coverToEstDatetime(beijingDateTime):
    beijingTZ = pytz.timezone('Asia/Shanghai')
    beijingDateTime = beijingTZ.localize(beijingDateTime) #不做这一步，会有一个6分钟的差距
    estTZ = pytz.timezone('US/Eastern')
    estDatetime=beijingDateTime.astimezone(estTZ)
    return estDatetime
  
  @staticmethod
  def coverDatetimeStrToTimestamp(dateStr,timeStr=' 19:00:00',localTZ=bjTZ,covertTZ=estTZ):
    localDatetime=datetime.datetime.strptime(dateStr+ timeStr,'%Y%m%d  %H:%M:%S')
    localDatetime = localTZ.localize(localDatetime)
    covertDatetime = localDatetime.astimezone(covertTZ)           
    covertTimestamp=time.mktime(covertDatetime.timetuple())
  
  
  @staticmethod
  def watchAll(msg):
    print str(msg)  