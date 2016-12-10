#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2016年11月25日

@author: liuwei
'''

from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
import sys,os.path, time,datetime,pytz
from collections import OrderedDict
import pandas as pd
from Util import Util
import redis
from multiprocessing import Queue





class HandleHisData:
  
  def __init__(self,conn):
    self.m_conn=conn
    self.tick_contract_Map={}
    self.taskQueue=Queue()
    
    self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379)  
    self.redis = redis.Redis(connection_pool=self.pool)
    
    self.write2file = False
    self.newRowData = []
    self.finished = False # true when historical data is done
    
    
    
    self.m_endESTDateStr = ''
    self.m_endESTtimeStr = ''
    self.m_barLength = ''
    self.m_duration=''
    
    #self.logFile=open('./logfile.txt', 'w+')
  
  def setContract(self,tickId,contract,endDateStr,duration='2 D',barLength='1 day',whatToShow='TRADES',useRTH=0,formatDate=1):
    #endDateStr= dateStr + ' 23:59:59 EST'
    if self.tick_contract_Map.has_key(tickId):
        print "the tiackId had in tick_contract_Map"
        return false   
    self.tick_contract_Map[tickId] = {}
    self.tick_contract_Map[tickId]['contract']=contract
    self.tick_contract_Map[tickId]['duration']=duration
    self.tick_contract_Map[tickId]['barLength']=barLength
    self.tick_contract_Map[tickId]['endDateStr']=endDateStr
    self.tick_contract_Map[tickId]['whatToShow']=whatToShow
    self.tick_contract_Map[tickId]['useRTH']=useRTH
    self.tick_contract_Map[tickId]['formatDate']=formatDate
  
  def reviceHisData(self,msg,msgTZ='US/Eastern'):
    #print >> self.logFile, msg
    if int(msg.high) > 0:
      estTZ=pytz.timezone('US/Eastern')
      estDatetime=''
      if  len(msg.date) == 8:
        estDatetime=datetime.datetime.strptime(msg.date,'%Y%m%d')
      else:
        estDatetime=datetime.datetime.strptime(msg.date,'%Y%m%d %H:%M:%S')
      estDatetime=estTZ.localize(estDatetime)
      NYtimeStr=estDatetime.strftime('%Y%m%d %H:%M:%S')
      dataStr =  '%s,%s,%s,%s,%s,%s' % (NYtimeStr,msg.open,msg.high,msg.low,msg.close,msg.volume)      
      if self.tick_contract_Map.has_key(msg.reqId):        
        self.tick_contract_Map[msg.reqId]['reviceData'].append(dataStr+'\n')   
    elif int(msg.high) == -1:
      if self.tick_contract_Map.has_key(msg.reqId):
        reqDic = self.tick_contract_Map[msg.reqId]
        reqDic['finished'] = True
        reqDic['write2file'] = True
        if len(reqDic['reviceData'])  > 0 and reqDic['write2file']:
          self.appendToFile(msg.reqId,'HistData',reqDic['contract'].m_symbol,reqDic['contract'].m_secType,reqDic['endDateStr'].split(' ')[0],reqDic['endDateStr'],reqDic['barLength'])
      
    
    
    
    #prices=[rec.split(",")[5] for rec in Data]
    #return [SMA200(prices),SMA50(prices)]
      
  
  def cleanMerge(self,seq1,seq2):
    seen = set(seq1)
    if seq1[0].split(',')[3:6]==[0,0,0]:
        seen={x[0:10] for x in seen}
        seq1.extend([ x for x in seq2 if x[0:10] not in seen])
    else:
        seq1.extend([ x for x in seq2 if x not in seen])
    return seq1
  
  

  
  def getEveryDay(self,contract,duration='1 D',barLength='1 min'):
    currentBeijingDatetime=datetime.datetime.now()
    currentESTDatetime=Util.coverToEstDatetime(currentBeijingDatetime)
    currentESTDatetime = currentESTDatetime + pd.DateOffset(days=-1)
    
    
    
    
  def appendToFile(self,tickID,directory,c_symbol,c_secType,endESTDateStr,endESTtimeStr,barLength):
    
    directory="HistData"
    if not os.path.exists(directory):
      os.makedirs(directory)
    barLengthStr = barLength.replace(" ","_") # add the bar length to the file name
    fileName = directory+'/'+c_symbol+'_'+c_secType+'_'+endESTDateStr+'_'+barLengthStr+'.csv'
    if os.path.isfile(fileName): # found a previous version
        file = open(fileName, 'r')
        oldRowData = file.readlines()
        file.close()
        prevRec=len(oldRowData)
        if prevRec > 1:
            # get the new end date and time from the last data line of the file
            lastRow = oldRowData[-1]
            lastRowData=lastRow.split(",")
            endtimeStr = ' %s:%s:%s EST' % (lastRowData[3],lastRowData[4],lastRowData[5])
            #20161211 16:00:00 EST
            if endtimeStr.find('::') :
                if barLength=='1 day':
                    duration=str((int(endESTDateStr[0:4])-int(lastRow[0:4]))*366+(int(endESTDateStr[4:6])-int(lastRow[5:7]))*31+int(endESTDateStr[6:8])-int(lastRow[8:10]))+' D'
                    print "duration:"+duration
                else:
                    print lastRow
                    print "barlength too short"
            #barlength is in mins and previous data has time
            elif barLength.find('min')>0:
                duration=str((int(endESTDateStr[6:8])-int(lastRow[8:10]))*24*60+(int(endESTtimeStr[1:3])-int(lastRow[11:13]))*60+int(endESTtimeStr[4:6])-int(lastRow[14:16]))+' D'
            else:
                print "other unit of time need more work"
    
    else:
        oldRowData = [] # and use default end date
        prevRec=0
        oldRowData.append('date_time,Open,High,Low,Close,Volume\n') 
    
    if self.tick_contract_Map.has_key(tickID):
        reqDic = self.tick_contract_Map[tickID]
        if reqDic['write2file']:
            
            Data=self.cleanMerge(oldRowData,reqDic['reviceData'])
            file = open(fileName, 'w')
            file.writelines(Data)
            file.close()
            print len(Data)-prevRec,' of CSV data appended to file: ', fileName
    
  
  def reqIbHistData(self):
    while True:
      if self.taskQueue.empty():
        break
      reqDic = self.taskQueue.get()
      try:
        print reqDic['contract'],reqDic['duration'],reqDic['barLength']
        if self.m_conn.isConnected():
          self.m_conn.reqHistoricalData(reqDic['tickId'],
                            reqDic['contract'],
                            reqDic['endDateStr'], # last requested bar date/time
                            reqDic['duration'],  # quote duration, units: S,D,W,M,Y
                            reqDic['barLength'],  # bar length
                            reqDic['whatToShow'],  # what to show
                            reqDic['useRTH'], reqDic['formatDate'])
          time.sleep(20)
        #print "endDateStr="+dateStr + ' 23:59:59 EST' 
      except Exception,e:
          print e
    
    return True
  
  
     

  def reqSche(self,tickId,contract,duration,endDatetimeStr,barLength,whatToShow='TRADES',useRTH=0,formatDate=1,write2file=True,write2redis=False):
    endDatetime = pd.to_datetime(endDatetimeStr)
    durationArr = duration.split(' ')

    if barLength == '1 min':
      startDatetime = endDatetime -   int(durationArr[0]) * pd.tseries.offsets.BDay()
      datetimeDelta = endDatetime - startDatetime
      if barLength == '1 min' and datetimeDelta.days >= 1:
        while startDatetime <= endDatetime:
          if  not self.tick_contract_Map.has_key(tickId):
            self.tick_contract_Map[tickId]={}
          self.tick_contract_Map[tickId]['tickId'] = tickId
          self.tick_contract_Map[tickId]['contract'] = contract
          self.tick_contract_Map[tickId]['endDateStr']=startDatetime.strftime('%Y%m%d') + " 23:59:59 EST"
          self.tick_contract_Map[tickId]['duration']='2 D'
          self.tick_contract_Map[tickId]['barLength']=barLength
          self.tick_contract_Map[tickId]['whatToShow']=whatToShow
          self.tick_contract_Map[tickId]['useRTH']=useRTH
          self.tick_contract_Map[tickId]['formatDate']=formatDate
          self.tick_contract_Map[tickId]['reviceData']=[]
          self.tick_contract_Map[tickId]['finished']=False
          self.tick_contract_Map[tickId]['write2file']=True
          self.tick_contract_Map[tickId]['write2redis']=False
          
          startDatetime = startDatetime +  pd.tseries.offsets.BDay()
          
          self.taskQueue.put(self.tick_contract_Map[tickId])
          
          tickId = tickId + 1
    else:
      if  not self.tick_contract_Map.has_key(tickId):
        self.tick_contract_Map[tickId]={}
      self.tick_contract_Map[tickId]['tickId'] = tickId
      self.tick_contract_Map[tickId]['contract'] = contract
      self.tick_contract_Map[tickId]['endDateStr']=endDatetime.strftime('%Y%m%d') + " 23:59:59 EST"
      self.tick_contract_Map[tickId]['duration'] = duration
      self.tick_contract_Map[tickId]['barLength']=barLength
      self.tick_contract_Map[tickId]['whatToShow']=whatToShow
      self.tick_contract_Map[tickId]['useRTH']=useRTH
      self.tick_contract_Map[tickId]['formatDate']=formatDate
      self.tick_contract_Map[tickId]['reviceData']=[]
      self.tick_contract_Map[tickId]['finished']=False
      self.tick_contract_Map[tickId]['write2file']=True
      self.tick_contract_Map[tickId]['write2redis']=False
      self.taskQueue.put(self.tick_contract_Map[tickId])
      tickId = tickId + 1  
        
    





if __name__ == '__main__':
  
  #futContact=Util.makeContract(contractSymbol='PEP',secType='STK',expiry='',exchange='SMART')
  #futContact=Util.makeContract(contractSymbol='TICK-NYSE',secType='IND',expiry='',exchange='NYSE')
  
  
  con = ibConnection(host='localhost',port=7496,clientId=0)
  con.registerAll(Util.watchAll)
  con.unregister(Util.watchAll, message.historicalData)
  
  hhd=HandleHisData(con)
  con.register(hhd.reviceHisData, message.historicalData)
  
  #contract=Util.makeContract(contractSymbol='YM',secType='FUT',expiry='20161216',exchange='ECBOT')
  contract=Util.makeContract(contractSymbol='INDU',secType='IND',exchange='NYSE')
  tickID=1
  hhd.reqSche(tickID,contract,'150 D','20161109' + ' 23:59:59','1 min')
  
  
  con.connect()
  hhd.reqIbHistData()
  #futContact=Util.makeContract(contaceSymbol='ES',secType='FUT',expiry='20161216',exchange='GLOBEX')
  #futContact=Util.makeContract(contaceSymbol='YM',secType='FUT',expiry='20161216',exchange='ECBOT')
  #hhd.getIbHistData(contract=futContact,duration='2 D',barLength='1 min',endDateStr='20161122 23:59:59')
  #hhd.logFile.close()
  con.disconnect()