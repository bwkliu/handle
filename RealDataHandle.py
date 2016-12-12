#! /usr/bin/env python
# -*- coding: utf-8 -*-

from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
from time import sleep
import threading
import redis
import pytz,time,datetime
import Util
from multiprocessing import Process,Queue
from Util import Util
import sys

class RealDataHandle:
    
    
    
    def __init__(self,tws_conn):
      self.tws_conn = tws_conn
      #self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379)  
      #self.redis = redis.Redis(connection_pool=self.pool)  
      self.bid_ask_q=None  
      
      self.tick_contract_Map={}
      self.bid_ask_map={}
    # print all messages from TWS
    
    def setContract(self,tickId,contract):
        if self.tick_contract_Map.has_key(tickId):
            print "the tiackId had in tick_contract_Map"
            return false
        
        
        if self.bid_ask_map.has_key(tickId):
            print "the tiackId had in bid_ask_map"
            return false
        
        self.tick_contract_Map[tickId] = contract
        
        self.bid_ask_map[tickId] = {}
        

    
    
    
    def watcher(self,msg):
        print '111111111111'
        
        print msg
        #pass
    # show Bid and Ask quotes
    def my_BidAsk(self,msg):
      
        self.bid_ask_q.put(str(msg))
      
        if not self.tick_contract_Map.has_key(msg.tickerId):
            print 'no tickID'
            return 
        if not self.bid_ask_map.has_key(msg.tickerId):
            print 'tickID map no init'
            return 

        
        contract=self.tick_contract_Map[msg.tickerId]
        currentBeijingDatetime=datetime.datetime.now()
        beijingTZ = pytz.timezone('Asia/Shanghai')
        beijingDateTime = beijingTZ.localize(currentBeijingDatetime)
        estTZ = pytz.timezone('US/Eastern')
        estDatetime=beijingDateTime.astimezone(estTZ)
        timestampStr=str(time.mktime(estDatetime.timetuple()))
            
        #self.redis.rpush(('%s_%s_%s' % (contract.m_symbol,contract.m_secType,estDatetime.strftime('%Y_%m_%d'))),str(msg))
        
        
        tmpDict=self.bid_ask_map[msg.tickerId]
        if msg.field == 1 and msg.canAutoExecute == 1:          
           tmpDict['c_bid_price'] = msg.price
           print ('c_bid_price: %s' %  msg.price)
        elif msg.field == 2 and msg.canAutoExecute == 1:
            tmpDict['c_ask_price'] = msg.price
            print ('c_ask_price: %s' %  msg.price)
            
    
    
      
            
            
    def my_Orders(self,msg):
        print '333333333333333333333333333333'
        print msg
        if msg.typeName == 'openOrder':
          contract = msg.contract
          order=msg.order
          orderState=msg.orderState
        elif  msg.typeName == 'orderStatus':
          print msg


def my_BidAsk_handle(bid_ask_q):
    print  'new_process'
    while True:
      try:
        value = bid_ask_q.get(True)
        print value
        
      except Exception,e:
        print  e
         
      
      
               

if __name__ == '__main__':
    
    bid_ask_q = Queue()
    
    try:
      pw = Process(target=my_BidAsk_handle, args=(bid_ask_q,))
      pw.start()
      
    except Exception,e:
      print e
      sys.exit()
    
    
    
    con = ibConnection(host='localhost',port=7496,clientId=1)
    rdh=RealDataHandle(con)
    rdh.bid_ask_q=bid_ask_q
    
    
    con.registerAll(rdh.watcher)
    showBidAskOnly = True  # set False to see the raw messages
    if showBidAskOnly:
        con.unregister(rdh.watcher, [message.tickPrice,message.openOrder,message.orderStatus])
        con.register(rdh.my_BidAsk, message.tickPrice)
        con.register(rdh.my_Orders, message.openOrder,message.orderStatus)
    con.connect()
    sleep(1)
    

    # Note: Option quotes will give an error if they aren't shown in TWS
    #contractTuple = ('QQQQ', 'OPT', 'SMART', 'USD', '20070921', 47.0, 'CALL')
    #ES_contractTuple = ('ES', 'FUT', 'GLOBEX', 'USD', '20161216', 0.0, '')
    #contractTuple = ('ES', 'FOP', 'GLOBEX', 'USD', '20070920', 1460.0, 'CALL')
    #contractTuple = ('EUR', 'CASH', 'IDEALPRO', 'USD', '', 0.0, '')
    #ym_contractTuple = ('YM', 'FUT', 'ECBOT', 'USD', '20161216', 0.0, '')
    YM_Contract = Util.makeContract(contractSymbol='YM',secType='FUT',expiry='20161216',exchange='ECBOT')
    tickId = 1
    rdh.setContract(tickId,YM_Contract)
    #ES_Contract = rdh.makeStkContract(ES_contractTuple)
    #print ('* * * * REQUESTING MARKET DATA * * * *')
    
    for tickId,contract in rdh.tick_contract_Map.items():
        con.reqMktData(tickId, contract, '', False)
    #con.reqAllOpenOrders()
    
    
    pw.join()
    sleep(1500000)
    print ('* * * * CANCELING MARKET DATA * * * *')
    con.cancelMktData(tickId)
    sleep(1)
    con.disconnect()
    sleep(1)
