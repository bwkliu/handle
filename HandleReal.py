#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# ezIBpy: Pythonic Wrapper for IbPy
# https://github.com/ranaroussi/ezibpy
#
# Copyright 2015 Ran Aroussi
#
# Licensed under the GNU Lesser General Public License, v3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ezibpy
import time,redis,time,pytz,sys,os
import pandas as pd
from Util import Util
from utils import dataTypes
import json
import logging.config
import calendar as cal


class RealJumpHandle:

    def __init__(self,clientId=3, host="127.0.0.1", port=7496):
      Util.setup_logging()
      self.tickLogger = logging.getLogger('tick_module')
      self.getCurrDayOpenTimestampSecs()
      self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379)  
      self.redis = redis.Redis(connection_pool=self.pool)
      self.isJumpStartTime = False
      self.ibConn = ezibpy.ezIBpy()
      self.ibConn.connect(clientId, host, port)
      self.ibConn.ibCallback = self.ibCallback
      
    
    
      
      
            

    def getCurrDayOpenTimestampSecs(self):
        currDateStr=pd.datetime.now().strftime('%Y%m%d')
        currDatetime=pd.to_datetime(currDateStr + ' 09:30:00')
        currDatetime = Util.estTZ.localize(currDatetime)
        self.openTimestampSecs=cal.timegm(currDatetime.utctimetuple())
        
    
    def getYestodyDataFromRedis(self):
        currentDatetime = pd.datetime.now() - pd.tserias
        pass
    
    def writeYestodyDataToRedis(sel):
        h=0
        l=0
        c=0
        try:
            h,l,c = Util.yestodayHLC()
            currDay=datetime.datetime.now().strftime('%Y%m%d')
            self.redis.set(currDay+'_yestoDay_H',h)
            self.redis.set(currDay+'_yestoDay_L',l)
            self.redis.set(currDay+'_yestoDay_C',c)
            self.redis.set(currDay+'_yestoDay_data_write_complete',1)
            
        except Exception,e:
            print e
            sys.exit()
        
    def jumpExcute(self,tickPrice):
        logging.info( 'start jumpExcute func,isJumpStartTime:%s' % self.isJumpStartTime )
        if self.isJumpStartTime:
            try:
                is_jump_excute = 0
                currDay=pd.datetime.now().strftime('%Y%m%d')
                is_jump_excute = int(self.redis.get(currDay+'_is_jump_excute'))
                yestodyClose =   float( self.redis.get(currDay+'_yestoDay_C') )
                currPrice =  float(tickPrice)
                if is_jump_excute:
                    contract = self.ibConn.createFuturesContract("YM", exchange="ECBOT", expiry="201612")

                    diffOpen_yestodayClose = currPrice - yestodyClose
                    logging.info('tickPrice:%s yestodyClose:%s diffOpen_yestodayClose:%s' % (tickPrice,yestodyClose,diffOpen_yestodayClose))
                    
                    if abs(diffOpen_yestodayClose) < 10:
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)

                    elif diffOpen_yestodayClose > 10:
                        
                        target = round( (currPrice - diffOpen_yestodayClose / 2 ),0) + 1
                        if diffOpen_yestodayClose > 40:
                            stop = round( (currPrice + diffOpen_yestodayClose ),0) - 1
                        else:
                            stop = round( (currPrice + diffOpen_yestodayClose*1.5 ),0) - 1
                        
                        order = self.ibConn.createBracketOrder(contract, quantity=-1, entry=0, target=target, stop=stop)
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)
                        time.sleep(2)
                        logging.info('tickPrice:%s yestodyClose:%s quantity:-1 entry:MKT target:%s stop:%s' % (tickPrice,yestodyClose,target,stop))
                        logging.info( "sell,main:%s,target:%s,stop:%s" % ('mkt',target,stop) )
                                   
                        
                    elif diffOpen_yestodayClose < -10:
                        target = round( (currPrice + abs(diffOpen_yestodayClose) / 2 ),0) - 1
                        if diffOpen_yestodayClose <= -40:
                            stop = round( (currPrice - abs(diffOpen_yestodayClose)),0) + 1
                        else:
                            stop = round( (currPrice - abs(diffOpen_yestodayClose)*1.5 ),0) - 1
                        
                        order = self.ibConn.createBracketOrder(contract, quantity=1, entry=0, target=target, stop=stop)
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)
                        time.sleep(2)
                        logging.info('tickPrice:%s yestodyClose:%s quantity:1 entry:MKT target:%s stop:%s' % (tickPrice,yestodyClose,target,stop))
                        logging.info( "buy,main:%s,target:%s,stop:%s" % ('mkt',target,stop) )
                        
            except Exception,e:
                print e
                logging.debug( e.message )
                sys.exit()    
        logging.info( 'end jumpExcute func' )
       
        

    def ibCallback(self,caller, msg, **kwargs):
        self.tickLogger.info(str(msg))
        if caller == "handleOrders":
            order = ibConn.orders[msg.orderId]
            if order["status"] == "FILLED":
                print(">>> ORDER FILLED")
                
        elif caller == "handleTickPrice":
            if  self.isJumpStartTime:
                print ( 'msg.tikePrice:%s' % msg.price )
                self.jumpExcute(msg.price)
            
        elif caller == "handleTickString":
            if msg.tickType == dataTypes["FIELD_LAST_TIMESTAMP"]:
                
                if msg.value ==  self.openTimestampSecs:
                    logging.info( 'msg value:%s currDayOpenTimestampSecs:%s' % (msg.value,self.openTimestampSecs) )
                    self.isJumpStartTime = True
            
            print ( 'custom CallBack:%s' % str(msg) )
            
        
           
    

    def requireContractData(self):        
        #contract = self.ibConn.createFuturesContract("ES", exchange="GLOBEX", expiry="201612")
        contract = self.ibConn.createFuturesContract("YM", exchange="ECBOT", expiry="201612")
        self.ibConn.requestMarketData()
        

if __name__ == '__main__':
  
    rjh=RealJumpHandle()
    rjh.requireContractData()
    
    # let order fill
    time.sleep(300000)
    
    # see the positions
    print("Positions")
    print(ibConn.positions)
    
    # disconnect
    ibConn.disconnect()
