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


class RealJumpHandle:

    def __init__(self,clientId=3, host="127.0.0.1", port=7496):
      self.setup_logging()
      logging.info("start func")
      sys.exit()
      self.getCurrDayOpenTimestampSecs()
      self.pool = redis.ConnectionPool(host='127.0.0.1', port=6379)  
      self.redis = redis.Redis(connection_pool=self.pool)
      self.isJumpStartTime = False
      self.ibConn = ezibpy.ezIBpy()
      self.ibConn.connect(clientId=3, host="127.0.0.1", port=7496)
      self.ibConn.ibCallback = self.ibCallback
      
    
    
    def setup_logging(self,default_path = "logging.json",default_level = logging.INFO,env_key = "LOG_CFG"):
        path = default_path
        value = os.getenv(env_key,None)
        if value:
            path = value
        if os.path.exists(path):
            with open(path,"r") as f:
                config = json.load(f)
                logging.config.dictConfig(config)
        else:
            logging.basicConfig(level = default_level)  
      
            

    def getCurrDayOpenTimestampSecs(self):
        currDateStr=pd.datetime.now().strftime('%Y%m%d')
        currDatetime=pd.to_datetime(currDateStr + ' 09:30:00')
        currDatetime = Util.estTZ.localize(currDatetime)
        self.openTimestampSecs=time.mktime(currDatetime.timetuple())
        
    
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
        if self.isJumpStartTime:
            try:
                currDay=pd.datetime.now().strftime('%Y%m%d')
                is_jump_excute = int(self.redis.get(currDay+'_is_jump_excute'))
                yestodyClose =   float( self.redis.get(currDay+'_yestoDay_C') )
                currPrice =  float(tickPrice)
                if is_jump_excute:
                    #contract = self.ibConn.createFuturesContract("YM", exchange="GLOBEX", expiry="201612")

                    diffOpen_yestodayClose = currPrice - yestodyClose
                    
                    if abs(diffOpen_yestodayClose) < 10:
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)

                    elif diffOpen_yestodayClose > 10:
                        
                        target = round( (currPrice - diffOpen_yestodayClose / 2 ),0) + 1
                        if diffOpen_yestodayClose > 40:
                            stop = round( (currPrice + diffOpen_yestodayClose ),0) - 1
                        else:
                            stop = round( (currPrice + diffOpen_yestodayClose*1.5 ),0) - 1
                        #order = self.ibConn.createBracketOrder(contract, quantity=-1, entry=0, target=2200, stop=1900.)
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)
                        print "sell,main:%s,target:%s,stop:%s" % ('mkt',target,stop)
                        print '********************************'           
                        
                    elif diffOpen_yestodayClose < -10:
                        target = round( (currPrice + abs(diffOpen_yestodayClose) / 2 ),0) - 1
                        if diffOpen_yestodayClose <= -40:
                            stop = round( (currPrice - abs(diffOpen_yestodayClose)),0) + 1
                        else:
                            stop = round( (currPrice - abs(diffOpen_yestodayClose)*1.5 ),0) - 1
                        
                        #order = self.ibConn.createBracketOrder(contract, quantity=1, entry=0, target=2200, stop=1900.)
                        self.isJumpStartTime = False
                        self.redis.set(currDay+'_is_jump_excute',0)
                        print "buy,main:%s,target:%s,stop:%s" % ('mkt',target,stop)
                        print '********************************'
            except Exception,e:
                print e
                sys.exit()    
    
       
        

    def ibCallback(self,caller, msg, **kwargs):
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
                print 'msg value:%s' % msg.value
                if msg.value ==  self.openTimestampSecs:
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
