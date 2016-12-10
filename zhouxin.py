import pandas as pd
from Util import Util
import sys,os,datetime,pytz

def yestodayHLC(yestodayDateStr=''):
  beijingDatetime = None
  try:
    if not yestodayDateStr:
      beijingDatetime = datetime.datetime.now().strftime('%Y%m%d')
    else:
      beijingDatetime = pd.to_datetime(yestodayDateStr)  
      
    estDatetime =  Util.coverToEstDatetime(beijingDatetime)
    startDate= estDatetime  - pd.tseries.offsets.BDay()*2
    endDate= estDatetime  - pd.tseries.offsets.BDay()*2
    endFile='g:/ibpy/HistData/%s_%s_%s_1_min.csv' % ('YM','FUT',endDate.strftime('%Y%m%d'))
    startFile='g:/ibpy/HistData/%s_%s_%s_1_min.csv' % ('YM','FUT',startDate.strftime('%Y%m%d'))
    startDF=pd.read_csv(startFile)
    endDF=pd.read_csv(endFile)
    ymDF=pd.concat([startDF,endDF])
    ymDF.index=pd.DatetimeIndex(pd.to_datetime(ymDF.date_time))
    print startDate.strftime('%Y%m%d')+' 16:30' , endDate.strftime('%Y%m%d')+' 16:14'
    yestodyDF=ymDF[startDate.strftime('%Y%m%d')+' 16:30' : endDate.strftime('%Y%m%d')+' 16:14']
    print yestodyDF
    h,l,c=(yestodyDF.High.max(),yestodyDF.Low.min(),yestodyDF.Close[-1])
    return (h,l,c)
  except Exception,e:
    print "yestoday HLC error"
    print e
    sys.exit()
   
    
  

def zhouxin(h,l,c):
  zhouxin=(c+h+l)/3;
  zl1=2*zhouxin-l;
  zl2=zhouxin+(h-l);
  zl3=zl1+(h-l);
  zc1=2*zhouxin-h;
  zc2=zhouxin-(h-l);
  zc3=zc1-(h-l);
  data={'Close':c,'zuli_3':zl3,'zuli_2':zl2,'zuli_1':zl1,'center':zhouxin,'zhicheng_1':zc1,'zhicheng_2':zc2,'zhicheng_3':zc3}
  zhouxinDF=pd.DataFrame(data,index=pd.DatetimeIndex(['2016-12-08']));
  return zhouxinDF
