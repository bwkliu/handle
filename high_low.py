from pandas_datareader import data
import pandas as pd
import numpy as np
import datetime
import time
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

def readDataframeFromSrc(stkName,srcName='yahoo'):
    df=data.DataReader(stkName,srcName)
    df.to_csv(r'HistData/yahooData/'+stkName+'.csv')
    return df

def readDataframeFromFile(stkName,filePth = r'HistData/yahooData/' ):
    df=pd.read_csv(filePth + stkName + '.csv')
    df.index=pd.DatetimeIndex(pd.to_datetime(df.Date))
    return df




def handleHigh(df):
    shortHighDF=df.copy()
    
    shortHighDF['prev_h_diff'] = shortHighDF.High - shortHighDF.High.shift(1)
    shortHighDF['post_h_diff']= shortHighDF.High - shortHighDF.High.shift(-1)
    shortHighDF['post_c_l_diff'] = shortHighDF.Low - shortHighDF.Close.shift(-1)
    shortHighDF['short_high']=np.where((shortHighDF.prev_h_diff > 0 ) & (shortHighDF.post_h_diff > 0 ) & (shortHighDF.post_c_l_diff > 0 ) ,1,0)
    
    midHighDF = shortHighDF[shortHighDF.short_high > 0].copy()
    midHighDF['prev_h_diff'] = midHighDF.High - midHighDF.High.shift(1)
    midHighDF['post_h_diff'] = midHighDF.High - midHighDF.High.shift(-1)
    midHighDF['mid_high'] =  np.where( (midHighDF.prev_h_diff > 0 ) & ( midHighDF.post_h_diff>0 ) ,1,0  )
    
    longHighDF=midHighDF[midHighDF.mid_high > 0].copy()
    longHighDF['prev_h_diff'] = longHighDF.High - longHighDF.High.shift(1)
    longHighDF['post_h_diff'] = longHighDF.High - longHighDF.High.shift(-1)
    longHighDF['long_high'] =  np.where( (longHighDF.prev_h_diff > 0 ) & ( longHighDF.post_h_diff>0 ) ,1,0  )
    
    df['short_high'] = 0
    df.short_high.loc[shortHighDF[shortHighDF.short_high>0].index] = 1
    
    df['mid_high'] = 0
    df.mid_high.loc[midHighDF[midHighDF.mid_high>0].index] = 1
    
    df['long_high'] = 0
    df.long_high.loc[longHighDF[longHighDF.long_high>0].index] = 1
    
    
    return shortHighDF,midHighDF,longHighDF

def handleLow(df):
    shortLowDF = df.copy()
    
    shortLowDF['prev_l_diff'] = shortLowDF.Low - shortLowDF.Low.shift(1)
    shortLowDF['post_l_diff']= shortLowDF.Low - shortLowDF.Low.shift(-1)
    shortLowDF['post_c_h_diff'] = shortLowDF.High - shortLowDF.Close.shift(-1)
    shortLowDF['short_low']=np.where((shortLowDF.prev_l_diff < 0 ) & (shortLowDF.post_l_diff < 0 ) & (shortLowDF.post_c_h_diff < 0 ) ,1,0)
    
    midLowDF=shortLowDF[shortLowDF.short_low > 0].copy()
    midLowDF['prev_l_diff'] = midLowDF.Low - midLowDF.Low.shift(1)
    midLowDF['post_l_diff'] = midLowDF.Low - midLowDF.Low.shift(-1)
    midLowDF['mid_low'] =  np.where( (midLowDF.prev_l_diff < 0 ) & ( midLowDF.post_l_diff<0 ) ,1,0  )
        
    longLowDF=midLowDF[midLowDF.mid_low > 0].copy()
    longLowDF['prev_l_diff'] = longLowDF.Low - longLowDF.Low.shift(1)
    longLowDF['post_l_diff'] = longLowDF.Low - longLowDF.Low.shift(-1)
    longLowDF['long_low'] =  np.where( (longLowDF.prev_l_diff < 0 ) & ( longLowDF.post_l_diff<0 ) ,1,0  )
    
    df['short_low'] = 0
    df.short_low.loc[shortLowDF[shortLowDF.short_low>0].index] = 1
    df['mid_low'] = 0
    df.mid_low.loc[midLowDF[midLowDF.mid_low>0].index] = 1
    df['long_low'] = 0
    df.long_low.loc[longLowDF[longLowDF.long_low>0].index] = 1
    
    return shortLowDF,midLowDF,longLowDF



if __name__ == '__main__':
    #df = readDataframeFromSrc('BABA')
    df = readDataframeFromFile('BABA')
    #df.to_csv(r'D:\charm\work\python_ws\handle\yahooData\BABA.csv')
    shd,mhd,lhd = handleHigh(df)
    sld,mld,lld = handleLow(df)
    print df.index


