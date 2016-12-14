from pandas_datareader import data
import pandas as pd
import numpy as np
import datetime
import time
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

def readDataframe(stkName,srcName='yahoo'):
    df=data.DataReader(stkName,srcName)
    df.to_csv(r'D:\charm\work\python_ws\handle\yahooData\BABA.csv')
    return df

def readDataframeFromFile(stkName,filePth = r'D:/charm\work/python_ws/handle/yahooData/' ):
    df=pd.read_csv(filePth + stkName + '.csv')
    df.index=pd.DatetimeIndex(pd.to_datetime(df.Date))
    return df

def handleHigh(df):
    df['prev_h_diff'] = df.High - df.High.shift(1)
    df['post_h_diff']= df.High - df.High.shift(-1)
    df['post_c_l_diff'] = df.Low - df.Close.shift(-1)
    df['short_high']=np.where((df.prev_h_diff > 0 ) & (df.post_h_diff > 0 ) & (df.post_c_l_diff > 0 ) ,1,0)
    
    shortHighDF=df[df.short_high > 0].copy()
    shortHighDF['prev_h_diff'] = shortHighDF.High - shortHighDF.High.shift(1)
    shortHighDF['post_h_diff'] = shortHighDF.High - shortHighDF.High.shift(-1)
    shortHighDF['midHigh'] =  np.where( (shortHighDF.prev_h_diff > 0 ) & ( shortHighDF.post_h_diff>0 ) ,1,0  )
    df['mid_high'] = 0
    df.mid_high.loc[shortHighDF[shortHighDF.midHigh>0].index] = 1
    del shortHighDF
    
    longHighDF=df[df.mid_high > 0].copy()
    longHighDF['prev_h_diff'] = longHighDF.High - longHighDF.High.shift(1)
    longHighDF['post_h_diff'] = longHighDF.High - longHighDF.High.shift(-1)
    longHighDF['long_high'] =  np.where( (longHighDF.prev_h_diff > 0 ) & ( longHighDF.post_h_diff>0 ) ,1,0  )
    df['long_high'] = 0
    df.long_high.loc[longHighDF[longHighDF.long_high>0].index] = 1
    del longHighDF 
    
    return df

def handleLow(df):
    df['prev_l_diff'] = df.Low - df.Low.shift(1)
    df['post_l_diff']= df.Low - df.Low.shift(-1)
    df['post_c_h_diff'] = df.High - df.Close.shift(-1)
    df['short_low']=np.where((df.prev_l_diff < 0 ) & (df.post_l_diff < 0 ) & (df.post_c_h_diff < 0 ) ,1,0)
    
    shortLowDF=df[df.short_low > 0].copy()
    shortLowDF['prev_l_diff'] = shortLowDF.Low - shortLowDF.Low.shift(1)
    shortLowDF['post_l_diff'] = shortLowDF.Low - shortLowDF.Low.shift(-1)
    shortLowDF['midLow'] =  np.where( (shortLowDF.prev_l_diff < 0 ) & ( shortLowDF.post_l_diff<0 ) ,1,0  )
    df['mid_low'] = 0
    df.mid_low.loc[shortLowDF[shortLowDF.midLow>0].index] = 1
    del shortLowDF
    
    longLowDF=df[df.mid_low > 0].copy()
    longLowDF['prev_l_diff'] = longLowDF.Low - longLowDF.Low.shift(1)
    longLowDF['post_l_diff'] = longLowDF.Low - longLowDF.Low.shift(-1)
    longLowDF['long_low'] =  np.where( (longLowDF.prev_l_diff < 0 ) & ( longLowDF.post_l_diff<0 ) ,1,0  )
    df['long_low'] = 0
    df.long_low.loc[longLowDF[longLowDF.long_low>0].index] = 1
    del longLowDF 
    
    return df



if __name__ == '__main__':
    df =  readDataframeFromFile('BABA')
    #df.to_csv(r'D:\charm\work\python_ws\handle\yahooData\BABA.csv')
    #handleHigh(df)
    handleLow(df)


