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
    df['post_c_l_diff'] = df.Low - df.Close.shift(1)
    df['is_short_high']=np.where((df.prev_h_diff > 0 ) & (df.post_h_diff > 0 ) & (df.post_c_l_diff < 0 ) ,1,0)
    return df

if __name__ == '__main__':
    df =  readDataframe('BABA')
    df.to_csv(r'D:\charm\work\python_ws\handle\yahooData\BABA.csv')


