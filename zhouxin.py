import pandas as pd


def read_data():
  

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
