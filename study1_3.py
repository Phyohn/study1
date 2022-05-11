#study1_3.py
from bs4 import BeautifulSoup
import csv
from glob import glob
import os
import pandas as pd
import pathlib
import re
import requests
from zipfile import ZipFile
import zipfile
import time
import datetime

top_d = "/Users/mac2018/Applications/Collection/study1/"
#TOP_DIR = os.path.dirname(__file__)
TOP_DIR = os.path.dirname(top_d)
ZIP_DIR = os.path.join(TOP_DIR, 'zip/')
CSV_DIR = os.path.join(TOP_DIR, 'csv/')
TRIP_DIR = os.path.join(CSV_DIR, 'trip/')
STATION_DIR = os.path.join(CSV_DIR, 'station/')
ANY_DIR = os.path.join(CSV_DIR, 'any/')
'''
#復活採用フルパスリストになるので使いにくい
trip_csv = glob(os.path.join(TRIP_DIR, '*.csv'))
<class 'list'>
'''
#存在するcsvだけ取得
#glob モジュールの glob() はリストを返すが、pathlib の glob() はイテレータを返す
trip_gen= pathlib.Path(TRIP_DIR).glob('*.csv')
print(type(trip_gen))
<class 'generator'>
リストに入れる
trip_csv_gen_l = list(pathlib.Path(TRIP_DIR).glob('*.csv'))
print(type(trip_csv_gen_l))
<class 'list'>

'''
def one_line(gen_l):
	for csv_gen in gen_l:
		with open(csv_gen) as f:
			for i, line in enumerate(f):
				if i < 1:
					s = (f'{csv_gen.name},{line}')
					l = s.split(',')
					m.append(int(len(l)))
					print(l)
					break

start_time = time.process_time()
m = []
one_line(trip_csv_gen_l)
print(max(m))
end_time = time.process_time()
elapsed_time = end_time - start_time
print(elapsed_time)
one_line 0.004108999999999696

start_time = time.process_time()
m = []
r_line(trip_csv_gen_l)
print(f'最大要素数{max(m)}個')
end_time = time.process_time()
elapsed_time = end_time - start_time
print(elapsed_time)
'''


def r_line(gen_l):
	m = []
	for csv_gen in gen_l:
		with open(csv_gen) as f:
			s =(f'{csv_gen.name},{f.readline()}')
			l = s.split(',')
			print(type(l))
			m.append(int(len(l)))
			print(l)
	max_len = max(m)
	print(f'最大要素数{max(m)}個')

＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼＼
やはりpathで処理したほうが汎用性が高いので
trip_csv_path_l = glob(os.path.join(TRIP_DIR, '*.csv'))
#ファイル名だけにスリットで分ける
#print(list(map(lambda x : x.split('/')[-1], trip_csv)))
#trip_csv_n_list = list(map(lambda x : x.split('/')[-1], trip_csv))


#def colum_n(csv_gen):
	with open(csv_gen) as f:
		s =(f'{csv_gen.name},{f.readline()}')
		l = s.split(',')
		return [l, len(l)]

def colum_n(csv_path):
	with open(csv_path) as f:
		name = csv_path.split('/')[-1]
		s =(f'{name},{f.readline()}')
		l = s.split(',')
		return [l, len(l)]


#colname_l_and_max = [colum_n(csv_gen) for csv_gen in trip_csv_gen_l]
col_name_l_and_max = [colum_n(csv_path) for csv_path in trip_csv_path_l]

#内包表記でmax取り出し
m = max([m[1] for m in colname_l_and_max])
print(f'最大要素数{m}個')
#columnsのlistのlist
colname_l = [colname_l[0] for colname_l in colname_l_and_max]
print(len(colname_l))
#columnsの要素数をmax個数に揃える
trim_colname_l = list(map(lambda x: x + [0]*(m-len(x)), colname_l))
#dfへ
#最新csvのカラム名を取得、'extfilename'を先頭列名にしたlistを作る
latest_trip_filename = df_trip.iloc[-1]['extfilename']
print(type(latest_trip_filename))
with open(os.path.join(TRIP_DIR, latest_trip_filename)) as f:
	s = f.readline()
	l = s.split(',')
	l.insert(0, 'extfilename')
	print(len(l))
#lをカラム名にしてdf
colname_df_trip = pd.DataFrame(trim_colname_l, columns=l)
#時系列ソートのためにdf_tripとon='datetime'でマージ
sort_colname_df_trip = colname_df_trip.merge(df_trip, on='extfilename').sort_values(by=['datetime'],ascending = [True])

#excelへ
def df_witer(df,path,filename):
	df.to_excel(os.path.join(path, filename +'.xlsx'), sheet_name=filename)
df_witer(sort_colname_df_trip,CSV_DIR,'colname_trip')



https://qiita.com/KTakahiro1729/items/c9cb757473de50652374
複数のリストからユニークな要素とその個数を取得

複数のリストから重複を取り除きユニークな（一意な）値の要素を抽出したい場合は、リストをすべて足し合わせてから集合set()型に変換する。

l1_l2_or = set(l1 + l2)
print(l1_l2_or)
# {'c', 'b', 'a', 'd'}

print(list(l1_l2_or))
# ['c', 'b', 'a', 'd']

print(len(l1_l2_or))
# 4
ファイルの最終更新時刻
def t_stamp(p):
	t1 = os.stat(p).st_birthtime
	t2 = os.path.getmtime(p)
	t3 = os.path.getatime(p)
	d1 = datetime.datetime.fromtimestamp(t1)
	d2 = datetime.datetime.fromtimestamp(t2)
	d3 = datetime.datetime.fromtimestamp(t3)
	return d1, d2, d3

t_s = [t_stamp(csv_path) for csv_path in trip_csv_path_l]
t_s[0]

import datetime
import os

p = './test.csv'

t1 = os.stat(p).st_birthtime
t2 = os.path.getmtime(p)
t3 = os.path.getatime(p)

d1 = datetime.datetime.fromtimestamp(t1)
d2 = datetime.datetime.fromtimestamp(t2)
d3 = datetime.datetime.fromtimestamp(t3)

print(d1)
# 2019-11-27 18:45:53.631646

print(d2)
# 2019-11-28 21:01:23.314099

print(d3)
# 2019-11-27 19:37:47.698257
#Zipfile内のcsvが解凍の仕方により作成日時を維持できていない可能性があるので調査
'202004-divvy-tripdata.csv'の作成時刻は2020年6月1日 月曜日 23:43
p = '/Users/mac2018/Applications/Collection/Study1/test/202004-divvy-tripdata.zip'
dp = '/Users/mac2018/Applications/Collection/Study1/test/'
zipinfo_list = ZipFile(p).infolist()
print(zipinfo_list[0])
<ZipInfo filename='202004-divvy-tripdata.csv' compress_type=deflate filemode='-rw-r--r--' external_attr=0x4000 file_size=14254024 compress_size=3322761>
zipinfo = zipinfo_list[0]
print(zipinfo)
print(zipinfo.date_time)
'202004-divvy-tripdata.csv'
同じファイルがPC上では2020,6/1/23/43 JST
UTC                                 2020,6/1/14/43 UTC
ファイルのタイムスタンプ 2020,6/1/10,43, EDT米国東部標準時(夏時間)
夏季時間と標準時間の切り替えがある。

(2020, 6, 1, 10, 43, 50)

def zonetime(zipinfo):
	time_taple = zipinfo.date_time
	a,b,c,d,e,f = time_taple
	d_time = datetime.datetime(a, b, c, d, e, f)
	return d_time



with zipfile.ZipFile(zipinfo) as zf:
	zf.extract(zipinfo, '/Users/mac2018/Applications/Collection/Study1/test')

zf = zipfile.ZipFile(p, mode='r', strict_timestamps=True)
	zf.extract(zipinfo)

name_l = zf.namelist()
name = name_l[0]
print(type(name_l))
zf.extractall(dp, name_l,)
with zf.open(zinfo, mode='r') as extfile:
			f = write(extfile.read())
import platform
dtstamp = creation_date(p)
dt = datetime.datetime.fromtimestamp(dtstamp)
2022-04-27 14:16:26.775768

def creation_date(path_to_file):
	if platform.system() == 'Windows':
		return os.path.getctime(path_to_file)
	else:
		stat = os.stat(path_to_file)
		try:
			return stat.st_birthtime
		except AttributeError:
			return stat.st_mtime



def expander(zf, zinfo, path, filename):
	with open(os.path.join(path, filename), mode='wb') as f :
		with zf.open(zinfo, mode='r') as extfile:
			f.write(extfile.read())

#m.append(int(len(l)))

print(sum(len(v) for v in l_2d))
## Flatten
X = sum(A.tolist(), [])
## Max size
l = max(map(len, X))
## Padding
B = np.array(map(lambda x: x + [0]*(l-len(x)), X))

B
=>
array([[1, 2, 3, 0],
       [1, 2, 3, 0],
       [1, 2, 3, 0],
       [1, 2, 0, 0],
       [1, 2, 0, 0],
       [1, 2, 0, 0],
       [1, 2, 0, 0],
       [1, 2, 3, 4],
       [1, 2, 3, 4]])


#ファイルを空にする
with open(event_list.txt, mode="r+") as f: 
    #print(f.read())
    f.truncate(0)