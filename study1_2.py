#study1_2.py
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
from datetime import datetime
from zoneinfo import ZoneInfo

#Python3.9では zoneinfo モジュールが追加され、標準ライブラリだけでタイムゾーン名を利用してタイムゾ#ーンを指定できるようになりました。
#tokyo = ZoneInfo("Asia/Tokyo") # タイムゾーン情報を取得
#now = datetime(2020, 10, 1, 0, 0, 0, tzinfo=tokyo) # Asia/Tokyoタイムゾーンでの現在時刻を取得
#tzdata」をpipで入れておかないとエラーになる。
#print(now.isoformat())

top_d = "/Users/mac2018/Applications/Collection/study1/"
#TOP_DIR = os.path.dirname(__file__)
TOP_DIR = os.path.dirname(top_d)
ZIP_DIR = os.path.join(TOP_DIR, 'zip/')
CSV_DIR = os.path.join(TOP_DIR, 'csv/')
TRIP_DIR = os.path.join(CSV_DIR, 'trip/')
STATION_DIR = os.path.join(CSV_DIR, 'station/')
ANY_DIR = os.path.join(CSV_DIR, 'any/')

tz_chicaco関数
def tz_chicaco(zipinfo):
	a,b,c,d,e,f = zipinfo.date_time
	utc = ZoneInfo('UTC')
	cst_cdt = ZoneInfo('America/Chicago')
	dt = datetime(a,b,c,d,e,f,tzinfo = cst_cdt)
	unix_dt =	dt.timestamp()
	print(f'{dt.tzinfo} {datetime.fromtimestamp(unix_dt, tz=cst_cdt )}')
	print(f'UTC {datetime.fromtimestamp(unix_dt, tz=utc )}')
	return unix_dt



#両方満たせばaware
#print(dt.tzinfo) が None でない　返す　America/Chicago
#print(dt.tzinfo.utcoffset(dt))が None を返さない　返す　-1 day, 19:00:00
#a,b,c,d,e,f = (2020, 6, 1, 10, 43, 50)

#def opzip_to_list(file):
	zipinfo_list = ZipFile(file).infolist()
	tmp_list =[]
	for zipinfo in zipinfo_list :
		rootfile = file.split('/',)[-1]
		print('------')
		file_n = zipinfo.filename.split('/')[-1]
		time_taple = zipinfo.date_time
		a,b,c,d,e,f = time_taple
		d_time = datetime.datetime(a, b, c, d, e, f)
		filesize = zipinfo.file_size
		is_dir = zipinfo.is_dir()
		tmp_list.append([rootfile,file_n,d_time, filesize, is_dir ])
	print(len(tmp_list))	
	return tmp_list

def opzip_to_list(file):
	zipinfo_list = ZipFile(file).infolist()
	tmp_list =[]
	for zipinfo in zipinfo_list :
		rootfile = file.split('/',)[-1]
		print('------')
		file_n = zipinfo.filename.split('/')[-1]
		filesize = zipinfo.file_size
		is_dir = zipinfo.is_dir()
		tmp_list.append([rootfile,file_n, zonetime(zipinfo), filesize, is_dir ])
	print(len(tmp_list))	
	return tmp_list

def df_witer(df,path,filename):
	df.to_excel(os.path.join(path, filename +'.xlsx'), sheet_name=filename)

def mkdir(path, name):
	p = os.path.join(path,name)
	print(p)
	if not os.path.isdir(p):
		os.makedirs(p, exist_ok=False)
		print(f'ディレクトリ{name}を新規作成しました。')
	else:
		print(f'ディレクトリ{name}は既に存在しています。')


#解凍関数
def expander(zf, zinfo, path, filename):
	with open(os.path.join(path, filename), mode='wb') as f :
		with zf.open(zinfo, mode='r') as extfile:
			f.write(extfile.read())

#def mkcomment(zipinfo):
	dt_now_utc = datetime.datetime.now(datetime.timezone.utc)
	filetime = zonetime(zipinfo)
	print(dt_now_utc)
	cmt = f'unziptime {dt_now_utc} byMT originalfile timestamp EDT/EST {filetime}'
	return cmt


files = glob(os.path.join(ZIP_DIR, '*.zip'))
l = []
for file in files:
	l.extend(opzip_to_list(file))


print(f'{len(files)}個のzipに{len(l)}個の書類' )

#list to pd
df = pd.DataFrame( l, columns = ['rootfile','extfilename', 'datetime', 'filesize', 'is_dir'] )
df.info()
#datacleansing
df_trim = df[(df['filesize'] >1000) & (df['is_dir'] == False)].sort_values(by=['datetime'],ascending = [True])
droped_df = df[(df['filesize'] <= 1000) | (df['is_dir'] == True)].sort_values(by=['datetime'],ascending = [True])
#output
df_witer(df_trim, TOP_DIR, 'ext_zip_list')
df_witer(droped_df, TOP_DIR, 'droped_list')
trim_len = int(len(df_trim))
droped_len = int(len(droped_df))
print(f'------全有効ファイル数{trim_len}------無効ファイル数{droped_len}------')

#makedir
mkdir(TOP_DIR, 'csv')
mkdir(CSV_DIR, 'trip')
mkdir(CSV_DIR, 'station')
mkdir(CSV_DIR, 'any')

#splitdf
df_station = df_trim[df_trim['extfilename'].str.contains('station', case=False)]
df_trip = df_trim[df_trim['extfilename'].str.contains('trip', case=False)]
df_any = df_trim[~(df_trim['extfilename'].str.contains('station', case=False)) & ~(df_trim['extfilename'].str.contains('trip', case=False))]

df_witer(df_station,CSV_DIR,'station')
df_witer(df_trip,CSV_DIR,'trip')
df_witer(df_any,CSV_DIR,'any')
#行数確認
if (len(df_station) + len(df_trip) + len(df_any)) == trim_len :
	print(f'-----   station{len(df_station)}行、trip{len(df_trip)}行、any{len(df_any)} = 総数{trim_len}行。 行数OK！！！-----')
else:
	print('取得漏れがあります')


#filenamelist from df
station_list = df_station['extfilename'].tolist()
trip_list = df_trip['extfilename'].tolist()
any_list = df_any['extfilename'].tolist()

#インフォリストを元に必要なファイルだけ数えながら抽出
cnt = [0,0,0,0]
for file in files:
	info_l = []
	zf = ZipFile(file)
	info_l.extend(zf.infolist())
	for zinfo in info_l:
		zinfo_str = zinfo.filename.split('/')[-1]
		print(zinfo_str)
		if zinfo_str in station_list:
			cnt[0] += 1
			expander(zf, zinfo, STATION_DIR, zinfo_str)
		elif zinfo_str in trip_list:
			cnt[1] += 1
			expander(zf, zinfo, TRIP_DIR, zinfo_str)
		elif zinfo_str in any_list:
			cnt[2] += 1
			expander(zf, zinfo, ANY_DIR, zinfo_str)
		else :
			cnt[3] +=1
			pass

print(f'===== stationフォルダに{cnt[0]}個、tripフォルダに{cnt[1]}個、anyフォルダに{cnt[2]}個、追加。その他の廃棄したデータ{cnt[3]} =====')

#ファイルに更新時間を残したまま解凍

import os
import zipfile
import time

outDirectory = '/Users/mac2018/Applications/Collection/Study1/test'
inFile = '/Users/mac2018/Applications/Collection/Study1/test/202004-divvy-tripdata.zip'
fh = open(os.path.join(outDirectory,inFile),'rb') 
z = zipfile.ZipFile(fh)

for f in z.infolist():
	name, date_time = f.filename, f.date_time
	name = os.path.join(outDirectory, name)
	with open(name, 'wb') as outFile:
		outFile.write(z.open(f).read())
	date_time = time.mktime(date_time + (0, 0, -1))
	os.utime(name, (date_time, date_time))



#csvの改行コード文字コード検査

# newidea
files = glob(os.path.join(ZIP_DIR, '*.zip'))

zf_l = [zipfile.ZipFile(x) for x in files]
print(type(zf_l))
info_l = zf.infolist
zf_info_l = [(zf, info) for zf in zf_l for info in zf.infolist()]  できた
zf_info_l = [[zf, info] for zf in zf_l for info in zf.infolist()] 


