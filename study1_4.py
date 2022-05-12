#study1_2.py
# -*- coding: ascii -*-
from bs4 import BeautifulSoup
import csv
import chardet
from datetime import datetime
from glob import glob
import os
import pandas as pd
import pathlib
import re
import requests
from zipfile import ZipFile
import zipfile
import time
from zoneinfo import ZoneInfo
import io
import openpyxl
import xlrd
import pprint
import collections


top_d = "/Users/mac2018/Applications/Collection/study1/"
#TOP_DIR = os.path.dirname(__file__)
TOP_DIR = os.path.dirname(top_d)
ZIP_DIR = os.path.join(TOP_DIR, 'zip/')
CSV_DIR = os.path.join(TOP_DIR, 'csv/')
TRIP_DIR = os.path.join(CSV_DIR, 'trip/')
STATION_DIR = os.path.join(CSV_DIR, 'station/')
ANY_DIR = os.path.join(CSV_DIR, 'any/')
UNZIP_DIR = os.path.join(TOP_DIR, 'unzip/')

###########   META   #############
def main_meta(ext, zf, info) -> list:
	zf_n = zf.filename
	info_n = info.filename
	#c_1 taple(zipname,ext)
	c_1 = name_slpit(zf_n)[0]
	#c_2 taple(csvname,ext)
	c_2 = name_slpit(info_n)[0]
	#c_3,4,5 taple(unix_dt, chicago_t, UTC)
	c_3, c_4, c_5 = tz_chicaco(info)
	#c_6 ext
	c_6 = ext
	#c_7 path_dir
	c_7 = name_checker(c_2)
	#c_8 bytes
	c_8 = info.file_size
	#c_9'metad'['c_type', 'c_sys', 'c_ver', 'ext_ver', 'reserv', 'f_bits', 'vol', 'inter_sttr', 'ext_attr', 'header_offset', 'CRC', 'comp_size', 'extra']
	c_9 = [f'c_type:{info.compress_type}, c_sys:{info.create_system}, c_ver:{info.create_version}, ext_ver:{info.extract_version}, reserv:{info.reserved}, f_bits:{info.flag_bits}, vol:{info.volume}, inter_attr:{info.internal_attr}, ext_attr:{info.external_attr}, header_offset:{info.header_offset}, CRC:{info.CRC},comp_size:{info.compress_size}, extra:{info.extra}']
	print('main_meta return c_1~c_9')
	return [c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8, c_9]


###########   MAIN_CSV_LIST   #################
		###   META   ###
		###   READLINE   ####
		###    C15_C16  CSV   ###
		###    C15_C16  EXCEL DF ###

def main_csv_list(ext, zf, info) -> list:
	main_list = main_meta(ext, zf, info)
	csv = readline(ext, zf, info)
	c1516 = c_15_16(ext, zf, info)
	main_list.extend(c1516)
	main_list.extend(csv)
	print('main_csv_list return c_1~c_16')
	return main_list

############################################



#make filename and ext
def name_slpit(str) -> tuple:
	s = str.split('/')[-1]
	if '.' in s:
		ext = s.split('.')[-1]
	else:
		ext = 'DIR'
	return s, ext

#tokyo = ZoneInfo("Asia/Tokyo") #now = datetime(2020, 10, 1, 0, 0, 0, tzinfo=tokyo) #tzdata #print(now.isoformat())
#両方満たせばaware
#条件１:print(dt.tzinfo) が None でない　返す　America/Chicago
#条件２:print(dt.tzinfo.utcoffset(dt))が None を返さない　返す　-1 day, 19:00:00
#a,b,c,d,e,f = (2020, 6, 1, 10, 43, 50)

def tz_chicaco(zipinfo):
	a,b,c,d,e,f = zipinfo.date_time
	utc = ZoneInfo('UTC')
	cst_cdt = ZoneInfo('America/Chicago')
	dt = datetime(a,b,c,d,e,f,tzinfo = cst_cdt)
	unix_dt =	dt.timestamp()
	#print(f'{dt.tzinfo} {datetime.fromtimestamp(unix_dt, tz=cst_cdt )}')
	chicago_t = f'{datetime.fromtimestamp(unix_dt, tz=cst_cdt )} {dt.tzinfo}'
	#print(f'UTC {datetime.fromtimestamp(unix_dt, tz=utc )}')
	UTC = f'{datetime.fromtimestamp(unix_dt, tz=utc )} UTC'
	return unix_dt, chicago_t, UTC

#dir_path
def name_checker(str):
	s, ext = name_slpit(str)
	if 'station' in s.casefold():
		return (f'unzip/{ext}/station')
	elif 'trip' in s.casefold():
		return (f'unzip/{ext}/trip')
	else :
		return (f'unzip/{ext}')

##################   READLINE   ############



#decode type "utf-8", "backslashreplace" is b'\x80abc' '\\x80abc'#後ほど\\検索で場所を拾える
def readline(ext, zf, info) -> list:
	if ext != 'csv':
		return [None, None, None, None, [0, 0]]
	else:
		with zf.open(info, mode='r') as myfile:
			bytes = myfile.readline()
			try:
				s = bytes.decode("utf-8", "backslashreplace")
			except Exception as e:
				print(e)
			else:
				#c_10 b_str
				c_10 = bytes
				#c_11 "utf-8" "ascii"...
				c_11 = getchara(bytes)
				#c_12 EOL
				c_12 = eol(bytes)
				#c_13 columns_sum
				c_13 = columns(s)[0]
				#c_14 col_name_l
				c_14 = columns(s)[1]
				readline_list =  [c_10, c_11, c_12, c_13]
				readline_list.extend(c_14)
				print('readline ruturn c_10~c_14')
				return readline_list

# "utf-8" "ascii"...
def getchara(bytes):
	return chardet.detect(bytes)["encoding"]

#"CRLF" "CR" "LF"
def eol(bytes):
	if bytes[-2:] == b'\r\n':
		return "CRLF"
	elif bytes[-1:] == b'\r':
		return "CR"
	elif bytes[-1:] == b'\n':
		return "LF"
	else:
		return "unknown"

def columns(str) -> list:
	str = re.sub('[\r\n]+$', '', str)
	str_l = str.split(',')
	return [len(str_l), str_l]


##############    C15_C16  EXCEL DF ################
def excel_to_df(ext,zf, info) -> list:
	zf_n = zf.filename
	info_n = info.filename
	c_1 = name_slpit(zf_n)[0]
	c_2 = name_slpit(info_n)[0]
	c_3, c_4, c_5 = tz_chicaco(info)
	c_6 = ext
	c_7 = name_checker(c_2)
	c_8 = info.file_size
	c_9 = [f'c_type:{info.compress_type}, c_sys:{info.create_system}, c_ver:{info.create_version}, ext_ver:{info.extract_version}, reserv:{info.reserved}, f_bits:{info.flag_bits}, vol:{info.volume}, inter_attr:{info.internal_attr}, ext_attr:{info.external_attr}, header_offset:{info.header_offset}, CRC:{info.CRC},comp_size:{info.compress_size}, extra:{info.extra}']
	toread = io.BytesIO()
	binary = zf.read(info)
	toread.write(binary) # pass your `decrypted` string as the argument here
	toread.seek(0) # reset the pointer
	df = pd.read_excel(toread)
	index = [c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8,c_9]
	c_15 = hdf_l = df.head().to_numpy().tolist()
	c_16 = tdf_l = df.tail().to_numpy().tolist()
	index.append(c_15)
	index.append(c_16)
	index.extend(['0','0','0'])
	c_13 = len(df.columns.tolist())
	index.append(c_13)
	c_14 = df.columns.tolist()
	index.extend(c_14)
	df.to_csv(os.path.join(TOP_DIR,ext,c_2)+".csv", header = True, index = False)
	return index


##############    C15_C16  CSV   ################
#.rstrip()メソッドは改行文字を削除 eval() クオートを削除
def c_15_16(ext, zf, info):
	with zf.open(info, mode='r') as myfile:
		bytes = myfile.readlines()
		myfile.close()
	c_15 = [[byte.decode("ascii", "backslashreplace").rstrip()] for byte in bytes[1:6]]
	c_16 = [[byte.decode("ascii", "backslashreplace").rstrip()] for byte in bytes[-6:-1]]
	time.sleep(1)
	print('c_15_c16 return c_15,c_16')
	return [c_15 , c_16]















$$$$$$$$$$$$$$$     SETTING   $$$$$$$$$$$$$

def macosx_checker(i, zf, info):
	if 'MACOSX' in info.filename:
		print('Delete MACOSX')
	else:
		print(i)
		return i, zf, info

def df_excel_witer(df,path,filename):
	df.to_excel(os.path.join(path, filename +'.xlsx'), sheet_name=filename)


#make UNZIP_DIR 
def mkdir2(list):
	for path in list:
		p = os.path.join(UNZIP_DIR, path)
		if not os.path.isdir(p):
			os.makedirs(p, exist_ok=False)
			print(f'ディレクトリ{path}を新規作成しました。')
		else:
			print(f'ディレクトリ{path}は既に存在しています。')

#(i,zf,info)>(ext,zf,info) == global new_list , make 'unzip' dir 'ext_name'
def ext_type_check(zftaplelist): 
	ext_list = []
	ext_list = [name_slpit(info.filename)[1] for i, zf, info in zftaplelist]
	itemcounter(ext_list)
	ext_set = set(ext_list)
	ext_trim_l = list(ext_set)
	mkdir2(ext_trim_l)
	global new_list
	new_list = []
	for i, zf, info in zftaplelist:
		i = name_slpit(info.filename)[1]
		t = [i , zf, info]
		new_list.append(t)
	return new_list

#<class 'collections.Counter'> 
# print(c['a']) == 4 , c.keys() == dict_keys(['a', 'b', 'c']), c.values() == dict_values([4, 1, 2]), c.items() == dict_items([('a', 4), ('b', 1), ('c', 2)])
def itemcounter(ext_l):
	ext_d = collections.Counter(ext_l)
	for ext in list(ext_d) :
		print(f'拡張子"{ext}"は{ext_d[ext]}item存在します')



@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ここまで関数

# [zip_path1,zip_path2,...]
files = glob(os.path.join(ZIP_DIR, '*.zip'))

# [[1, ZipFile_object1],[2, ZipFile_object2],...]
zf_l = [(i, zipfile.ZipFile(x)) for i, x in enumerate (files, start=1)]


# testzip
for i, zf in zf_l:
	try:
		zf.testzip()
	except (RuntimeError, TypeError, NameError) as error:
    		print(error)
	else:
		print(i, zf)
	print('all file zip check pass')


#[[1, ZipFile_object1, ZipInfo__object1], [1, ZipFile_object1, ZipInfo__object2], [2, ZipFile_object2, ZipInfo__object1], ....]
zf_info_l = [(i, zf, info) for i, zf in zf_l for info in zf.infolist()]
print(f'----------zipfile{zf_info_l[-1][0]}個,全要素数{len(zf_info_l)}個----------')

#macosx delete
#[[1, ZipFile_object1, ZipInfo__object1], None, [2, ZipFile_object2, ZipInfo__object1], ....]
l = [macosx_checker(i, zf, info) for i, zf, info in zf_info_l]
#[[1, ZipFile_object1, ZipInfo__object1],[2, ZipFile_object2, ZipInfo__object1], ....]
l = [x for x in l if x != None]
print(f'----------MACOSX削除,全要素数{len(l)}個----------')

#ext_type_check mkdir2 itemcounter
#new_list = (ext, zf, info)
new_list = ext_type_check(l)
print(new_list)
print(f'new_listの要素数{len(new_list)}')
"""
拡張子"csv"は62リンク存在します
拡張子"zip"は3リンク存在します
拡張子"txt"は8リンク存在します
拡張子"xlsx"は1リンク存在します
ディレクトリzipは既に存在しています。
ディレクトリxlsxは既に存在しています。
ディレクトリcsvは既に存在しています。
ディレクトリtxtは既に存在しています。
"""
# max(columns_number)
columns_sum_l = [readline(ext, zf, info)[3] for ext ,zf, info in new_list if ext == 'csv']
maxcol_num = max(columns_sum_l)
print(f'csvの最大カラム数 {maxcol_num}')
m = 13 + int(maxcol_num)
print(f' 最大{m}列のデータフレーム')


#△△△△△△△△△△△△△△△△△△△△△△△△△△global_meta_list に格納していく
global_meta_list = []

#zip_check
#xlsx_to_list & extract
#main_csv_list

for ext, zf, info in new_list:
	if ext == 'xlsx' :
		print(f'{ext}file extract unzip/xlsx dir')
		excel_list = excel_to_df(ext, zf, info)
		zf.extract(info, path=os.path.join(UNZIP_DIR,ext))
		global_meta_list.append(excel_list)
		print(excel_list)
		print(f'meta_data "xlsx" {(len(global_meta_list))}列格納')
	if ext == 'zip' :
		print(f'{ext}file extract unzip/zip dir')
		zf.extract(info, path=os.path.join(UNZIP_DIR,ext))
		zip_list = main_meta(ext, zf, info)
		global_meta_list.append(zip_list)
		print(f'meta_data "zip" {(len(global_meta_list))}列格納')
	if ext == 'csv' :
		main_csv = main_csv_list(ext, zf, info)
		global_meta_list.append(main_csv)
		print(f'meta_data "csv" {(len(global_meta_list))}列格納')
	else :
		print(f'その他の拡張子{ext, zf, info}があります')

print(f'global_meta_list{(len(global_meta_list))}列')

#メタデータ出力,最大列数に揃えてDF化#skipinitialspace=Trueコンマ後の空白除去

df = pd.DataFrame(global_meta_list,).dropna(how='all').reset_index(drop=True).fillna('')

c_name = ['zipfile','name', 'unix_time', 'America/Chicago', 'UTC','extension','dir_path', 'bytes', 'metad', 'head5', 'tail5', 'b_str', 'chara_code', 'EOL', 'colmns_sum', 'c_1', 'c_2', 'c_3', 'c_4','c_5', 'c_6', 'c-7', 'c_8', 'c_9', 'c_10', 'c_11', 'c_12', 'c_13']
df.columns = c_name

df = df.sort_values(by=['dir_path','unix_time'],ascending = [True, True]).reset_index(drop=True)


dt_now = datetime.now()
now = dt_now.strftime('%Y_%m_%d %H_%M_%S')
df_excel_witer(df,TOP_DIR, f'METADATA{now}')

#△△△△△△△△△△△△△△△△△△△△△△△△△△METADATAファイル作成
#valueの要素数をカウントするc_1~c_13
print(df['c_1'].value_counts(ascending=True))
dic= df.iloc[:,15:].value_counts().to_dict()
print(dic)
df.iloc[:,15:].unique()
df.iloc[:,15:]





with open('data.csv', 'w') as file:
writer = csv.writer(file, lineterminator='\n')
writer.writerow(main)



for i in range(開始行数):

    y.readline()

for i in range(読み出し行数):

    lines1 = y.readline()

    data.append(float(lines1))

y.close()
			s = bytes.decode("utf-8", "backslashreplace")
		print(s)

ext, zf, info = a,b,c
print(zf.open(info).read())
f = zf.extract(info)
df = pd.read_excel(zf.extract(info))
hdf_l = df.head().to_numpy().tolist()
tdf_l = df.tail().to_numpy().tolist()

fd = b.extract(c).read()
b.extract(c).read()







oooooooooooooooooooooooo
csv_l =[]
for ext, zf, info in new_list:
	if ext == 'csv':
		#readline(ext, zf, info)
		main_csv_list(ext, zf, info)
		print('yyyyyy')
		print(csv_l)

#csv実験
sniffでdialectの型を判別
with open('eggs.csv', newline='') as csvfile:
	dialect = csv.Sniffer().sniff(csvfile.read(1024))
	print(dialect) #<class 'csv.Sniffer.sniff.<locals>.dialect'>
	csvfile.seek(0)
	reader = csv.reader(csvfile, dialect)
	print(type(reader)) #<class '_csv.reader'>
	print(reader.dialect)
	for row in reader:
		print(row)



読んだ行数
csvreader.line_num
名前？
csvreader.fieldnames





#pandas DF trim_csv_l  sort 'dir_path','unix_time' reset_index
df = pd.DataFrame(trim_csv_l, columns = ['zipfile','name', 'unix_time', 'America/Chicago', 'UTC','extension','dir_path', 'bytes', 'metad', 'b_str', 'chara_code', 'EOL', 'colmns_sum', 'c_1', 'c_2', 'c_3', 'c_4','c_5', 'c_6', 'c-7', 'c_8', 'c_9', 'c_10', 'c_11', 'c_12', 'c_13', 'c_14']).sort_values(by=['dir_path','unix_time'],ascending = [True, True]).reset_index(drop=True)

#excelへ
dt_now = datetime.now()
now = dt_now.strftime('%Y_%m_%d %H_%M_%S')

df_excel_witer(df,TOP_DIR, f'METADATA{now}')

#メタデータをチェック後
#テスト用
files = glob(os.path.join('/Users/mac2018/Applications/Collection/Study1/test', '*.zip'))


zf <zipfile.ZipFile filename='/Users/mac2018/Applications/Collection/Study1/test/Divvy_Stations_Trips_2014_Q1Q2.zip' mode='r'>

l[1]
(1, <zipfile.ZipFile filename='/Users/mac2018/Applications/Collection/Study1/test/Divvy_Stations_Trips_2014_Q1Q2.zip' mode='r'>, <ZipInfo filename='Divvy_Stations_2014_Q1Q2.shp.zip' external_attr=0x20 file_size=17412>)

i, zf, info = l[2]

print(zf.open(info).read())
f = zf.extract(info)
df = pd.read_excel(zf.extract(info))

#zf,infoからdf作成してヘッダー情報を[0],df情報を[1]に持つリストを返す
def excel_to_df(zf, info):
	#シートの数と名前
	wb = openpyxl.load_workbook(zf.extract(info))
	wb_l = wb.worksheets
	list(range(0,len(wb_l)))
	index = []
	for i in list(range(0,len(wb_l))):
		#全てのシートを読み込むsheet_name=None
		#空のリストにdf{i}.columns.tolist()を追加
		exec(f'df{i} = pd.read_excel(zf.extract(info), sheet_name=i)')
		print(f'df{i}作成、合計{len(wb_l)}個のDF')
		exec(f'index.append(df{i}.columns.tolist())')







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

df = pd.read_csv(r'/Users/mac2018/Applications/Collection/Study1/test/202004-divvy-tripdata.zip',compression='zip')

print(df.head())









"""
#解凍関数
def expander(zf, zinfo, path, filename):
	with open(os.path.join(path, filename), mode='wb') as f :
		with zf.open(zinfo, mode='r') as extfile:
			f.write(extfile.read())

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




#一旦ここで書き出し

#存在するcsvだけ取得

name_slpit(info.filename)[1] == 'csv'
#glob モジュールの glob() はリストを返すが、pathlib の glob() はイテレータを返す
trip_gen= pathlib.Path(TRIP_DIR).glob('*.csv')
print(type(trip_gen))
<class 'generator'>
リストに入れる
trip_csv_gen_l = list(pathlib.Path(TRIP_DIR).glob('*.csv'))
print(type(trip_csv_gen_l))
<class 'list'>

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


'''
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



#CSV出力実験
with open('eggs.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=' ',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
    spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])

Spam Spam Spam Spam Spam |Baked Beans|
Spam |Lovely Spam| |Wonderful Spam|

with open('eggs.csv', newline='') as csvfile:
	pamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in pamreader:
		print(', '.join(row))

with open('eggs.csv', newline='') as csvfile:
	
	pamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in pamreader:
		print(', '.join(row))

Spam, Spam, Spam, Spam, Spam, Baked Beans
Spam, Lovely Spam, Wonderful Spam
############
 class csv.Sniffer

    Sniffer クラスは CSV ファイルの書式を推理するために用いられるクラスです。

    Sniffer クラスではメソッドを二つ提供しています:

    sniff(sample, delimiters=None)

        与えられた sample を解析し、発見されたパラメータを反映した Dialect サブクラスを返します。オプションの delimiters パラメータを与えた場合、有効なデリミタ文字を含んでいるはずの文字列として解釈されます。
Sniffer の利用例:

with open('202101-divvy-tripdata.csv', newline='') as csvfile:
	print(type(csvfile)) #<class '_io.TextIOWrapper'>
	dialect = csv.Sniffer().sniff(csvfile.readline())
	print(csv.list_dialects())
	csvfile.seek(0)
	reader = csv.reader(csvfile, dialect)
	for row in reader:
		print(row)
    # ... process CSV file contents here ...

csv モジュールでは以下の定数を定義しています:

csv.QUOTE_ALL

    writer オブジェクトに対し、全てのフィールドをクオートするように指示します。

csv.QUOTE_MINIMAL

    writer オブジェクトに対し、 delimiter 、 quotechar または lineterminator に含まれる任意の文字のような特別な文字を含むフィールドだけをクオートするように指示します。

csv.QUOTE_NONNUMERIC

    writer オブジェクトに対し、全ての非数値フィールドをクオートするように指示します。

    reader に対しては、クオートされていない全てのフィールドを float 型に変換するよう指示します。

csv.QUOTE_NONE

    writer オブジェクトに対し、フィールドを決してクオートしないように指示します。現在の delimiter が出力データ中に現れた場合、現在設定されている escapechar 文字が前に付けられます。 escapechar がセットされていない場合、エスケープが必要な文字に遭遇した writer は Error を送出します。

    reader に対しては、クオート文字の特別扱いをしないように指示します。

csv モジュールでは以下の例外を定義しています: