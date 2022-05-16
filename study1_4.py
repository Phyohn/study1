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
	print('readline return c_10~c_14')
	c1516 = c_15_16(ext, zf, info)
	main_list.extend(c1516)
	main_list.extend(csv)
	#print('main_csv_list return c_1~c_16')
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
"""
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
	print('main_meta return c_1~c_9')
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
"""

#main_metaに置き換えできないか？
def excel_to_df(ext,zf, info) -> list:
	#[c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8,c_9]	
	index = main_meta(ext, zf, info)
	toread = io.BytesIO()
	binary = zf.read(info)
	toread.write(binary) # pass your `decrypted` string as the argument here
	toread.seek(0) # reset the pointer
	df = pd.read_excel(toread)
	c_15 = hdf_l = df.head().to_numpy().tolist()
	c_16 = tdf_l = df.tail().to_numpy().tolist()
	index.append(c_15)
	index.append(c_16)
	index.extend(['0','0','0'])
	c_13 = len(df.columns.tolist())
	index.append(c_13)
	c_14 = df.columns.tolist()
	index.extend(c_14)
	df.to_csv(os.path.join(TOP_DIR,ext,info.filename)+".csv", header = True, index = False)
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


##############    txt   ##############
#txt
def if_txt(ext, zf, info):
	return main_meta(ext, zf, info)

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
#print(new_list)
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
"""
# max(columns_number)
columns_sum_l = [readline(ext, zf, info)[3] for ext ,zf, info in new_list if ext == 'csv']
maxcol_num = max(columns_sum_l)
print(f'csvの最大カラム数 {maxcol_num}')
m = 13 + int(maxcol_num)
print(f' 最大{m}列のデータフレーム')
"""

#△△△△△△△△△△△△△△△△△△△△△△△△△△global_meta_list に格納していく
global_meta_list = []

#zip_check
#xlsx_to_list & extract
#main_csv_list
#txt_check
for ext, zf, info in new_list:
	if ext == 'xlsx' :
		print(f'{info.filename}file extract unzip/xlsx dir')
		excel_list = excel_to_df(ext, zf, info)
		zf.extract(info, path=os.path.join(UNZIP_DIR,ext))
		global_meta_list.append(excel_list)
		#print(excel_list)
		print(f'--------  Catch "xlsx"  -------- count {(len(global_meta_list))}')
	elif ext == 'zip' :
		print(f'{info.filename}file extract unzip/zip dir')
		zf.extract(info, path=os.path.join(UNZIP_DIR,ext))
		zip_list = main_meta(ext, zf, info)
		global_meta_list.append(zip_list)
		print(f'--------  Catch "zip"  -------- count {(len(global_meta_list))}')
	elif ext == 'csv' :
		main_csv = main_csv_list(ext, zf, info)
		global_meta_list.append(main_csv)
		print(f'--------  Catch "csv"  -------- count {(len(global_meta_list))}')
	elif ext == 'txt':
		print(f'{info.filename}file extract unzip/txt dir')
		zf.extract(info, path=os.path.join(UNZIP_DIR,ext))
		txt_l = if_txt(ext, zf, info)
		global_meta_list.append(txt_l)
		print(f'--------  Catch "txt"  -------- count {(len(global_meta_list))}')
	else :
		print(f'--?????  Catch "???"  ?????---{ext, zf, info}?????')
		print('?????????????????????????????')

print(f'global_meta_list{(len(global_meta_list))}列')

#メタデータ出力,最大列数に揃えてDF化#skipinitialspace=Trueコンマ後の空白除去

df = pd.DataFrame(global_meta_list,).dropna(how='all').reset_index(drop=True).fillna('')

c_name = ['zipfile','name', 'unix_time', 'America/Chicago', 'UTC','extension','dir_path', 'bytes', 'metad', 'head5', 'tail5', 'b_str', 'chara_code', 'EOL', 'colmns_sum', 'c_1', 'c_2', 'c_3', 'c_4','c_5', 'c_6', 'c-7', 'c_8', 'c_9', 'c_10', 'c_11', 'c_12', 'c_13']
df.columns = c_name

#df = df.sort_values(by=['dir_path','unix_time'],ascending = [True, True]).reset_index(drop=True)
df = df.sort_values(by=['dir_path','unix_time'],ascending = [True, True])

dt_now = datetime.now()
now = dt_now.strftime('%Y_%m_%d %H_%M_%S')
df_excel_witer(df,TOP_DIR, f'METADATA{now}')

#△△△△△△△△△△△△△△△△△△△△△△△△△△METADATA file
#▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽columns_rename
"" > '' , ' ' > '_' , 'A' > 'a' 
def meta_trim(s):
	s = s.str.strip('"')
	s = s.str.strip()
	s = s.str.replace(' ', '_')
	s = s.str.lower()
	return s

#c_1 ~ c_13 "" > '' , ' ' > '_' , 'A' > 'a'  COLMUNS.xlsx
c_df = df.iloc[:,15:].astype(str).apply(lambda x: meta_trim(x))
df_excel_witer(c_df,TOP_DIR, f'COLMUNS{now}')

#to_dict orient='index'
#{0: {'c_1': 'id', 'c_2': 'name', 'c_3': 'latitude',,,, > dict_values([{'c_1': 'id', 'c_2': 'name', 'c_3': 'latitude',,,, > [{'c_1': 'id', 'c_2': 'name', 'c_3': 'latitude',,,,,

d = c_df.to_dict(orient='index')
values = d.values()
values_list = list(values)

#count columns value pattern
l = []
for values_set in values_list:
	#print(values_set)
	s = values_set
	if s not in l:
		l.append(s)
		print(len(l))
v_l = []
for patten in l:
	v = list(patten.values())
	v_l.append(v)
"""
v_l[0]
['id', 'name', 'latitude', 'longitude', 'dpcapacity', 'landmark', 'online_date', '', '', '', '', '', '']
v_l[1]
['id', 'name', 'latitude', 'longitude', 'dpcapacity', 'datecreated', '', '', '', '', '', '', '']
v_l[2]
['id', 'name', 'latitude', 'longitude', 'dpcapacity', 'landmark', '', '', '', '', '', '', '']
v_l[3]
['id', 'name', 'latitude', 'longitude', 'dpcapacity', 'online_date', '', '', '', '', '', '', '']
v_l[4]
['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date', '', '', '', '', '', '']

v_l[5] NG 'birthday' 'stoptime'
['trip_id', 'starttime', 'stoptime', 'bikeid', 'tripduration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'usertype', 'gender', 'birthday', '']

v_l[6] NG 'stoptime'
['trip_id', 'starttime', 'stoptime', 'bikeid', 'tripduration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'usertype', 'gender', 'birthyear', '']
v_l[7] OK
['trip_id', 'start_time', 'end_time', 'bikeid', 'tripduration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'usertype', 'gender', 'birthyear', '']
v_l[8] NG
['01_-_rental_details_rental_id', '01_-_rental_details_local_start_time', '01_-_rental_details_local_end_time', '01_-_rental_details_bike_id', '01_-_rental_details_duration_in_seconds_uncapped', '03_-_rental_start_station_id', '03_-_rental_start_station_name', '02_-_rental_end_station_id', '02_-_rental_end_station_name', 'user_type', 'member_gender', '05_-_member_details_member_birthday_year', '']
v_l[9] OK
['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual']
v_l[10] NG # empty
"""
#old_index(new_list)
[25, 2, 51, 58, 42, 46, 65, 38, 27, 13, 3, 4, 5, 50, 52, 17, 20, 19, 18, 62, 61, 60, 59, 43, 47, 64, 67, 39, 40, 10, 21, 9, 12, 56, 53, 55, 35, 8, 32, 36, 68, 30, 37, 33, 69, 11, 7, 29, 54, 23, 71, 72, 0, 70, 22, 49, 34, 24, 48, 57, 31, 28, 16, 6, 63, 45, 44, 66, 41, 15, 26, 14, 1]

#reversed
#[1, 14, 26, 15, 41, 66, 44, 45, 63, 6, 16, 28, 31, 57, 48, 24, 34, 49, 22, 70, 0, 72, 71, 23, 54, 29, 7, 11, 69, 33, 37, 30, 68, 36, 32, 8, 35, 55, 53, 56, 12, 9, 21, 10, 40, 39, 67, 64, 47, 43, 59, 60, 61, 62, 18, 19, 20, 17, 52, 50, 5, 4, 3, 13, 27, 38, 65, 46, 42, 58, 51, 2, 25]

#同一カラムリスト
[[15],[31, 57, 48, 24, 34, 49, 22, 70, 0, 72, 71, 23, 54, 29, 7, 11, 69, 33, 37, 30, 68, 36, 32, 8],[35, 55, 53, 56, 12, 9, 21, 10, 40, 39, 67, 64, 47, 43, 59, 60, 61, 62, 18, 19, 20, 17, 52, 50, 5, 4, 3, 13, 27], [38, 65],[46, 42, 58],[51],[2],[25]]

old_rev_index = list(reversed(df.index))
#{1: ('zip', <zipfile.ZipFile filename='/Users/mac2018/Applications/Collection/study1/zip/Divvy_Stations_Trips_2014_Q3Q4.zip' mode='r'>, <ZipInfo filename='Divvy_Stations_Trips_2014_Q3Q4/Divvy_Stations_2014_Q3Q4.zip' external_attr=0x20 file_size=16536>), 14: ('zi,,,,,

#ext == 'csv' len = 61
#[31, 57, 48, 24, 34, 49, 22, 70, 0, 72, 71, 23, 54, 29, 7, 11, 69, 33, 37, 30, 68, 36, 32, 8, 35, 55, 53, 56, 12, 9, 21, 10, 40, 39, 67, 64, 47, 43, 59, 60, 61, 62, 18, 19, 20, 17, 52, 50, 5, 4, 3, 13, 27, 38, 65, 46, 42, 58, 51, 2, 25]

DL_d = {}
for i in old_rev_index:
	ext, zf, info  = new_list[i]
	if ext == 'csv':
		#DL_l.append((ext, zf, info))
		DL_d.update({ i : (ext, zf, info)})
print(len(DL_d))

#pickup headercsv and endcsv for loop
head_index_group = [31, 35, 38, 46, 51, 2, 25]
end_index_group= [8, 27, 65, 58, 51, 2, 25]

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#unzip
global_list = []
name_l = []
CSV_META = [['old_index', 'zipfilename', 'csvfilename', 'short_name', 'header','original_header','original_time', 'total_rows','rows_number']]
for k, (ext, zf, info) in DL_d.items():
	print( k, ext, zf, info)
	if k in head_index_group and k in end_index_group: #ヘッドかつエンドオブジェクトであれば
		print(f'単独 {k}, {ext}, {zf}, {info}')
		time.sleep(3)
		binary = to_binary(zf, info)
		trim_list = decode_sep_trim(binary) #デコード#文字列整形#改行分割
		sn = short_name(info.filename) #short_name()
		name_l.append(sn) 
		date_time = info.date_time 
		rows = len(trim_list)  #行数カウントに格納
		trim_binay = newline_encode(trim_list)#改行、エンコード処理
		csv_maker(date_time, name_l, trim_binay)#出力#ファイル作成#list初期化
		global_list = []
		name_l = []
		CSV_META.append([ k, zf.filename.split('/')[-1], info.filename.split('/')[-1], sn, trim_list[0],True, date_time, rows, rows])
	elif k in head_index_group: #先頭であれば
		op = 1
		print(f'先頭 {k}, {ext}, {zf}, {info}')
		time.sleep(3)
		binary = to_binary(zf, info)
		trim_list = decode_sep_trim(binary) #デコード#文字列整形#改行分割
		sn = short_name(info.filename) #short_name()
		name_l.append(sn) 
		date_time = info.date_time 
		rows = len(trim_list)  #行数カウントに格納
		column_heder_stock = trim_list[0]  #△column_heder_stock
		global_list.extend(trim_list) #△global_list.extend(list)
		CSV_META.append([ k, zf.filename.split('/')[-1], info.filename.split('/')[-1], sn, trim_list[0],True, date_time, int(0), rows])
	elif k in end_index_group: #エンドであれば
		print(f'エンド {k}, {ext}, {zf}, {info}')
		time.sleep(3)
		binary = to_binary(zf, info)
		trim_list = decode_sep_trim(binary) #デコード#文字列整形#改行分割
		sn = short_name(info.filename) #short_name()
		name_l.append(sn) 
		date_time = info.date_time
		trim_list = trim_list[1:] #△header削除
		rows = len(trim_list) #len(lines) = ヘッダーなし行数カウントに格納
		global_list.extend(trim_list) #global_list.extend(list)
		total_rows = len(global_list)
		trim_binay = newline_encode(global_list)#△改行、エンコード処理
		csv_maker(date_time, name_l, trim_binay)#出力#ファイル作成#list初期化
		global_list = []
		name_l = []
		op=0 #△
		CSV_META.append([ k, zf.filename.split('/')[-1], info.filename.split('/')[-1], sn, column_heder_stock,False, date_time, total_rows,rows]) #△
	elif op == 1: #中間要素
		print(f'中間 {k}, {ext}, {zf}, {info}')
		time.sleep(3)
		binary = to_binary(zf, info)
		trim_list = decode_sep_trim(binary) #デコード#文字列整形#改行分割
		sn = short_name(info.filename) #short_name()
		name_l.append(sn) 
		date_time = info.date_time
		trim_list = trim_list[1:] #△header削除
		rows = len(trim_list) #len(lines) = ヘッダーなし行数カウントに格納
		global_list.extend(trim_list) #global_list.extend(list)
		CSV_META.append([ k, zf.filename.split('/')[-1], info.filename.split('/')[-1], sn, column_heder_stock,False, date_time, int(0), rows]) #△
	else :
		print(f' {k}, {ext}, {info.filename} は処理されませんでした。')

#CSV_META to DF to excel
csvdf = pd.DataFrame(CSV_META,).dropna(how='all')
dt_now = datetime.now()
now = dt_now.strftime('%Y_%m_%d %H_%M_%S')
df_excel_witer(csvdf,TOP_DIR, f'CSV_META{now}')

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def short_name(url):
	s = re.sub('[^0-9Qq]', '', url.split('/')[-1])
	return s

#name_l = ['2013', '2014']
#os.path.join(UNZIP_DIR)  + "csv/" + n + ".csv"
#UNZIP_DIR + "csv/" + n + ".csv"
#binay_l = zf.open(info).read()
#print(binay_l)
#print(type(binay_l)) #byte
#date_time = info.date_time

def csv_maker(date_time, name_l, binay_l):
	csvname = '_'.join(name_l) + ".csv"
	path = UNZIP_DIR + "csv/" + csvname
	with open(path, 'wb') as f:
		f.write(binay_l)
	date_time = time.mktime(date_time + (0, 0, -1))
	os.utime(path, (date_time, date_time))
	line_sum = len(binay_l.splitlines())
	print(f'{csvname} {line_sum}-1 header行、出力')
	

def to_binary(zf, info):
	r_binary = zf.open(info).read()
	return r_binary

def decode_sep_trim(binary):
	try:
		s = binary.decode("ascii", "backslashreplace")
		lines_list= s.splitlines()
		trim_list = [str_trim(lines) for lines in lines_list]
	except Exception as e:
		print(f'decode_error{e}')
	else:
		return trim_list

#テスト
#tes = decode_sep_trim(binay_l)

#戻しテスト
#by = newline_encode(tes)
#len(by) #20109
#type(tes)

def newline_encode(list):
	try:
		s = '\r\n'.join(list)
		binary = s.encode("ascii", "backslashreplace")
	except Exception as e:
		print(f'encode_error{e}')
	else:
		return binary

def str_trim(s):
	s = s.replace('"', '')
	s = s.replace(' ', '_')
	s = s.lower()
	return s

def newline_char(s):
	s_l = s.splitlines()
	s_r_n = '\r\n'.join(s_l)
	return s_r_n

def header_cut(s):
	s, num = s[1:], len(s[1:])
	return = s, num





-------------ここまで











csv_maker(date_time, name_l, by)

csv_DL_l =[ DL_d for k ,(ext, zf, info) in DL_d.items() if ext == 'csv']
print(len(csv_DL_l))

#colum_group pickup
group_head_index = [15, 31, 35, 38, 46, 51, 2, 25]
group_mid_index = [ 57, 48, 24, 34, 49, 22, 70, 0, 72, 71, 23, 54, 29, 7, 11, 69, 33, 37, 30, 68, 36, 32, 55, 53, 56, 12, 9, 21, 10, 40, 39, 67, 64, 47, 43, 59, 60, 61, 62, 18, 19, 20, 17, 52, 50, 5, 4, 3, 13, 42]
group_end_index = [15, 8, 27, 65, 58, 51, 2, 25]


f_l = [[15],[31, 57, 48, 24, 34, 49, 22, 70, 0, 72, 71, 23, 54, 29, 7, 11, 69, 33, 37, 30, 68, 36, 32, 8],[35, 55, 53, 56, 12, 9, 21, 10, 40, 39, 67, 64, 47, 43, 59, 60, 61, 62, 18, 19, 20, 17, 52, 50, 5, 4, 3, 13, 27], [38, 65],[46, 42, 58],[51],[2],[25]]

for i, fl in enumerate (f_l):
	for j, f in enumerate (fl):
		if  fl[0] == fl[-1] :
			print(f'単独配列{f}')
			print(f'出力{f}')
			break
		elif f == fl[0] :
			print(f'配列の先頭番号{f}')
			print(f'{i}番ファイルopenデコード変換{f}')
		elif f != fl[0] and f != fl[-1]:
			print(f'{i}番ファイル中間要素{f}')
			print(f'headerOFFデコード変換{f}')
			print(f'{i}番ファイル中間要素{f}')
		elif f == fl[-1]:
			print(f'{i}番ファイル最終要素{f}')
			print(f'headerOFFデコード変換{f}')
			print(f'{i}番ファイル出力{f}')
		else :
			print('error')


global_lines = []
for i, v in DL_d.items() :
	#print( i , v)
	ext, zf, info = v
	#print(i, ext, zf, info)
	if ext == 'csv':
		print('csvout')
		#csv_extract( i, ext, zf, info)
	elif ext == 'xlsx':
		print('xlsxout')
		#excel_to_df(ext,zf, info)
	else:
		pass


def csv_extract( i, ext, zf, info):
	if (i in group_head_index) and (i in group_end_index):
		print(f'単独配列{info.filename}')
		print(f'デコード後改行整形文字列整形')
		print(f'出力{info.filename}')
	elif i in group_head_index:
		print(f'配列の先頭番号{i}')
		print(f'デコード後リスト変換{i}')
		print(f'ループ外リスト変数へappend {i} ')
		print(f'まとめて{info.filename}')
	elif i in group_mid_index:
		print(f'配列の中間要素{i}')
		print(f'デコード後リスト変換{i}')
		print(f'header_cut')
		print(f'ループ外リスト変数へappend {i} ')
		print(f'まとめて{info.filename}')
	elif i in group_end_index:
		print(f'配列の最終要素{i}')
		print(f'header_cut')
		print(f'デコード後リスト変換{i}')
		print(f'ループ外リスト変数へappend {i} ')
		print('改行整形後文字列整形')
		print(f'出力{info.filename}')
	else :
		print('error')

#ファイルオープン状態で関数で追加できるか実験
DL_l[14:16]
def test(ext,zf,info):
	r_binary = zf.open(info).read()
	return r_binary

for ext, zf, info in DL_l[14:16]:
	name, date_time = name_slpit(info.filename)[0], info.date_time
	name = os.path.join(UNZIP_DIR, ext)  + "/" + name
	test_f = open(name, 'wb')

	test_f.write(trimed_r_binary)


		#trimed_r_binary = dec_encoder(r_binary)
		with open(name, 'wb') as f:
			#print(type(f)) #'_io.BufferedWriter'
			




#{0: 'Divvy_Stations_2013.csv', 1: 'Divvy_Stations_2014-Q3Q4.csv', 2: 'Divvy_Stations_2015.csv', 3:
dl_dict = df['name'].to_dict()
print(len(dl_dict)) #73
#print(len(new_list))
#{5:new_list[0], 2:new_list[1], 10:new_list[2],,,,,}
DL_l = {}
for ext, zf, info in new_list:
	info_n = info.filename
	c_2 = name_slpit(info_n)[0]
	for k, v in dl_dict.items():
		if c_2 == v:
			kezi = {k:(ext, zf, info)}
			DL_l.update(kezi)
			break
		else:
			print(f'????{c_2}????{v}??????')


print(len(DL_l))
#{0:new_list[40], 1:new_list[3], 2:new_list[45],,,,,}
score_sorted = sorted(DL_l.items(), key=lambda x:x[0])
DL_l = [items for i, items in score_sorted]
print(len(DL_l))
	
	if c_2 in




for ext, zf, info in new_list:
	if ext == 'csv' :
		#main_csv = main_csv_list(ext, zf, info)
		#global_meta_list.append(main_csv)
		#print(f'--------  Catch "csv"  -------- count {(len(global_meta_list))}')
		name, date_time = info.filename, info.date_time
		with zf.open(info, mode='r') as myfile:
			bytes = myfile.read()
			try:
				s = bytes.decode("utf-8", "backslashreplace")
				print(s)
			except Exception as e:
				print(e)
			else:
				#c_10 b_str
				print(ext)


		name = os.path.join(UNZIP_DIR, ext)  + "/" + name
		print(name)
		with open(name, 'wb') as outf:
			print(type(zf.open(info).read()))
			outf.write(zf.open(info).read())
	date_time = time.mktime(date_time + (0, 0, -1))
	os.utime(name, (date_time, date_time))








with open('matome.csv', 'w', newline='') as f:
	writer = csv.writer(f)


    writer = csv.writer(f)
    writer.writerows(someiterable)
	.write(z.open(f).read())
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














デフォルトでは要素を置換した新たなDataFrameが返されるが、引数inplace=Trueとすると元のDataFrameが変更される。


print(f'set_stationの要素の種類は{len(set_station)}個')
print(f'set_tripの要素の種類は{len(set_trip)}個')
#set_stationの要素の種類は18個  > 6個
#set_tripの要素の種類は55個 > 13個

#空欄削除しながらsetにして重複削除
st_l =[trimming(str) for str in list(set_station) if str != '']
print(set(st_l))
st_l = list(set(st_l))
#クォートの指定は"だけではダメで'"'クォートで囲むこと


rename_trip_col(s)

s = '01 - Rental Details Rental ID'
def rename_trip_col(v):
	#c_1 ride_id
	if v == 'trip_id' or v == "trip_id" or v == '01 - Rental Details Rental ID':
		return 'ride_id'


set_all = {item for item in set_l}
print(len(set_all))
print(set_all)
完成したら
frozenset型





print(df['c_1'].value_counts(ascending=True))
dic= df.iloc[:,15:].value_counts().to_dict()
print(dic)
df.iloc[:,15:].unique()
df.iloc[:,15:]

    pandas.DataFrameをそのままforループに適用
    1列ずつ取り出す
        DataFrame.iteritems()メソッド
    1行ずつ取り出す
        DataFrame.iterrows()メソッド
        DataFrame.itertuples()メソッド
    特定の列の値を順に取り出す
    ループ処理で値を更新する
    処理速度

for i in range(1,14):
	col = ['c_f'{i}'']
	testdic = df['c_1'].value_counts().to_dict()
	exec(f"testdic = df['c_{i}'].value_counts().to_dict()")
	exce(f"dic{i} = df['c_{i}'].value_counts().to_dict()")
exec(f'df{i} = pd.read_excel(zf.extract(info), sheet_name=i)')
i = 1

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




リネームボツ分
#▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽▽columns_rename
"""
#///////////////  station nename  ///////
#station全18カラムへ
#"" > '' , ' ' > '_' , 'A' > 'a'
def trimming(str):
	s = str.strip('"')
	s = s.strip()
	s = s.replace(' ', '_')
	s = s.lower()
	return s

#////////////    trip nename   ////////
#	  //  station nename  //
#trip全18カラムへ
def rename_trip_col(v):
	v = trimming(v)
	#c_1 ride_id
	if v == 'trip_id' or v == '01_-_rental_details_rental_id':
		return 'ride_id'
	#c_2 'rideable_type'
	#c_3 start at
	if v == 'starttime' or v == 'start_time' or v == '01_-_rental_details_local_start_time' :
		return 'started_at'
	#c_4
	if v == 'end_time' or v == 'stoptime' or v == '01_-_rental_details_local_end_time' :
		return 'ended_at'
	#c_5 start name
	if v == 'from_station_name' or v == '03_-_rental_start_station_name':
		return 'start_station_name'
	#c_6 start id
	if v == 'from_station_id' or v == '03_-_rental_start_station_id':
		return 'start_station_id'
	#c_7 end name
	if v == 'to_station_name' or v == '02_-_rental_end_station_name':
		return 'end_station_name'
	#c_8 end id
	if v == 'to_station_id' or v == '02_-_rental_end_station_id':
		return 'end_station_id'
	#c_9 #'start_lat'
	#c_10 #'start_lng'
	#c_11 #'end_lat'
	#c_12 #'end_lng'
	#c_13 'member_casual'
	#old_4 'bikeid'
	if v == "bikeid" or v == '01_-_rental_details_bike_id':
		return 'bikeid'
	#old_5 'tripduration'
	if v == "tripduration" or v == '01_-_rental_details_duration_in_seconds_uncapped':
		return 'tripduration'
	#old_10 'usertype'
	if v == 'usertype' or v == 'user_type':
		return 'usertype'
	#old_11 'gender'
	if v == 'gender' or v == 'member_gender':
		return 'gender'
	#old_12 'birthyear'
	if v ==  '05_-_member_details_member_birthday_year' or v == 'birthday' or v == 'birthyear' :
		return 'birthyear'
	else:
		return  v
#////////////////////////////////
#DF[[a,b,c],[a,a,c],[a,c,'']] > ('a', 4), ('c', 2), ('b', 1), ('', 1)
def count_df_v(DF):
	values = [ i for column_name, item in DF.iloc[:,15:].iteritems() for i in item.values.tolist() if i != '' ]
	c = collections.Counter(values)
	print(f'{name}の要素 : {c.most_common()}')
	return c.most_common()

#df[df['name']].iloc[:,15:] > [[('a', 4), ('c', 2), ('b', 1), ('', 1)], [('d', 4), ('e', 2), ('f', 1),('g', 2), ('h', 1)]]
dict_l = []
for name in ['station', 'trip']:
	df_n = df[df['dir_path'].str.contains(name)]
	dict_l.append(count_df_v(df_n))
	print('--------------')
	

#df[df['name']].iloc[:,15:] > [{'a','c','b',''}, {'d','e','f','g','h}]
df_station = df[df['dir_path'].str.contains('station')]
df_trip = df[df['dir_path'].str.contains('trip')]
set_station = { i for column_name, item in df_station.iloc[:,15:].iteritems() for i in item.values.tolist()}
set_trip = { i for column_name, item in df_trip.iloc[:,15:].iteritems() for i in item.values.tolist()}
set_l = [ set_station, set_trip]


#set to list [{'a','c','b',''}, {'d','e','f','g','h}] > [['a','c','b'], ['d','e','f','g','h]]
v_l_l = [[ v if v != '' else '' for v in set] for set in set_l]

#"" > '' , ' ' > '_' , 'A' > 'a'  
trim_set_l = [list({trimming(str) for str in v_l if str != ''}) for v_l in v_l_l ]

#[(trim_set_l[0], dict_l[0]), (trim_set_l[1], dict_l[1])]
#[(['a','c','b'], [('a', 4), ('c', 2), ('b', 1), ('', 1)]), (['d','e','f','g','h], [('d', 4), ('e', 2), ('f', 1),('g', 2), ('h', 1)])]
rename_l = list(zip(trim_set_l, dict_l))
print(rename_l[0])
print(rename_l[1])

"""
trim_set_l[0]
['landmark', 'id', 'longitude', 'name', 'latitude', 'city', 'dpcapacity', 'online_date', 'datecreated']

dict_l[0]
[('id', 8), ('name', 8), ('latitude', 8), ('longitude', 8), ('dpcapacity', 8), ('online_date', 4), ('landmark', 2), ('online date', 2), ('"id"', 1), ('"name"', 1), ('"city"', 1), ('city', 1), ('"latitude"', 1), ('"longitude"', 1), ('dateCreated', 1), ('"dpcapacity"', 1), ('"online_date"', 1)]

trim_set_l[1]
['tripduration', 'usertype', '05_-_member_details_member_birthday_year', 'to_station_id', 'start_lng', 'start_station_id', 'start_station_name', 'end_lng', '01_-_rental_details_local_end_time', '02_-_rental_end_station_id', 'start_lat', '02_-_rental_end_station_name', 'rideable_type', 'ended_at', 'end_station_id', 'member_casual', 'stoptime', 'gender', 'start_time', 'birthday', 'from_station_name', '01_-_rental_details_duration_in_seconds_uncapped', '03_-_rental_start_station_id', '01_-_rental_details_rental_id', '01_-_rental_details_local_start_time', 'trip_id', '01_-_rental_details_bike_id', 'end_station_name', 'ride_id', 'bikeid', '03_-_rental_start_station_name', 'from_station_id', 'starttime', 'started_at', 'end_time', 'end_lat', 'to_station_name', 'member_gender', 'birthyear', 'user_type']

dict_l[1]
[('ride_id', 25), ('rideable_type', 25), ('started_at', 25), ('ended_at', 25), ('start_station_name', 25), ('start_station_id', 25), ('end_station_name', 25), ('end_station_id', 25), ('start_lat', 25), ('start_lng', 25), ('end_lat', 25), ('end_lng', 25), ('member_casual', 25), ('trip_id', 22), ('bikeid', 22), ('tripduration', 22), ('from_station_id', 22), ('from_station_name', 22), ('to_station_id', 22), ('to_station_name', 22), ('usertype', 22), ('gender', 22), ('birthyear', 21), ('starttime', 15), ('stoptime', 15), ('start_time', 7), ('end_time', 7), ('"trip_id"', 5), ('"bikeid"', 5), ('"tripduration"', 5), ('"from_station_id"', 5), ('"from_station_name"', 5), ('"to_station_id"', 5), ('"to_station_name"', 5), ('"usertype"', 5), ('"gender"', 5), ('"birthyear"', 5), ('"start_time"', 3), ('"end_time"', 3), ('01 - Rental Details Rental ID', 2), ('"starttime"', 2), ('01 - Rental Details Local Start Time', 2), ('"stoptime"', 2), ('01 - Rental Details Local End Time', 2), ('01 - Rental Details Bike ID', 2), ('01 - Rental Details Duration In Seconds Uncapped', 2), ('03 - Rental Start Station ID', 2), ('03 - Rental Start Station Name', 2), ('02 - Rental End Station ID', 2), ('02 - Rental End Station Name', 2), ('User Type', 2), ('Member Gender', 2), ('05 - Member Details Member Birthday Year', 2), ('birthday', 1)]
"""
--------------------------------------
#station_col_rename = {'landmark', 'id', 'longitude', 'name', 'latitude', 'city', 'dpcapacity', 'online_date', 'datecreated'}
x ={}
station_col_rename = set(x)
for name, n in dict_l[0]:
	station_col_rename.add(trimming(name))

print(f'stationカラム{len(station_col_rename)}列に集約')
#set
#station_col_rename後で使う

----------------------------------------
#trip_col_rename = {'started_at', 'end_station_name', 'rideable_type', 'ride_id', 'ended_at', 'start_lng', 'end_station_id', 'end_lat', 'member_casual', 'bikeid', 'start_station_id', 'usertype', 'gender', 'start_station_name', 'tripduration', 'birthyear', 'end_lng', 'start_lat'}

x ={}
trip_col_rename = set(x)
print(type(trip_col_rename))
for name, n in dict_l[1]:
	trim = trimming(name)
	trip_col_rename.add(rename_trip_col(trim))

print(f'tripカラム{len(trip_col_rename)}列に集約')
#set
#trip_col_rename後で使う
---------------------------------------
"""