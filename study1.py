#study1.py
from bs4 import BeautifulSoup
import csv
from glob import glob
import os
import pathlib
import re
import requests
import shutil
import zipfile
from pprint import pprint
import copy
import collections
from tqdm import tqdm


top_d = "/Users/mac2018/Applications/Collection/study1/"
#TOP_DIR = os.path.dirname(__file__)
TOP_DIR = os.path.dirname(top_d)
ZIP_DIR = os.path.join(TOP_DIR, 'zip/')

def yes_no_input():
	while True:
		choice = input("        OK? yes' or 'no' [y/N]:  ( q = quit )").lower()
		if choice in ['y', 'ye', 'yes']:
			return True
		elif choice in ['n', 'no']:
			return False
		elif choice in ['q', 'Q']:
			return quit()

#download function to zipfolder with tqdm
def download_file(url):
    filename = os.path.join(ZIP_DIR, url.split('/')[-1])
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in tqdm(r.iter_content(chunk_size=1024)):
            if chunk:
                f.write(chunk)
                f.flush()
        return filename
    return False

#os.makedirs
os.makedirs(ZIP_DIR, exist_ok=True)

#Parse html
#sorce_url "https://divvy-tripdata.s3.amazonaws.com/index.html"
#r = requests.get('https://divvy-tripdata.s3.amazonaws.com/index.html')
#soup = BeautifulSoup(r.text, 'lxml')  #ajax
path = 'Index of bucket "divvy-tripdata".html'
soup = BeautifulSoup(open(path), 'html.parser')
#pprint(soup)

#example <a href="https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2017_Q1Q2.zip">Divvy_Trips_2017_Q1Q2.zip</a>
#example  element.get('href') == 'https://divvy-tripdata.s3.amazonaws.com/index.html'
url_list = [element.get('href') for element in soup.find_all('a')]
print(f'html_tag_a {len(url_list)}')

#sort url_list_by_filename re.sub

#re.sub('\D', '', url.split('/')[-1]) is NG 2020_10 = 2020Q1
#OK re.sub('[^0-9Q]', '', url.split('/')[-1])

def short_name(url):
	s = re.sub('[^0-9Qq]', '', url.split('/')[-1])
	if s == '':
		s =1
	z = str(s).ljust(10, '0')
	return z

#{short_name(url):url,'2020100000':'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip'}
dict = {short_name(url) : url for url in url_list}
sorted_dict = sorted(dict.items())
sorted_url_l = [ v for k, v in sorted_dict]
print(f'sorted {len(sorted_url_l)}url_link')

# ext_check
ext_list = [url.split('.')[-1] for url in sorted_url_l]

#<class 'collections.Counter'> 
# print(c['a']) == 4 , c.keys() == dict_keys(['a', 'b', 'c']), c.values() == dict_values([4, 1, 2]), c.items() == dict_items([('a', 4), ('b', 1), ('c', 2)])
ext_d = collections.Counter(ext_list)

for ext in list(ext_d) :
	print(f'拡張子"{ext}"は{ext_d[ext]}リンク存在します')

#target_ext is 'zip'
target_l = [ url.replace('\n', '') for url in sorted_url_l if url.split('.')[-1] == 'zip' ]

total_len = len(target_l)

#pastdata check inside link.txt
paths = glob(os.path.join(ZIP_DIR, '*.*'))
txt_l = [path for path in paths if path.split('.')[-1] == 'txt']

if len(txt_l) == 0 :
	print(f'新規ダウンロード。全{total_len}fileを開始しますか？')
	if yes_no_input():
		with open(ZIP_DIR + 'link.txt', 'w', newline='\n') as f:
			for url in target_l:
				f.write("%s\n" % url)
				download_file(url)
		print(f'全{total_len}fileを新規ダウンロードしました')
		print(f'全{total_len}LINKを記入、link.txtを新規作成しました')
		quit()
	else :
		with open(ZIP_DIR + 'link.txt', 'w', newline='\n') as difffile:
			difffile.write('\n'.join(target_l))
		print(f'全{total_len}LINKを記入、link.txtを新規作成しました')
		quit()
else:
	pass


#check_l = target_l + link.txt
check_l = copy.copy(target_l)
if len(txt_l) != 0 :
	for txt in txt_l:
		with open(txt, 'r') as f:
			lines = f.readlines()
			lines = [ line.replace('\n', '') for line in lines ]
			check_l.extend(lines)

#filename_d{,,,.zip:2, ,,,.zip: 2 , ,,,.zip : 1 }  value == 1 is newitem
filename = {}
filename_d = collections.Counter(check_l)

new = [ k for k, v in filename_d.items() if v == 1 and k.split('.')[-1] == 'zip']

if len(new) > 0:
	print('差分のみダウンロードしますか？')
	if yes_no_input():
		for n in new:
			download_file(n)
			name = n.split('/')[-1]
			print(f'{name}を保存しました')
		with open(ZIP_DIR + 'link.txt', 'a', newline='\n') as difffile:
			difffile.write("%s\n" % '\ndiff')
			difffile.write('\n'.join(new))
		print(f'差分{len(new)}LINKをlink.txtに追記しました')
	else:
		with open(ZIP_DIR + 'link.txt', 'a', newline='\n') as difffile:
			difffile.write("%s\n" % '\ndiff')
			difffile.write('\n'.join(new))
		print(f'差分{len(new)}LINKをlink.txtに追記しました')
else:
	pass

if len(new) == 0:
	print('更新はありません')
	print('全てダウンロードしますか？')
	if yes_no_input():
		for url in target_l:
			download_file(url)
		print(f'全{total_len}fileを再ダウンロードしました')
	else :
		pass
else:
	pass

print(f'TOTAL{total_len}箇所の"zip"リンク取得')
print(f'Total Zip files {total_len}files downloadone!')

quit()