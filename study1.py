#study1.py
from bs4 import BeautifulSoup
import csv
import os
import pathlib
import re
import requests
import shutil
import zipfile
from pprint import pprint
import collections
from tqdm import tqdm

top_d = "/Users/mac2018/Applications/Collection/study1/"
#TOP_DIR = os.path.dirname(__file__)
TOP_DIR = os.path.dirname(top_d)
ZIP_DIR = os.path.join(TOP_DIR, 'zip/')
CSV_DIR = os.path.join(TOP_DIR, 'csv/')
README_SUM_DIR = os.path.join(TOP_DIR, 'readme_sum/')

'''
#download function
def download_file(url):
    filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
        return filename
    return False
'''

#download function to zipfolder
def download_file(url):
    filename = os.path.join(ZIP_DIR, url.split('/')[-1])
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
        return filename
    return False

#os.makedirs
os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(CSV_DIR + 'trip', exist_ok=True)
os.makedirs(CSV_DIR + 'station', exist_ok=True)
os.makedirs(CSV_DIR + 'any', exist_ok=True)
os.makedirs(README_SUM_DIR, exist_ok=True)

#Parse html
path = "Index of bucket _divvy-tripdata_.html"
soup = BeautifulSoup(open(path), 'html.parser')
pprint(soup)

#example <a href="https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2017_Q1Q2.zip">Divvy_Trips_2017_Q1Q2.zip</a>
#example  element.get('href') == 'https://divvy-tripdata.s3.amazonaws.com/index.html'
url_list = [element.get('href') for element in soup.find_all('a')]
print(f'html_tag_a {len(url_list)}')
#sort url_list_by_filename re.sub
#{zeroume(url):url,'2020100000':'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip'}

#re.sub('\D', '', url.split('/')[-1]) is NG 2020_10 = 2020Q1
#OK re.sub('[^0-9Q]', '', url.split('/')[-1])

def zeroume(url):
	s = re.sub('[^0-9Qq]', '', url.split('/')[-1])
	if s == '':
		s =1
	z = str(s).ljust(10, '0')
	return z


dict = {zeroume(url) : url for url in url_list}
sorted_dict = sorted(dict.items())
sorted_url_l = [ v for k, v in sorted_dict]
print(f'sorted {len(sorted_url_l)}')
# ext_check
ext_list = [url.split('.')[-1] for url in sorted_url_l]
#<class 'collections.Counter'> 
# print(c['a']) == 4 , c.keys() == dict_keys(['a', 'b', 'c']), c.values() == dict_values([4, 1, 2]), c.items() == dict_items([('a', 4), ('b', 1), ('c', 2)])
ext_d = collections.Counter(ext_list)

for ext in list(ext_d) :
	print(f'拡張子"{ext}"は{ext_d[ext]}リンク存在します')

#target_ext is 'zip'
target_ext = [x for x in list(ext_d) if x == 'zip' ]

target_l =[]
for url in sorted_url_l:
	item_ex = url.split('.')[-1]
	for x in target_ext:
		if item_ex == x :
			target_l.append(url)

print(f'{len(target_l)}箇所の"zip"リンク取得')

#download all zip



#make link text ver
with open('link.txt', 'w') as f:
	for url in url_list:
		f.write("%s\n" % url)
		#download_file(url)

"""
for url in target_l:
	download_file(url)
"""

print(f'{len(target_l)}files downloadone!')

quit()