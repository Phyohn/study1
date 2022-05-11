#decode.py
import base64
import sys
from functools import reduce

encodes = [
    'cp932',
    'euc_jp',
    'euc_jis_2004',
    'euc_jisx0213',
    'iso2022_jp',
    'iso2022_jp_1',
    'iso2022_jp_2',
    'iso2022_jp_2004',
    'iso2022_jp_3',
    'iso2022_jp_ext',
    'shift_jis',
    'shift_jis_2004',
    'shift_jisx0213',
    'utf_16',
    'utf_16_be',
    'utf_16_le',
    'utf_7',
    'utf_8',
    'utf_8_sig',
]

def main(args):
    target = int('0x' + reduce(lambda acc, x: acc + x, args), 16)
    for encode in encodes:
        try:
            print('[OK]{0}: {1}'.format(encode, target.to_bytes(len(args), 'big').decode(encode)))
        except Exception as e:
            print('[NG]{0}: {1}'.format(encode, e))

if __name__ == '__main__':
    main(sys.argv[1:])