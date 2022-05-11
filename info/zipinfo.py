#!/usr/bin/env python
# coding: utf-8

import os, sys
from functools import reduce

def hex(data, offset, limit, description):
    if limit == 0:
        print('{}: {}'.format(description, ''))
        return offset, None
    l = offset + limit
    v = data[offset:l]
    h = reduce(lambda acc, x: acc + ' ' + x, [b.to_bytes(1, byteorder='big').hex() for b in v])
    print('{}: {}'.format(description, h))
    return l, v

def local_file_header(data, i):
    print('=============== 1. ZIPローカルファイルヘッダ               ===============')
    i, v = hex(data, i, 4, 'シグネチャ                                        ')
    i, v = hex(data, i, 2, '展開に必要なバージョン                            ')
    i, v = hex(data, i, 2, '汎用目的のビットフラグ                            ')
    i, v = hex(data, i, 2, '圧縮メソッド                                      ')
    i, v = hex(data, i, 2, 'ファイルの最終変更時間                            ')
    i, v = hex(data, i, 2, 'ファイルの最終変更日付                            ')
    i, v = hex(data, i, 4, 'CRC-32                                            ')
    i, v = hex(data, i, 4, '圧縮サイズ                                        ')
    i, v = hex(data, i, 4, '非圧縮サイズ                                      ')
    i, n = hex(data, i, 2, 'ファイル名の長さ (n)                              ')
    n = int.from_bytes(n, 'little')
    i, m = hex(data, i, 2, '拡張フィールドの長さ (m)                          ')
    m = int.from_bytes(m, 'little')
    i, v = hex(data, i, n, 'ファイル名                                        ')
    i, v = hex(data, i, m, '拡張フィールド                                    ')
    print()
    return i

def central_directory_file_header(data, i):
    print('=============== 2 .ZIPセントラルディレクトリファイルヘッダ ===============')
    i, v = hex(data, i, 4, 'シグネチャ                                        ')
    i, v = hex(data, i, 2, '作成されたバージョン                              ')
    i, v = hex(data, i, 2, '展開に必要なバージョン                            ')
    i, v = hex(data, i, 2, '汎用目的のビットフラグ                            ')
    i, v = hex(data, i, 2, '圧縮メソッド                                      ')
    i, v = hex(data, i, 2, 'ファイルの最終変更時間                            ')
    i, v = hex(data, i, 2, 'ファイルの最終変更日付                            ')
    i, v = hex(data, i, 4, 'CRC-32                                            ')
    i, v = hex(data, i, 4, '圧縮サイズ                                        ')
    i, v = hex(data, i, 4, '非圧縮サイズ                                      ')
    i, n = hex(data, i, 2, 'ファイル名の長さ (n)                              ')
    n = int.from_bytes(n, 'little')
    i, m = hex(data, i, 2, '拡張フィールドの長さ (m)                          ')
    m = int.from_bytes(m, 'little')
    i, k = hex(data, i, 2, 'ファイルコメントの長さ (k)                        ')
    k = int.from_bytes(k, 'little')
    i, v = hex(data, i, 2, 'ファイルが開始するディスク番号                    ')
    i, v = hex(data, i, 2, '内部ファイル属性                                  ')
    i, v = hex(data, i, 4, '外部ファイル属性                                  ')
    i, v = hex(data, i, 4, 'ローカルファイルヘッダの相対オフセット            ')
    i, v = hex(data, i, n, 'ファイル名                                        ')
    i, v = hex(data, i, m, '拡張フィールド                                    ')
    i, v = hex(data, i, k, 'ファイルコメント                                  ')
    print()
    return i

def end_of_central_directory(data, i):
    print('=============== 3. ZIPセントラルディレクトリの終端レコード ===============')
    i, v = hex(data, i, 4, 'シグネチャ                                        ')
    i, v = hex(data, i, 2, 'このディスクの数                                  ')
    i, v = hex(data, i, 2, 'セントラルディレクトリが開始するディスク          ')
    i, v = hex(data, i, 2, 'このディスク上のセントラルディレクトリレコードの数')
    i, v = hex(data, i, 2, 'セントラルディレクトリレコードの合計数            ')
    i, v = hex(data, i, 4, 'セントラルディレクトリのサイズ (バイト)           ')
    i, v = hex(data, i, 4, 'セントラルディレクトリの開始位置のオフセット      ')
    i, n = hex(data, i, 2, 'ZIPファイルのコメントの長さ (n)                   ')
    n = int.from_bytes(n, 'little')
    i, v = hex(data, i, n, 'ZIPファイルのコメント                             ')
    print()
    return i

def show(path):
    if not os.path.exists(path):
        sys.stderr.write('not found {}\n'.format(path))
        sys.exit(1)
    if os.path.isdir(path):
        sys.stderr.write('not file {}\n'.format(path))
        sys.exit(1)
    with open(path, 'rb') as f:
        data = f.read()
        i = 0
        while i < len(data):
            if data[i:i+4] == b'PK\x03\x04':
                i = local_file_header(data, i)
            elif data[i:i+4] == b'PK\x01\x02':
                i = central_directory_file_header(data, i)
            elif data[i:i+4] == b'PK\x05\x06':
                i = end_of_central_directory(data, i)
            else:
                i += 1

def main(paths):
    for path in paths:
        show(path)

if __name__ == '__main__':
    main(sys.argv[1:])