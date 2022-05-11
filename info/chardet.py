#!/usr/bin/env python
# coding: utf-8

import chardet
def getencoding(dat:bytes):
    return chardet.detect(dat)["encoding"]

