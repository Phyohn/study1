#!/usr/bin/env python
# coding: utf-8

import nkf
def getencoding(dat:bytes):
    if b"\0" in dat:
        return None
    enc = nkf.guess(dat).lower()
    if enc and enc == "shift_jis":
        return "cp932"
    elif enc == "binary":
        return None
    else:
        return enc
