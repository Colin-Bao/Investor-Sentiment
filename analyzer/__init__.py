#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :__init__.py.py
# @Time      :2022/11/14 22:59
# @Author    :Colin
# @Note      :None

from __future__ import absolute_import, print_function

import sys


def _check_python_version():
    if not sys.version_info >= (3, 4) and not sys.version_info >= (2, 7):
        raise ImportError("""
pystata only supports Python 2.7 and 3.4+.
        """)


_check_python_version()
del _check_python_version
