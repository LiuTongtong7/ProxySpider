#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 
# Created by liutongtong on 2018/7/5 01:16
#

import logging


def set_logging(log_enable=True, log_file=None, log_level='DEBUG'):
    level_names = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
        'SILENT': logging.CRITICAL + 1
    }
    if log_enable:
        log_level = level_names[log_level]
        if not log_file:
            handler = logging.StreamHandler()
        else:
            handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logging.basicConfig(handlers=[handler], level=log_level)
