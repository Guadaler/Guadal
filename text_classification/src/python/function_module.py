#!/usr/bin/python
#coding: utf-8
#
# backup_remote_data_files
# $Id: backup_remote_data_files.py  2015-11-06 Qiu $
#
# history:
# 2015-11-06 Qiu   created
#
# QiuQiu@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# backup_remote_data_files.py is
#
# Copyright (c)  by ShangHai KunYan Data Service Co. Ltd..  All rights reserved.
#
# By obtaining, using, and/or copying this software and/or its
# associated documentation, you agree that you have read, understood,
# and will comply with the following terms and conditions:
#
# Permission to use, copy, modify, and distribute this software and
# its associated documentation for any purpose and without fee is
# hereby granted, provided that the above copyright notice appears in
# all copies, and that both that copyright notice and this permission
# notice appear in supporting documentation, and that the name of
# ShangHai KunYan Data Service Co. Ltd. or the author
# not be used in advertising or publicity
# pertaining to distribution of the software without specific, written
# prior permission.
#
# --------------------------------------------------------------------

"""
industry classification


"""

import codecs
import MySQLdb
import re
import sys
import jieba
reload(sys)
sys.setdefaultencoding('utf8')

class FunctionModule(object):

    """industry classification function module


    Attributes:
        no.
    """

    def __init__(self):

        """initiate object


        Attributes:
            no.
        """
        self.conn = MySQLdb.connect(host='192.168.0.22', user='root',
                                    passwd='mysql', db='test',
                                    charset='utf8')
        self.cur = self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)

    def get_data_from_MySQL(self, sql):

        """get data from mysql


        Attributes:
            no.
        """
        try:
            self.cur.execute(sql)
        except Exception, e:
            print e
        result = self.cur.fetchall()
        return result

    # def get_eles_from_data(self, data, ele_list):
    #
    #     """get ele from data
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     dict = {}
    #     for ele in ele_list:
    #         dict_temp = {}
    #         for lines in data:
    #             if ele == 'title':
    #                 dict_temp[str(lines[0])] = lines[1]
    #             elif ele == 'article':
    #                 dict_temp[str(lines[0])] = lines[2]
    #             elif ele == 'tag':
    #                 dict_temp[str(lines[0])] = lines[-1]
    #             else:
    #                 print 'Wrong elements name given!'
    #         dict[ele] = dict_temp
    #     return dict

    def get_data_from_text(self, dir, type, code):

        """get ele from data


        Attributes:
            no.
        """
        temp_files = codecs.open(dir, type, code)
        result = []
        for lines in temp_files:
            dict_temp = {}
            temp = lines.split(',')
            ID = re.search(r'([A-Z])', temp[0]).group(1)
            dict_temp['stock_code'] = temp[3].zfill(6)
            dict_temp['stock_name'] = temp[4].split()[0]
            dict_temp['industry_1_id'] = ID
            dict_temp['industry_1_name'] = temp[0]
            dict_temp['industry_2_id'] = temp[1]
            dict_temp['industry_2_name'] = temp[2]
            result.append(dict_temp)
        return result

    def insert_into_mysql(self, sql):

        """insert into mysql


        Attributes:
            no.
        """
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except Exception,e:
            print e

    # def get_stock_code(self, sql):
    #
    #     """get stock code from mysql
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     try:
    #         self.cur.execute(sql)
    #     except Exception, e:
    #         print e
    #     result = self.cur.fetchall()
    #     return result










