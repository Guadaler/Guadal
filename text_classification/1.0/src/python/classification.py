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

import re
import sys
from function_module import FunctionModule
reload(sys)
sys.setdefaultencoding('utf8')

class FinanceClassification(object):

    """industry classification function module


    Attributes:
        no.
    """

    def __init__(self):

        """initiate object


        Attributes:
            no.
        """
        self.sql_get_data = 'SELECT * FROM shangshi_zixun'
        self.sql_get_codes = 'SELECT stock_code, stock_name, industry_1_id FROM stock_codes'
        self.sql_get_keywords = 'SELECT industry_1_id, keywords FROM industry_keywords'
        # self.ele_list = ('title', 'article', 'tag')
        self.control = ('t_title', 'industry_1_id')
        self.func = FunctionModule()

    def _match(self, data, stcodes, args, keywords=None):

        """match function


        Attributes:
            args:four eles, stock code, stock name, title, industry id

        """
        if keywords:
            for line in keywords:
                line['keywords'] = line['keywords'].split(',')
        tag = {}
        for line in data:
            temp_stock = []
            # for code in stcodes:
            #     code_judge = code['stock_code'] in line[args[0]]
            #     name_judge = code['stock_name'] in line[args[0]]
            #     if code_judge or name_judge:
            #         temp_stock.append(code[args[1]])
            temp_keywords = []
            if keywords:
                for item in keywords:
                    for w in item['keywords']:
                        keywords_judge = w in line[args[0]]
                        if keywords_judge:
                            temp_keywords.append(item[args[1]])
            # Tag = self._merge_tags(tags_stock, tags_keywords)
            Tag = list(set(temp_stock + temp_keywords))
            tag[line['i_id']] = Tag
        return tag

    # def _merge_tags(self, *args):
    #
    #     """match function
    #
    #
    #     Attributes:
    #         args:two dict with same keys
    #
    #     """
    #     if len(args) > 1:
    #         new_dict = {}
    #         for k_1, v_1 in args[0].items():
    #             for k_2, v_2 in args[1].items():
    #                 if k_1 == k_2:
    #                     temp_value = v_1 + v_2
    #                     temp_value = list(set(temp_value))
    #             new_dict[k_1] = temp_value
    #         return new_dict
    #     else:
    #         return args




    def main(self):

        """main function


        Attributes:
            no.
        """
        data = self.func.get_data_from_MySQL(self.sql_get_data)
        stock_codes = self.func.get_data_from_MySQL(self.sql_get_codes)
        keywords = self.func.get_data_from_MySQL(self.sql_get_keywords)
        tags = self._match(data, stock_codes,self.control,keywords=keywords)
        for k, v in tags.items():
            if len(v) > 1:
                print k, v
        print tags

        # eles = self.func.get_eles_from_data(data, self.ele_list)
        # tags = self._match(eles['title'],stock_codes)

if __name__ == '__main__':
    classification = FinanceClassification()
    classification.main()






