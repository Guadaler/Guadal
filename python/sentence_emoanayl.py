#!/usr/bin/python
#coding: utf-8
#
# backup_remote_data_files
# $Id: backup_remote_data_files.py  2015-09-30 Qiu $
#
# history:
# 2015-10-19 Qiu   created
#
# wangQiu@kunyandata.com
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
sent emoanal


"""

import jieba
import MySQLdb
import codecs
import numpy
import sys
reload(sys)
sys.setdefaultencoding('utf8')

class Sent_emoanal(object):

    """sent emoanal.


    Attributes:
        no.
    """

    def __init__(self):

        """initiate object


        Attributes:
            no.
        """
        try:
            self.conn = MySQLdb.connect(host='127.0.0.1', user='root',
                                        passwd='root', db='stock',
                                        charset='utf8')
        except Exception, e:
            print e
        self.cur = self.conn.cursor()
        self.word_seg = jieba.lcut
        self.dir = 'D:/dicts/'


    # def _judge_odd(self, num):
    #
    #     """judge if it's an odd?
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     if (num/2)*2 == num:
    #         return 'even'
    #     else:
    #         return 'odd'

    def get_text_from_MySQL(self):

        """get text from Mysql


        Attributes:
            no.
        """
        try:
            self.cur.execute("SELECT i_id, t_article FROM finance_news")
        except Exception, e:
            print e
        temp = self.cur.fetchall()
        result_id = []
        result_article = []
        for line in temp:
            result_id.append(line[0])
            result_article.append(line[1])
        self.cur.close()
        self.conn.close()
        return [result_id, result_article]

    # def _word_seg(self, sent):
    #
    #     """sentence segmentation
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     seg_list = jieba.cut(sent)
    #     seg_result = ' '.join(seg_list)
    #     return seg_result

    # def _word_pos_tag(self, sent):
    #
    #     """sentence pos tagging
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     pos_data2 = jieba.posseg.cut(sent)
    #     pos_list2 = []
    #     for w2 in pos_data2:
    #         pos_list2.extend([w2.word.encode('utf8'), w2.flag])
    #     pos_str = ' '.join(pos_list2)
    #     return pos_str

    def _sent_seg(self, paragraph):

        """sentences segmentation


        Attributes:
            no.
        """
        punt_list = '：，。！？；…'.decode('utf8')
        for punt in punt_list:
            paragraph = paragraph.replace(punt, u'\u3000')
        result = paragraph.split(u'\u3000')
        return result

    def _paragraph_seg(self, article):

        """paragraph segmentation


        Attributes:
            no.
        """
        temp = article.split(u'\u3000'u'\u3000')
        result = []
        for line in temp:
            if len(line):
                result.append(line)
        return result

    def get_dicts(self, dir):

        """get posdict, negdict, mostdict
               moredict, ishdict, insuffdict
               and inversedict


        Attributes:
            no.
        """
        dict_name = ['posdict', 'negdict', 'mostdict', 'verydict', 'moredict',
                     'ishdict', 'insuffdict', 'inversedict']
        dict = []
        for name in dict_name:
            temp = codecs.open(dir+name, 'r', 'utf8')
            temps = []
            for word in temp.readlines():
                temps.append(word.replace('\r\n', ''))
            dict.append(temps)
            temp.close()
        return dict

    def _compute_sent_socre(self, word, dict, score):

        """sentiment score


        Attributes:
            no.
        """
        if word in dict[2]:
            score *= 2.0
        elif word in dict[3]:
            score *= 1.5
        elif word in dict[4]:
            score *= 1.25
        elif word in dict[5]:
            score *= 0.5
        elif word in dict[6]:
            score *= 0.25
        elif word in dict[7]:
            score *= -1
        return score

    def _transfrom_scores(self, p, n):

        if p < 0 and n >= 0:
            neg_count = n - p
            pos_count = 0
        elif n < 0 and p >= 0:
            pos_count = p - n
            neg_count = 0
        elif p < 0 and n < 0:
            neg_count = -p
            pos_count = -n
        else:
            pos_count = p
            neg_count = n
        if pos_count == 1:
            pos_count = 0
        if neg_count == 1:
            neg_count = 0
        return [pos_count, neg_count]

    # def _final_score(self, list, args):
    #
    #     """sentiment score
    #
    #
    #     Attributes:
    #         no.
    #     """
    #     if args == 'cents':
    #         result = list[0][0][0]


    def sentiment_score(self, article, dict):

        """sentiment score


        Attributes:
            no.
        """
        count_sents = []
        count_paras = []
        count_articles = []
        for text in article[1]:
            paras = self._paragraph_seg(text)
            for para in paras:
                sents = self._sent_seg(para)
                for sent in sents:
                    words = self.word_seg(sent)
                    i = 0  # index of counter
                    s = 0  # index of emo word
                    pos_score = 1
                    neg_score = 1
                    for word in words:
                        if word in dict[0]:
                            for w in words[s:i]:
                                pos_score = self._compute_sent_socre(w, dict, pos_score)
                            s = i + 1
                        elif word in dict[1]:
                            for w in words[s:i]:
                                neg_score = self._compute_sent_socre(w, dict, neg_score)
                            s = i + 1
                        i += 1
                    count_sents.append(self._transfrom_scores(pos_score, neg_score))
                count_paras.append(count_sents)
                count_sents = []
            count_articles.append(count_paras)
            count_paras = []
        # result = self._regroup_article(article[0], count_articles)
        #　return result
        return count_articles

    def _regroup_article(self, id, data):

        """regroup data


        Attributes:
            no.
        """
        temp = zip(id, data)
        dic = {}
        for line in temp:
            dic[str(line[0])] = line[1]
        return dic

    def _statistic(self, array):

        """statistic computing


        Attributes:
            no.
        """
        np_temp = numpy.array(array)
        pos_mean = numpy.mean(np_temp[:, 0])
        neg_mean = numpy.mean(np_temp[:, 1])
        pos_std = numpy.std(np_temp[:, 0])
        neg_std = numpy.std(np_temp[:, 1])
        pos_sum = numpy.sum(np_temp[:, 0])
        neg_sum = numpy.sum(np_temp[:, 1])

        return {'sum':[pos_sum, neg_sum], 'mean':[pos_mean, neg_mean],
                'std':[pos_std, neg_std]}

    def compute_stat(self, array, arg):

        """stat


        Attributes:
            no.
        """
        paras_stat = []
        articles_stat = []
        paras_stat_temp = []
        articles_stat_temp = []
        for i in array:
            for j in i:
                x = self._statistic(j)
                paras_stat_temp.append(x)
                articles_stat_temp.append(x['sum'])
            articles_stat.append(self._statistic(articles_stat_temp))
            paras_stat.append(paras_stat_temp)
            paras_stat_temp = []
        if arg == 'paras':
            return paras_stat
        elif arg == 'articles':
            return articles_stat
        else:
            print "invald arg.(it should be 'paras' or 'article')"

    def main(self):

        """main function


        Attributes:
            no.
        """
        articles = self.get_text_from_MySQL()
        # articles = ['我爱北京天安门']
        dicts = self.get_dicts(self.dir)
        result = self.sentiment_score(articles, dicts)
        stat = self.compute_stat(result, 'articles')
        data = self._regroup_article(articles[0], stat)
        print data


if __name__ == '__main__':
    Emo_analysis = Sent_emoanal()
    Emo_analysis.main()



