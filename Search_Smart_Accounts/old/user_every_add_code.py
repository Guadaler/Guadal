#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# user_day_value
# $Id: user_every_add_code.py  2015-10-29 Zhangruibo $
#

# --------------------------------------------------------------------
# user_every_add_code.py is
#
#get every user  new code  in every day
#
# --------------------------------------------------------------------

"""
user_every_add_code.py

"""
import redis
import MySQLdb
import re
REDIS2 = redis.StrictRedis(host='192.168.0.2', port=6379,
                           db=5, password='kunyandata')
HOURS = REDIS2.keys()
HOURS = sorted(HOURS)
CONN = MySQLdb.connect(host='localhost', user='root',
                       passwd='mysql', db='mysql',
                       port=3306)
CUR = CONN.cursor()
SQL = 'create table user_everyday_add_code(' \
      'user varchar(255) ,' \
      'date varchar(255),' \
      'code varchar(255)' \
      ')'
CUR.execute(SQL)   #记录每一个USER每天新增股票到库里
LEN_HOURS = len(HOURS)
def hanshu(user_inner, time, time_next):
    """Set hanshu to get  every user  new code  in every day and insert to mysql

        Args:
            no
    """
    hour_now = REDIS2.hgetall(HOURS[time])
    hour_next = REDIS2.hgetall(HOURS[time_next])
    if re.search(user_inner, str(hour_next)):
        time = time_next
        code = re.findall(r'\d+', str(hour_next[user_inner]))
        code_z = re.findall(r'\d+', str(hour_now[user_inner]))
        count = []
        for len_code in code:
            count.append(len_code)
        len_data = len(count)
        data_len = 0
        countif = 0
        while data_len < len_data:
            if re.search(count[data_len], str(code_z)):
                countif = countif + 0
            else:
                countif = countif + 1
                date = re.findall(r'\d+', HOURS[time_next])
                date = date[0] + date[1] + date[2]
                code_new = count[data_len]
                CUR.execute('insert into  '
                            'user_everyday_add_code '
                            'values("%s", "%s","%s")'
                            % (user_inner, date, code_new))
            data_len = data_len + 1
        while time_next+1 == LEN_HOURS:
            break
        else:
            time_next = time_next+1
            hanshu(user_inner, time, time_next)
    else:
        while time_next+1 == LEN_HOURS:
            break
        else:
            time_next = time_next+1
            hanshu(user_inner, time, time_next)
COUNT = 0
COUNT_HOUR = 0
USER = []
for hour in HOURS:
    data = REDIS2.hgetall(hour)
    for user_data in data:
        USER.append(user_data)
USER_STRING = ''
USER_HOUR = []
for user_len in USER:
    if re.search(user_len, USER_STRING):
        pass
    else:
        USER_STRING = USER_STRING + user_len
        USER_HOUR.append(user_len)
while COUNT < len(USER_HOUR):
    while COUNT_HOUR + 1 != LEN_HOURS:
        if re.search(USER_HOUR[COUNT], str(REDIS2.hgetall(HOURS[COUNT_HOUR]))):
            hanshu(USER_HOUR[COUNT], COUNT_HOUR, COUNT_HOUR+1)
            break
        else:
            COUNT_HOUR = COUNT_HOUR + 1
    COUNT = COUNT + 1
CONN.commit()
CONN.close()












