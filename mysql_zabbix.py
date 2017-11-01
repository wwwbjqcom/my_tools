#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
''''''
'''
import traceback
import struct
import json
import socket
import MySQLdb
import time
import threading
import logging

logging.basicConfig(filename='/var/log/monitor/mysql.log', level=logging.ERROR)

# -----mysql-------------------
mysql_user = ''
mysql_password = ''
host_tk = {'10.1.8.72': 3306}
host_us = {}
host_mm = {}
host_flkf = {}
host_hk = {}

host_list = [host_tk, host_us, host_mm, host_flkf, host_hk]

# -----zabbix server-----------
zabbix_server = '127.0.0.1'
zabbix_port = 10051

# ------------------------------
sleep_time = 5


class zabbix_sender:
    def __init__(self, zbx_server_host, zbx_server_port):
        self.zbx_server_host = zbx_server_host
        self.zbx_server_port = zbx_server_port
        self.zbx_header = 'ZBXD'
        self.zbx_protocols_version = 1
        self.zbx_send_value = {'request': 'sender data', 'data': []}

    def adddata(self, host, key, value):
        add_data = {'host': host, 'key': key, 'value': value}
        self.zbx_send_value['data'].append(add_data)

    # 按照协议封装数据包
    def makesenddata(self):
        zbx_send_json = json.dumps(self.zbx_send_value)
        zbx_send_json_len = len(zbx_send_json)
        self.zbx_send_data = struct.pack("<4sBq" + str(zbx_send_json_len) + "s", 'ZBXD', 1, zbx_send_json_len,
                                         zbx_send_json)

    def send(self):
        self.makesenddata()
        zbx_server_socket = socket.socket()
        zbx_server_socket.connect((self.zbx_server_host, self.zbx_server_port))
        zbx_server_write_df = zbx_server_socket.makefile('wb')
        zbx_server_write_df.write(self.zbx_send_data)
        zbx_server_write_df.close()
        zbx_server_read_df = zbx_server_socket.makefile('rb')
        zbx_response_package = zbx_server_read_df.read()
        zbx_server_read_df.close()
        # 按照协议解数据包
        zbx_response_data = struct.unpack("<4sBq" + str(len(zbx_response_package) - struct.calcsize("<4sBq")) + "s",
                                          zbx_response_package)
        return zbx_response_data[3]


def log(response, host, var):
    if int(response['info'].split(';')[1].split(':')[1]) == 1:
        logging.error("host " + host + " " + var + " " + str(response))


def slave_state(slave_result):
    slave_status = {'io_thread': None, 'sql_thread': None,'seconds_behind': 0}
    if len(slave_result) > 0:
        for i in range(len(slave_result)):
            if slave_result[i][10] == 'Yes':
                if slave_status['io_thread'] is not None and slave_status['io_thread'] == 0:
                    slave_status['io_thread'] = 0
                elif slave_status['io_thread'] == 1 or slave_status['io_thread'] is None:
                    slave_status['io_thread'] = 1
            else:
                slave_status['io_thread'] = 0
            if slave_result[i][11] == 'Yes':
                if slave_status['sql_thread'] is not None and slave_status['sql_thread'] == 0:
                    slave_status['sql_thread'] = 0
                elif slave_status['sql_thread'] == 1 or slave_status['sql_thread'] is None:
                    slave_status['sql_thread'] = 1
            else:
                slave_status['sql_thread'] = 0
            if slave_result[i][32] > slave_status['seconds_behind']:
                slave_status['seconds_behind'] = slave_result[i][32]
    else:
        slave_status['io_thread'], slave_status['sql_thread'] = 2, 2
    return slave_status


class MySQL:
    def __init__(self, mysql_host, mysql_port, mysql_user, mysql_passwd, server_name):
        local_conn = MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_passwd, port=mysql_port, db='',
                                     charset="utf8")
        self.server_name = server_name
        self.mysql_cur = local_conn.cursor()
        self.Variables = ["Com_insert", "Com_update", "Com_delete", "Com_select", "Questions",
                          "Innodb_row_lock_current_waits", "Innodb_row_lock_time",
                          "Created_tmp_disk_tables", "Created_tmp_tables", "Innodb_buffer_pool_reads",
                          "Innodb_buffer_pool_read_requests", "Handler_read_first",
                          "Handler_read_key", "Handler_read_next", "Handler_read_prev", "Handler_read_rnd",
                          "Handler_read_rnd_next", "Innodb_os_log_pending_fsyncs",
                          "Innodb_os_log_pending_writes", "Innodb_log_waits", "Threads_connected", "Threads_running",
                          "Bytes_sent", "Bytes_received"]

    def __enter__(self):
        old_struct = {}
        global sleep_time
        sql = 'show global status'
        if len(old_struct) <= 0:
            self.mysql_cur.execute(sql)
            result = self.mysql_cur.fetchall()
            for _i in range(len(result)):
                for _k in self.Variables:
                    if result[_i][0] == _k:
                        value = (int(result[_i][1]))
                        old_struct[_k] = value
        while True:
            self.mysql_cur.execute(sql)
            result = self.mysql_cur.fetchall()
            try:
                zabbix_sender_1 = zabbix_sender(zabbix_server, zabbix_port)
            except Exception, e:
                logging.error(traceback.format_exc())
                logging.error("zabbix server is not running")

            for i in range(len(result)):
                for k in self.Variables:
                    if result[i][0] == k:
                        if k != "Threads_connected" and k != "Threads_running":
                            value = ((int(result[i][1])) - old_struct[k]) / sleep_time
                            old_struct[k] = (int(result[i][1]))
                            zabbix_sender_1.adddata(self.server_name, 'mysql.' + k, value)
                            response = eval(zabbix_sender_1.send())
                            log(response, self.server_name, k)

                        else:
                            value = (int(result[i][1]))
                            zabbix_sender_1.adddata(self.server_name, 'mysql.' + k, value)
                            response = eval(zabbix_sender_1.send())
                            log(response, self.server_name, k)
            # 同步状态
            slave_sql = 'show slave status'
            self.mysql_cur.execute(slave_sql)
            slave_result = self.mysql_cur.fetchall()
            slave_status = slave_state(slave_result)
            for var in slave_status:
                zabbix_sender_1.adddata(self.server_name, 'mysql.' + var, slave_status[var])
                response = eval(zabbix_sender_1.send())
                log(response, self.server_name, var)

            zabbix_sender_1.adddata(self.server_name, 'mysql.status', 1)
            response = eval(zabbix_sender_1.send())
            log(response, self.server_name, 'mysql.status')

            time.sleep(sleep_time)

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.error(exc_tb)


def start(host, port, agent_server):
    while True:
        try:
            with  MySQL(host, port, mysql_user, mysql_password, agent_server) as a:
                pass
        except Exception, e:
            logging.error(traceback.format_exc())
            try:
                zabbix_sender_1 = zabbix_sender(zabbix_server, zabbix_port)
                zabbix_sender_1.adddata(host, 'mysql.status', 0)
                response = eval(zabbix_sender_1.send())
                if int(response['info'].split(';')[1].split(':')[1]) == 1:
                    logging.error("host " + host + " mysql.status " + str(response))
                with  MySQL(host, port, mysql_user, mysql_password, agent_server) as a:
                    pass
            except Exception, e:
                logging.error(traceback.format_exc())
                logging.error("zabbix server is not running")


def thread_pool():
    for i in range(len(host_list)):
        host_struct = host_list[i]
        for host in host_struct:
            thread = threading.Thread(target=start, args=(host, host_struct[host], host))
            thread.start()


if __name__ == '__main__':
    thread_pool()
