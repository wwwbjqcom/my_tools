# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import struct
import json
import socket
import redis
import time
import threading
import logging
logging.basicConfig(filename='/var/log/monitor/redis.log', level=logging.ERROR)
from rediscluster import StrictRedisCluster

#监控项
status_list = ['used_memory','used_memory_rss','mem_fragmentation_ratio','connected_clients','client_longest_output_list',
                   'client_biggest_input_buf','blocked_clients','instantaneous_ops_per_sec','instantaneous_input_kbps','instantaneous_output_kbps',
                   'role','aof_last_bgrewrite_status','rdb_last_bgsave_status']

#需要监控的节点配置,只需配置每个集群中的一个节点信息即可
host_list = {'10.1.13.11':7000,'10.2.13.11':7000,'10.3.13.11':7000,'10.4.13.11':7000,'10.5.13.11':7000}



# -----zabbix server-----------
zabbix_server = '127.0.0.1'
zabbix_port = 10051

#间隔时间
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


#打印错误日志
def log(response, host, var):
    if int(response['info'].split(';')[1].split(':')[1]) == 1:
        logging.error("host " + host + " " + var + " " + str(response))

#发送数据
def put_status_value(host,variables,value):
    zabbix_sender_1 = zabbix_sender(zabbix_server,zabbix_port)
    zabbix_sender_1.adddata(host, 'redis.'+variables, value)
    response = eval(zabbix_sender_1.send())
    log(response,host,variables)


#创建redis连接
def create_conn(host,port):
    pool = redis.ConnectionPool(host=host, port=port)
    return redis.Redis(connection_pool=pool)

#获取集群中的地址
def get_cluster_nodes(host,port):
    nodes_list = {}
    nodes = [{"host":host,"port":port}]
    rc = StrictRedisCluster(startup_nodes=nodes)
    nodes = rc.cluster_nodes()
    for i in range(len(nodes)):
        nodes_list[nodes[i]['host']] = nodes[i]['port']
    return nodes_list


#组合所有redis节点
def group_all_host():
    all_host_list = {}
    for host in host_list:
        port = host_list[host]
        try:
            single_cluster_nodes = get_cluster_nodes(host,port)
        except:
            logging.error("host :" + host + " get cluster nodes failed")
        all_host_list = dict(all_host_list.items() + single_cluster_nodes.items())
    return all_host_list


def Monitor(host,port):
    try:
        conn = create_conn(host, port)
    except:
        log('conn err',host,'conn')
    while True:
        try:
            info_list = conn.info()
        except:
            time.sleep(2)
            try:
                info_list = conn.info()
            except:
                log('get info error !!!!!', host, 'get info')
                info_list = []
        if len(info_list) > 0:
            for i in info_list:
                if i in status_list:
                    if i == "role" and info_list[i] == "master":
                        put_status_value(host,i,1)
                    elif i == "role" and info_list[i] == "slave":
                        put_status_value(host, i, 0)
                    elif i == 'aof_last_bgrewrite_status' or i == 'rdb_last_bgsave_status':
                        if info_list[i] == 'ok':
                            put_status_value(host, i, 1)
                        else:
                            put_status_value(host, i, 0)
                    else:
                        put_status_value(host, i, info_list[i])
        time.sleep(5)

def Start_Now():
    host_list_all = group_all_host()
    for host in host_list_all:
        thread = threading.Thread(target=Monitor,args=(host,host_list_all[host]))
        thread.start()

if __name__ == "__main__":
    Start_Now()