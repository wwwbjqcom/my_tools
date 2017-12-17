# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
用于接收业务数据传送到zabbix
'''
import threading,socket
import socketserver,json,struct

# -----zabbix server-----------
zabbix_server = '150.242.213.201'
zabbix_port = 10051

listen_port = 9011
encoding = 'utf-8'
BUFSIZE = 1024

import logging
logging.basicConfig(filename='erlang_zabbix.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')

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


class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client
        self.zabbix_sender = zabbix_sender(zabbix_server,zabbix_port)
    def run(self):
        while True:
            data = self.client.recv(BUFSIZE)
            string = eval(bytes.decode(data, encoding))
            for tiem in string:
                self.zabbix_sender.adddata('bigdc', tiem, int(string[tiem]))
                response = eval(self.zabbix_sender.send())
                logging.info('server bigdc send key:{} value:{} status :{}'.format(tiem,string[tiem],response))



class Listener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(0)

    def run(self):
        logging.info("listener started")
        while True:
            client, cltadd = self.sock.accept()
            Reader(client).start()
            logging.info("accept a connect")

if __name__ == "__main__":
    lst = Listener(listen_port)
    lst.start()