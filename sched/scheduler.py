# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

from apscheduler.schedulers.blocking import BlockingScheduler
import ConfigParser,pymysql,traceback
import logging
logging.basicConfig(filename='sche.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')

class job_meteta:
    mysql_user = None
    mysql_passwd = None

class GetConfig:
    def __init__(self):
        path = 'sche.conf'
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(path)

    def get_sections(self):
        return self.conf.sections()

    def get_mysql_itmes(self):
        job_meteta.mysql_user = self.conf.get('mysql','user')
        job_meteta.mysql_passwd = self.conf.get('mysql','passwd')

    def get_times(self,section):
        _tmp = {}
        for value in self.conf.items(section):
            _tmp[value[0]] = value[1]
        return _tmp

class Mysql:
    def __init__(self,mysql_host,mysql_user,mysql_password,mysql_port,db,sql):
        self.sql = sql
        self.conn = pymysql.connect(host=mysql_host,
                                              user=mysql_user,
                                              password=mysql_password, port=mysql_port,
                                              db=db,
                                              charset='utf8mb4',
                                              cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()

    def __enter__(self):
        try:
            self.cur.execute(self.sql)
            self.conn.commit()
        except pymysql.Error:
            logging.error(traceback.format_exc())
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

def job_items(items,type,section,kwargs):
    __agrus = {}
    __agrus['id'] = section
    __agrus['name'] = section
    __agrus['kwargs'] = kwargs
    if type == 'cron':
        __agrus['year'] = items['year'] if 'year' in items else None
        __agrus['month'] = items['month'] if 'month' in items else None
        __agrus['day'] = items['day'] if 'day' in items else None
        __agrus['week'] = items['week'] if 'week' in items else None
        __agrus['day_of_week'] = items['day_of_week'] if 'day_of_week' in items else None
        __agrus['hour'] = items['hour'] if 'hour' in items else None
        __agrus['minute'] = items['minute'] if 'minute' in items else None
        __agrus['second'] = items['second'] if 'second' in items else None
        __agrus['start_date'] = items['start_date'] if 'start_date' in items else None
        __agrus['end_date'] = items['end_date'] if 'end_date' in items else None
        __agrus['timezone'] = items['timezone'] if 'timezone' in items else None
    elif type == 'date':
        __agrus['run_date'] = items['run_date']
        __agrus['timezone'] = items['timezone'] if 'timezone' in items else None
    elif type == 'interval':
        if 'weeks' in items:
            __agrus['weeks'] = int(items['weeks'])
        elif 'days' in items:
            __agrus['days'] = int(items['days'])
        elif 'hours' in items:
            __agrus['hours'] = int(items['hours'])
        elif 'minutes' in items:
            __agrus['minutes'] = int(items['minutes'])
        elif 'seconds' in items:
            __agrus['seconds'] = int(items['seconds'])
        __agrus['start_date'] = items['start_date'] if 'start_date' in items else None
        __agrus['end_date'] = items['end_date'] if 'end_date' in items else None
        __agrus['timezone'] = items['timezone'] if 'timezone' in items else None
    else:
        logging.error('type [{}] error, type must in (cron,date,interval)'.format(type))
    return __agrus




def job_func(mysql_host=None,mysql_port=None,mysql_db=None,mysql_sql=None,type=None,sys_cmd=None):
    if type == 'mysql':
        with Mysql(mysql_host=mysql_host, mysql_port=mysql_port, mysql_user=job_meteta.mysql_user,
                  mysql_password=job_meteta.mysql_passwd, db=mysql_db, sql=mysql_sql) as mysql:
            pass
    elif type == 'system':
        import os
        os.popen(sys_cmd)
    else:
        logging.error(msg='type [{}] is error'.format(type))


def job_main():
    sched = BlockingScheduler()
    get_config = GetConfig()
    sections = get_config.get_sections()
    for section in sections:
        if section == "mysql":
            get_config.get_mysql_itmes()
        else:
            items = get_config.get_times(section)
            if items['type'] == 'mysql':
                _kwargs = {'type': items['type'], 'mysql_host':items['host'],'mysql_port':items['port'],'mysql_db':items['db'],'mysql_sql':items['sql']}
                try:
                    if items['sche_type'] == 'cron':
                        sched.add_job(job_func,'cron',**job_items(items,'cron',section,_kwargs))
                    elif items['sche_type'] == 'date':
                        sched.add_job(job_func, 'date', **job_items(items,'date',section,_kwargs))
                    elif items['sche_type'] == 'interval':
                        sched.add_job(job_func,'interval',**job_items(items,'interval',section,_kwargs))
                except:
                    logging.error(traceback.format_exc())
            elif items['type'] == 'system':
                _kwargs = {'type':items['type'],'sys_cmd':items['cmd']}
                try:
                    if items['sche_type'] == 'cron':
                        sched.add_job(job_func,'cron',**job_items(items,'cron',section,_kwargs))
                    elif items['sche_type'] == 'date':
                        sched.add_job(job_func, 'date', **job_items(items,'date',section,_kwargs))
                    elif items['sche_type'] == 'interval':
                        sched.add_job(job_func,'interval',**job_items(items,'interval',section,_kwargs))
                except:
                    logging.error(traceback.format_exc())
    sched.start()

if __name__ == "__main__":
    job_main()


