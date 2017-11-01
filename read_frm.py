# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import struct,getopt,sys

class ha_key_alg:
  HA_KEY_ALG_UNDEF=	0		#/* Not specified (old file) */
  HA_KEY_ALG_BTREE=	1		#/* B-tree, default one          */
  HA_KEY_ALG_RTREE=	2		#/* R-tree, for spatial searches */
  HA_KEY_ALG_HASH=	3		#/* HASH keys (HEAP tables) */
  HA_KEY_ALG_FULLTEXT=	4		#/* FULLTEXT (MyISAM tables) */

class column_type_dict:
    MYSQL_TYPE_DECIMAL=0
    MYSQL_TYPE_TINY=1
    MYSQL_TYPE_SHORT=2
    MYSQL_TYPE_LONG=3
    MYSQL_TYPE_FLOAT=4
    MYSQL_TYPE_DOUBLE=5
    MYSQL_TYPE_NULL=6
    MYSQL_TYPE_TIMESTAMP=7
    MYSQL_TYPE_LONGLONG=8
    MYSQL_TYPE_INT24=9
    MYSQL_TYPE_DATE=10
    MYSQL_TYPE_TIME=11
    MYSQL_TYPE_DATETIME=12
    MYSQL_TYPE_YEAR=13
    MYSQL_TYPE_NEWDATE=14
    MYSQL_TYPE_VARCHAR=15
    MYSQL_TYPE_BIT=16
    MYSQL_TYPE_TIMESTAMP2=17
    MYSQL_TYPE_DATETIME2=18
    MYSQL_TYPE_TIME2=19
    MYSQL_TYPE_JSON=245
    MYSQL_TYPE_NEWDECIMAL=246
    MYSQL_TYPE_ENUM=247
    MYSQL_TYPE_SET=248
    MYSQL_TYPE_TINY_BLOB=249
    MYSQL_TYPE_MEDIUM_BLOB=250
    MYSQL_TYPE_LONG_BLOB=251
    MYSQL_TYPE_BLOB=252
    MYSQL_TYPE_VAR_STRING=253
    MYSQL_TYPE_STRING=254
    MYSQL_TYPE_GEOMETRY=255

legacy_db_type = {
    0:'UNKNOWN',
    1:'DIAB_ISAM',
    2:'HASH',
    3:'MISAM',
    4:'PISAM',
    5:'RMS_ISAM',
    6:'HEAP',
    7:'ISAM',
    8:'MRG_ISAM',
    9:'MYISAM',
    10:'MRG_MYISAM',
    11:'BERKELEY_DB',
    12:'INNODB',
    13:'GEMINI',
    14:'NDBCLUSTER',
    15:'EXAMPLE_DB',
    16:'ARCHIVE_DB',
    17:'CSV_DB',
    18:'FEDERATED_DB',
    19:'BLACKHOLE_DB',
    20:'PARTITION_DB',
    21:'BINLOG',
    22:'SOLID',
    23:'PBXT',
    24:'TABLE_FUNCTION',
    25:'MEMCACHE',
    26:'FALCON',
    27:'MARIA',
    28:'PERFORMANCE_SCHEMA',
    42:'FIRST_DYNAMIC',
    127:'DEFAULT'
    }


charset_collate = {33:'utf8_general_ci',56:'utf8_tolower_ci',223:'utf8_general_mysql500_ci',83:'utf8_bin',254:'utf8_general_cs',
                28:'gbk_chinese_ci',87:'gbk_bin',8:'latin1_swedish_ci',31:'latin1_german2_ci',47:'latin1_bin',45:'utf8mb4_general_ci',
                46:'utf8mb4_bin',224:'utf8mb4_unicode_ci',192:'utf8_unicode_ci'}

charset_value = {33:'utf8',56:'utf8',223:'utf8',83:'utf8',254:'utf8',28:'gbk',87:'gbk',8:'latin1',
                 31:'latin1',47:'latin1',45:'utf8mb4',46:'utf8mb4',224:'utf8mb4',192:'utf8'}

charset_bytes = {33:3,56:3,223:3,83:3,254:3,28:2,87:2,8:1,
                 31:1,47:1,45:3,46:3,224:3,192:3}

class filedata:
    file = None

class headerconst:
    io_size = None
    db_type = None
    pos_names = None
    pos_length = None
    tmp_key_length = None
    reclength = None
    key_info_length = None
    charset = None
    key_length = None
    key_block_size = None
    extra_rec_buf_length = None
    field_pack_length = 17

class Const:
    GENERATED_FIELD = 128
    FRM_GCOL_HEADER_SIZE = 4
    FIELDFLAG_DEC_SHIFT = 8
    FIELDFLAG_MAX_DEC = 31
    FIELDFLAG_DECIMAL = 1
    DECIMAL_MAX_PRECISION = 65
    HA_NOSAME = 1
    PART_KEY_FLAG = 16384


class GetHeader:
    """
    position,bytes(64字节的header)
	0,1: 固定值254
	1,1: 固定值1
	2,1: FRM_VER+3+MY_TEST(create_info->varchar) frm_ver表示frm文件版本号，在mysql_version.h.in中，最后是判断该表中时候包含varchar，包含为1否则为0
	3,1: (uchar) ha_legacy_type(ha_checktype(thd,ha_legacy_type(create_info->db_type),0,0)) 引擎类型
	4,1: 固定值1
	5,1: 0
	6,2: IO_size=4096
	8,2: 固定值
	10,4: Length, based on key_length + rec_length + create_info->extra_size
	14,2: tmp_key_length
	16,2: reclength
	18,4: max_rows
	22,4: min_rows
	26,1: 0
	27,1: 固定值2
	28,2: key_info_length
	30,2: blob指针，create_info->table_options
	32,1: 固定值0
	33,1: 固定值5 代表frm版本为5.0
	34,4: create_info->avg_row_length  行平均长度
	38,1: 表默认字符集编号
	39,1: 固定值0
	40,1: create_info->row_type
	41,6: 这6个字节记录RAID支持
	47,4: key_length   用于偏移读取默认值信息,默认值其实位置在io_size+key_length
	51,4: mysql版本号
	55,4: create_info->extra_size
	59,2: extra_rec_buf_length
	61,1: default_part_db_type 分区表类型
	62,2: create_info->key_block_size
    """
    def __init__(self):
        self.__get_header()
        #self.__test()
    def __get_header(self):
        pack = filedata.file.read(64)
        headerconst.io_size, = struct.unpack('<H',pack[6:8])
        headerconst.db_type, = struct.unpack('<B',pack[3:4])
        headerconst.charset, = struct.unpack('<B',pack[38:39])
        headerconst.key_block_size, = struct.unpack('<H',pack[62:64])
        headerconst.key_info_length, = struct.unpack('<H',pack[28:30])
        headerconst.pos_length, = struct.unpack('<H',pack[4:6])
        headerconst.pos_names, = struct.unpack('<H',pack[8:10])
        headerconst.tmp_key_length, = struct.unpack('<H',pack[14:16])
        headerconst.reclength, = struct.unpack('<H',pack[16:18])
        headerconst.key_length, = struct.unpack('<I',pack[47:51])
        headerconst.extra_rec_buf_length, = struct.unpack('<H',pack[59:61])

    def __test(self):
        offset = headerconst.io_size + (headerconst.key_length if
                                        headerconst.tmp_key_length == b'\xff\xff' else headerconst.tmp_key_length)
        filedata.file.seek(offset)
        print struct.unpack('{}s'.format(headerconst.reclength),filedata.file.read(headerconst.reclength))

class GetKey:
    """
    6bytes: 记录索引基本信息
        0,1:1bytes    索引数
        1,1: 1bytes  用于索引的字段数
        4,2: 2bytes 索引信息结束后存储索引名的字节长度
    8bytes: 记录单个索引信息
        0,2: flags   1代表唯一有唯一约束，普通为0
        2,2: 索引总长度
        4,1: 索引字段数,有多少字段，后面就有多少个9bytes
        5,1: 索引类型
        6,2: block_size
        9bytes: 记录所有单个字段信息
            0,1: 字段在表中的编号
            2,2: record字节偏移量
            5,2: 字段类型
            7,2: 字段长度
        后面紧跟索引名：
            ff+索引名
    :return: 
    """
    def __init__(self):
        GetHeader()
        filedata.file.seek(headerconst.io_size)
        self.FIELD_NR_MASK = 16383
        self.HA_NOSAME = 1
        self.keys_length = 8
        self.key_parts_length = 9
        self.keys_list = []
        self.key_names_list = []
        self.key_algorithm = []
        self.keys, self.key_parts, self.key_names_length = None,None,None

    def __get_key_info(self,pack):
        """获取索引基本信息"""
        self.keys, self.key_parts, _, self.key_names_length = struct.unpack('=BB2sH', pack)
        self.__get_keys()

    def __get_keys(self):
        for i in range(self.keys): #循环索引
            key_columns = []
            flags,_,columns,algorithm,_ = struct.unpack('=HHBB2s',filedata.file.read(self.keys_length))
            self.key_algorithm.append(algorithm)
            for k in range(columns): #循环索引字段
                file_num,_,record_offset,_,column_type,column_length = struct.unpack('=BcHcHH',filedata.file.read(self.key_parts_length))
                key_columns.append({'file_num':file_num & self.FIELD_NR_MASK,'record_offset':record_offset-1,'column_type':column_type,
                             'column_length':column_length,'flags':flags ^ Const.HA_NOSAME})
            self.keys_list.append(key_columns)

        self.__get_key_names()

    def __get_key_names(self):
        """循环获取索引名称"""
        pack = filedata.file.read(self.key_names_length)
        _tmp = struct.pack('b',0)
        for index,a in enumerate(pack):
            if a == b'\xff' and index != 0:
                self.key_names_list.append(struct.unpack('b{}s'.format(len(_tmp)-1),_tmp)[1])
                _tmp = struct.pack('b', 0)
            elif a != b'\xff':
                _tmp += a

    def GetKey(self):
        self.__get_key_info(pack=filedata.file.read(6))
        columns_list, column_names_list, table_comment_str = GetColumn().GetInfo()
        return columns_list, column_names_list, table_comment_str,self.keys_list,self.key_names_list,self.key_algorithm
        #print  self.keys_list,self.key_names_list,self.key_algorithm


class Unit:
    def __init__(self):
        pass
    def f_decimals(self,x):
        return (x >> Const.FIELDFLAG_DEC_SHIFT) & Const.FIELDFLAG_MAX_DEC
    def my_decimal_precision_to_length_no_truncation(self,precision,scale,flag):
        retval = precision + (1 if scale > 0 else 0) + (0 if flag or not precision else 1)
        return retval
    def read_int24_be(self,pack):
        a, b, c = struct.unpack('BBB', pack)
        res = (a << 16) | (b << 8) | c
        if res >= 0x800000:
            res -= 0x1000000
        return res

class GetColumn(Unit):
    """
    288bytes基本信息
        46,1: 表注释所占长度，后面紧跟注释内容
        258,2: 两个字节记录字段数
        260,2: 字段基本信息长度
        268,2: 所有字段名长度，每个字段名以ff隔开
        284,2: 字段注释所占长度
    288字节后紧跟字段信息，格式排列顺序：
        content_pos: 基础信息
        fields*17 : 字段信息
        n_length: 字段名总长度
        int_length : 紧跟字段之后，为enum、set类型的值
        com_length: 字段注释长度
        gcol_screen: 虚拟列信息
            0,1: 固定值1
            1,3: 虚拟列信息长度
            3,0: 是否存储数据
            剩余的为信息
            如有多个虚拟列，循环上面的字节排序
    """
    def __init__(self):
        self.column_names_list = []
        self.columns_list = []

        filedata.file.seek(64)
        pack = filedata.file.read(headerconst.pos_length + (headerconst.pos_names * 4))
        _,self.pos = struct.unpack('={}si'.format(headerconst.pos_length),pack)
        filedata.file.seek(self.pos)
        self.forminfo = filedata.file.read(288) #288个字节基本信息
        self.fields, = struct.unpack('<H', self.forminfo[258:260])  # 字段数
        self.content_pos, = struct.unpack('<H',self.forminfo[260:262]) #基础信息字节长度
        self.n_length, = struct.unpack('<H',self.forminfo[268:270]) #所有字段名记录长度
        self.interval_count,self.interval_parts,self.int_length = struct.unpack('<hhh',self.forminfo[270:276]) #分表表示enum、set类型的字段数、多少个part（ff分割也就是多少个ff）、总长度
        self.null_fields,self.com_length,self.gcol_screen_length = struct.unpack('<hhh',self.forminfo[282:288]) #null_fields 允许为空的字段数，com_length 字段注释长度，gcol_screen_length 虚拟列所占长度

    def __get_column_info(self):
        table_comment_length, = struct.unpack('<B',self.forminfo[46])
        table_comment_str, = struct.unpack('{}s'.format(table_comment_length),self.forminfo[47:47+table_comment_length])
        read_length = self.fields * headerconst.field_pack_length + self.content_pos + (self.n_length + self.int_length + self.com_length + self.gcol_screen_length)
        filedata.file.seek(self.pos+288)
        _pack = filedata.file.read(read_length)

        """循环获取字段名"""
        offset = self.content_pos + (self.fields * headerconst.field_pack_length)
        stop = self.content_pos + (self.fields * headerconst.field_pack_length)+ self.n_length# + self.int_length
        _tmp_column_name_pack = _pack[offset:stop]
        _tmp = struct.pack('b',0)
        for index,a in enumerate(_tmp_column_name_pack):
            if a == b'\xff' and index != 0:
                self.column_names_list.append(struct.unpack('b{}s'.format(len(_tmp)-1),_tmp)[1])
                _tmp = struct.pack('b', 0)
            elif a != b'\xff':
                _tmp += a

        """循环获取字段信息"""
        self.__get_column()
        return table_comment_str

    def __get_column(self):
        """
        17bytes记录字段信息：
            3,5: 字段类型
            5,8: 记录偏移量
            8,10: flags 
            10,11: 是否为虚拟列
            12,13: enum、set类型的顺序号
            13,14: 字段类型
            14,15: 字段字符类型
            15,17: 字段注释长度
        :return: 
        """
        filedata.file.seek(self.pos + 288 + self.content_pos)
        _tmp_columns_list = []
        for i in range(self.fields):
            _tmp_pack = filedata.file.read(headerconst.field_pack_length)
            field_length, = struct.unpack('<H', _tmp_pack[3:5])
            recpos = self.read_int24_be(_tmp_pack[5:8])
            unireg_type, = struct.unpack('<B',_tmp_pack[10])
            interval_nr, = struct.unpack('<B',_tmp_pack[12])
            charset_type, = struct.unpack('<B', _tmp_pack[14])
            field_type, = struct.unpack('<B', _tmp_pack[13])
            comment_length, = struct.unpack('<H', _tmp_pack[15:17])
            flags, = struct.unpack('<H',_tmp_pack[8:10])
            _tmp_columns_list.append({'field_length':field_length,'charset_type':charset_type,'field_type':field_type,
                                      'comment_length':comment_length,'flags':flags | Const.PART_KEY_FLAG,'interval_nr':interval_nr,'recpos':recpos,'unireg_type':unireg_type})
            #print struct.unpack('<B',_tmp_pack[9]),struct.unpack('<B',_tmp_pack[11]),struct.unpack('<B',_tmp_pack[10]),struct.unpack('<BBB',_tmp_pack[0:3])

        """添加备注信息"""
        _tmp_columns_list_ = []
        filedata.file.seek(self.pos + 288 + self.content_pos + (self.fields * headerconst.field_pack_length) + self.n_length + self.int_length)
        for _column in _tmp_columns_list:
            if _column['comment_length']:
                _column['comment'], = struct.unpack('{}s'.format(_column['comment_length']),filedata.file.read(_column['comment_length']))
            _tmp_columns_list_.append(_column)


        """添加虚拟列信息"""
        _tmp_columns_add_unireg_com = []
        filedata.file.seek(self.pos + 288 + self.content_pos + (self.fields * headerconst.field_pack_length) + self.n_length + self.int_length + self.com_length)
        for _column in _tmp_columns_list_:
            if _column['unireg_type'] == Const.GENERATED_FIELD:
                _,length,stored_type = struct.unpack('<BHB',filedata.file.read(4))
                _column['unireg_com'], = struct.unpack('{}s'.format(length),filedata.file.read(length))
                _column['unireg_stored_type'] = 'STORED' if stored_type else 'VIRTUAL'
            _tmp_columns_add_unireg_com.append(_column)

        self.__get_default_value(self.__get_enum_value(_tmp_columns_add_unireg_com))


    def GetInfo(self):
        table_comment_str = self.__get_column_info()

        for idex,column in enumerate(self.column_names_list):
            print 'column: {}, column_info: {}'.format(column,self.columns_list[idex])
        #print table_comment_str
        return self.columns_list, self.column_names_list, table_comment_str

    def __get_default_value(self,col_list):
        offset = headerconst.io_size + (headerconst.key_length if
                                        headerconst.tmp_key_length == b'\xff\xff' else headerconst.tmp_key_length)
        filedata.file.seek(offset + 1)

        __tmp_col_list = []
        for column in col_list:
            if column['unireg_type'] == Const.GENERATED_FIELD:
                pass
            elif column['field_type'] == column_type_dict.MYSQL_TYPE_LONG:
                _value, = struct.unpack('<I',filedata.file.read(4))
            elif column['field_type'] == column_type_dict.MYSQL_TYPE_VARCHAR:
                #_value, = struct.unpack('{}s'.format(column['field_length']),filedata.file.read(column['field_length']))
                _value = None
            elif column['field_type'] == column_type_dict.MYSQL_TYPE_NEWDECIMAL:
                decimals = self.f_decimals(column['flags'])
                decimals = Const.DECIMAL_MAX_PRECISION if decimals >= Const.DECIMAL_MAX_PRECISION else decimals
                field_length = self.my_decimal_precision_to_length_no_truncation(column['field_length'],decimals,
                                                                                 (column['flags'] & Const.FIELDFLAG_DECIMAL) == 0)
                field_length = field_length - 2
                digits_per_integer = 9
                compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
                unmp_int = int(field_length /digits_per_integer)
                comp_int = field_length - (unmp_int * digits_per_integer)
                read_length = unmp_int + compressed_bytes[comp_int]
                filedata.file.read(read_length)
                #_value, = struct.unpack(a,filedata.file.read(column['field_length']/2))
                _value = None
            elif column['field_type'] == column_type_dict.MYSQL_TYPE_ENUM:
                _value, = struct.unpack('<b',filedata.file.read(1))

            column['defalut_value'] = _value if _value else 'Null'
            __tmp_col_list.append(column)

        """获取虚拟列的默认值,虚拟列默认值在所有默认值最后，不管字段顺序在哪个位置"""
        for info in __tmp_col_list:
            if info['unireg_type'] == Const.GENERATED_FIELD:
                if info['field_type'] == column_type_dict.MYSQL_TYPE_LONG:
                    _value, = struct.unpack('<I', filedata.file.read(4))
                elif info['field_type'] == column_type_dict.MYSQL_TYPE_VARCHAR:
                    _value, = struct.unpack('{}s'.format(info['field_length']),filedata.file.read(info['field_length']))
                info['default_value'] = _value
            self.columns_list.append(info)

    def __get_enum_value(self,col_list):
        offset = self.pos + 288 + self.content_pos + (self.fields * headerconst.field_pack_length) + self.n_length
        filedata.file.seek(offset)
        #_pack = filedata.file.read(self.int_length)
        _tmp_column_list_add_enum_value = []
        for column in col_list:
            _str = ''
            _tmp = ''
            if column['interval_nr']:
                _num = 0
                while True:
                    _a = filedata.file.read(1)
                    if _a == b'\x00' and _tmp == b'\xff':
                        _value = struct.unpack('{}s'.format(_num),_str)[0].split(b'\xff')
                        column['enum_value'] = tuple([a for a in _value if a])
                        break
                    _tmp = _a
                    _str += _a
                    _num += 1
            _tmp_column_list_add_enum_value.append(column)
        return _tmp_column_list_add_enum_value


class JoinSql:
    """拼接SQL"""
    def __init__(self,columns_list,column_names_list,table_comment_str,keys_list,key_names_list,table_name,key_algorithm):
        self.columns_list,self.column_names_list,self.table_comment_str,\
        self.keys_list,self.key_names_list,self.key_algorithm = columns_list,column_names_list,\
                                             table_comment_str,keys_list,key_names_list,key_algorithm

        self.sql = 'CREATE TABLE `{}`(\n'.format(table_name)

    def __join_column(self):
        for index,column_name in enumerate(self.column_names_list):
            self.sql += '{: >2}`{}`'.format('',column_name)
            column_info = self.columns_list[index]

            if column_info['field_type'] == column_type_dict.MYSQL_TYPE_LONG:
                self.sql += ' int({}) '.format(column_info['field_length'])
                if column_info['unireg_type'] == Const.GENERATED_FIELD:
                    self.sql += 'GENERATED ALWAYS AS ({}) {}'.format(column_info['unireg_com'],
                                                                   column_info['unireg_stored_type'])
            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_TINY:
                self.sql += ' tinyint({}) '.format(column_info['field_length'])
                if column_info['unireg_type'] == Const.GENERATED_FIELD:
                    self.sql += 'GENERATED ALWAYS AS ({}) {}'.format(column_info['unireg_com'],
                                                                   column_info['unireg_stored_type'])
            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_LONGLONG:
                self.sql += ' bigint({}) '.format(column_info['field_length'])
                if column_info['unireg_type'] == Const.GENERATED_FIELD:
                    self.sql += 'GENERATED ALWAYS AS ({}) {}'.format(column_info['unireg_com'],
                                                                   column_info['unireg_stored_type'])
            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_SHORT:
                self.sql += ' smallint({}) '.format(column_info['field_length'])
                if column_info['unireg_type'] == Const.GENERATED_FIELD:
                    self.sql += 'GENERATED ALWAYS AS ({}) {}'.format(column_info['unireg_com'],
                                                                   column_info['unireg_stored_type'])
            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_VARCHAR:
                self.sql += ' varchar({}) '.format(column_info['field_length'] / charset_bytes[column_info['charset_type']])
                if column_info['charset_type'] != headerconst.charset:
                    self.sql += 'CHARACTER SET {} COLLATE {} '.format(charset_value[column_info['charset_type']],
                                                                      charset_collate[column_info['charset_type']])
                if column_info['unireg_type'] == Const.GENERATED_FIELD:
                    self.sql += 'GENERATED ALWAYS AS ({}) {}'.format(column_info['unireg_com'],
                                                                   column_info['unireg_stored_type'])

            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_BLOB:
                print column_name
            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_STRING:
                print column_name

            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_ENUM:
                self.sql += ' enum{} '.format(column_info['enum_value'])
                if column_info['charset_type'] != headerconst.charset:
                    self.sql += 'CHARACTER SET {} COLLATE {}'.format(charset_value[column_info['charset_type']],
                                                                     charset_collate[column_info['charset_type']])

            elif column_info['field_type'] == column_type_dict.MYSQL_TYPE_JSON:
                self.sql += ' json'

            if column_info['unireg_type'] != Const.GENERATED_FIELD:
                self.sql += ' DEFAULT {}'.format(column_info['defalut_value'])

            self.sql += ' COMMENT "{}",\n'.format(column_info['comment']) if 'comment' in column_info else ',\n'

        for index,key_name in enumerate(self.key_names_list):
            self.sql += '{: >2}'.format('')
            key_info,key_algorithm = self.keys_list[index],self.key_algorithm[index]
            column_name = ','.join(['`{}`'.format(self.column_names_list[_info['file_num']-1]) for _info in key_info])
            key_flages = key_info[0]['flags'] & Const.HA_NOSAME
            if key_name == 'PRIMARY':
                self.sql += 'PRIMARY KEY({})'.format(column_name)
            elif key_algorithm == ha_key_alg.HA_KEY_ALG_FULLTEXT:
                self.sql += 'FULLTEXT KEY `{}` ({})'.format(key_name,column_name)
            else:
                if key_flages:
                    self.sql += 'UNIQUE KEY `{}` ({})'.format(key_name,column_name)
                else:
                    self.sql += 'KEY `{}` ({})'.format(key_name,column_name)
            if key_algorithm == ha_key_alg.HA_KEY_ALG_BTREE:
                self.sql += ' USING BTREE,\n'
            else:
                if key_name != self.key_names_list[-1]:
                    self.sql += ',\n'
                else:
                    self.sql += '\n'

        self.sql += ') ENGINE={} DEFAULT CHARSET={} COLLATE {} COMMENT "{}"'.format(legacy_db_type[headerconst.db_type],
                                                                        charset_value[headerconst.charset],
                                                                        charset_collate[headerconst.charset],
                                                                        self.table_comment_str)


    def PrintSql(self):
        self.__join_column()
        print self.sql
def Usage():
    __usage__ = """
    	Usage:
    	Options:
      		-h [--help] : print help message
      		-f [--file] : the file path
    	    """
    print __usage__


def main(argv):
    _argv = {}
    try:
        opts, args = getopt.getopt(argv[1:], 'hf:',['--help','--file='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-f', '--file'):
            _argv['file'] = a
        else:
            Usage()
            sys.exit()

    if 'file' in _argv:
        filedata.file = open(_argv['file'],'rb')
        #GetKey().GetKey()
        columns_list,column_names_list,table_comment_str,keys_list,key_names_list,key_algorithm = GetKey().GetKey()

        table_name = _argv['file'].split('/')[-1].split('.')[0]
        JoinSql(columns_list,column_names_list,table_comment_str,keys_list,key_names_list,table_name,key_algorithm).PrintSql()

if __name__ == "__main__":
    main(sys.argv)