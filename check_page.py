# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import getopt, sys, struct,os


class filedata:
    file = None
    page_size = 0
    fil_header_offset = 16 + 4
    lsn_size = 4
    lsn_check = 4
    pages = 0
    page_err = 0


def check(database=None,table=None):
    filedata.pages,filedata.page_err = 0,0
    get_page_size().get_page_size()
    filedata.file.seek(filedata.page_size * 3,0)
    while True:
        try:
            filedata.file.seek(filedata.fil_header_offset, 1)
            _value, = struct.unpack('>I', filedata.file.read(filedata.lsn_size))
            filedata.file.seek(filedata.page_size - filedata.fil_header_offset - filedata.lsn_size - filedata.lsn_check,
                               1)
            __value, = struct.unpack('>I', filedata.file.read(filedata.lsn_check))
            if int(_value) != int(__value):
                filedata.page_err += 1
            filedata.pages += 1
        except:
            break
            pass
    if database and table:
        print 'database: {: >30}  table: {: >30}  pages: {: >10} lsn_warning_pages: {}'.format(database, table,
                                                                                           filedata.pages,
                                                                                           filedata.page_err)
    elif table:
        print 'table: {: >30}  pages: {: >10} lsn_warning_pages: {}'.format(table,filedata.pages,filedata.page_err)
    else:
        print 'this file pages: {: >10} lsn_warning_pages: {}'.format(filedata.pages,filedata.page_err)

class Path:
    def __init__(self):
        pass
    def get_dirs(self,dir_path):
        dirs_path,root_dir = [],None
        for root, dirs, files in os.walk(dir_path):
            if dirs:
                dirs_path,root_dir = dirs,root
            else:
                break
        return dirs_path,root_dir
    def get_ibd_file(self,path):
        ibd_file_list = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if os.path.splitext(file)[1] == '.ibd':
                    ibd_file_list.append(os.path.join(root, file))
        return ibd_file_list

class get_page_size:
    def __init__(self):
        self.fsp_header_offset = 38
        self.fsp_space_flags = 16
        self.fsp_flags_width_page_ssize = 4
        self.fsp_flags_pos_page_ssize = 6
        self.univ_page_ssize_orig = 5
        self.univ_zip_size_min = (1 << 10)
        self.fsp_flags_mask_page_ssize = ((~(~0 << self.fsp_flags_width_page_ssize)) << self.fsp_flags_pos_page_ssize)

    def get_page_size(self):
        filedata.file.seek(self.fsp_header_offset + self.fsp_space_flags)
        flags, = struct.unpack('<I',filedata.file.read(4))
        filedata.page_size = self.__fsp_flags_get_page_size(flags)

    def __fsp_flags_get_page_size(self, flags):
        _ssize = ((flags & self.fsp_flags_mask_page_ssize) >> self.fsp_flags_pos_page_ssize)
        ssize = self.univ_page_ssize_orig if _ssize == 0 else _ssize
        size = ((self.univ_zip_size_min  >> 1) << ssize)
        return size


def Run(dir_path):
    obtain_path = Path()
    dir_path_list,root_dir = obtain_path.get_dirs(dir_path)
    if dir_path_list and root_dir:
        for file_folder in dir_path_list:
            ibd_file_list = obtain_path.get_ibd_file(os.path.join(root_dir,file_folder))
            for ibd_file in ibd_file_list:
                filedata.file = open(ibd_file, 'rb')
                check(database=file_folder,table=os.path.splitext(ibd_file)[0].split('/')[-1])
                filedata.file.close()
    else:
        ibd_file_list = obtain_path.get_ibd_file(dir_path)
        for ibd_file in ibd_file_list:
            filedata.file = open(ibd_file, 'rb')
            check(table=os.path.splitext(ibd_file)[0].split('/')[-1])
            filedata.file.close()

def Usage():
    __usage__ = """
        Usage:
        Options:
                -h [--help] : print help message
                -d [--dir] : the file path
                -f [--file] : the ibd file
            """
    print __usage__


def main(argv):
    _argv = {}
    try:
        opts, args = getopt.getopt(argv[1:], 'hd:f:', ['--help', '--dir=','--file='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-d', '--dir'):
            _argv['dir'] = a
        elif o in ('-f','--file'):
            _argv['file'] = a
        else:
            Usage()
            sys.exit()

    if 'dir' in _argv:
        Run(_argv['dir'])
    elif 'file' in _argv:
        filedata.file = open(_argv['file'],'rb')
        check()
        filedata.file.close()


if __name__ == "__main__":
    main(sys.argv)
