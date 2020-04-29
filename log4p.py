# coding:utf-8
import logging
from logging.handlers import RotatingFileHandler # 按文件大小滚动备份
import colorlog  # 控制台日志输入颜色
import time
import datetime
import os, sys, io
import traceback

currentframe = lambda: sys._getframe(3)
_logging_srcfile = os.path.normcase(logging.addLevelName.__code__.co_filename)
_this_srcfile = __file__
#Get both logger's and this file's path so the wrapped logger can tell when its looking at the code stack outside of this file.
_loggingfile = os.path.normcase(logging.__file__)
if hasattr(sys, 'frozen'): #support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)
_wrongCallerFiles = set([_loggingfile, _srcfile])

cur_path = os.path.dirname(os.path.realpath(__file__))  # log_path是存放日志的路径
log_path = os.path.join(cur_path, 'logs')
if not os.path.exists(log_path): os.mkdir(log_path)  # 如果不存在这个logs文件夹，就自动创建一个
logName = os.path.join(log_path, '%s.log' % (os.path.basename(__file__).split('.')[0] + '_' + time.strftime('%Y-%m-%d-%H-%M-%S')))  # 文件的命名

log_colors_config = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'red',
}


class Log:
    def __init__(self, logName=logName):
        logging.setLoggerClass(WrappedLogger)  # 重新绑定logging实例
        filename = os.path.basename(logName).split('.')[0]
        self.logName = os.path.join(log_path, '%s.log' % (os.path.basename(logName).split('.')[0] + '_' + time.strftime('%Y-%m-%d-%H-%M-%S')))
        self.logger = logging.getLogger(filename)
        self.logger.setLevel(logging.DEBUG)
        self.handle_logs()

    def get_file_sorted(self, file_path):
        """最后修改时间顺序升序排列 os.path.getmtime()->获取文件最后修改时间"""
        dir_list = os.listdir(file_path)
        if not dir_list:
            return
        else:
            dir_list = sorted(dir_list, key=lambda x: os.path.getmtime(os.path.join(file_path, x)))
            return dir_list

    def TimeStampToTime(self, timestamp):
        """格式化时间"""
        timeStruct = time.localtime(timestamp)
        return str(time.strftime('%Y-%m-%d', timeStruct))

    def handle_logs(self):
        """处理日志过期天数和文件数量"""
        dir_list = ['logs']  # 要删除文件的目录名
        for dir in dir_list:
            dirPath = os.path.dirname(self.logName)   # 拼接删除目录完整路径
            file_list = self.get_file_sorted(dirPath)  # 返回按修改时间排序的文件list
            if file_list:  # 目录下没有日志文件
                for i in file_list:
                    file_path = os.path.join(dirPath, i)  # 拼接文件的完整路径
                    t_list = self.TimeStampToTime(os.path.getctime(file_path)).split('-')
                    now_list = self.TimeStampToTime(time.time()).split('-')
                    t = datetime.datetime(int(t_list[0]), int(t_list[1]),
                                          int(t_list[2]))  # 将时间转换成datetime.datetime 类型
                    now = datetime.datetime(int(now_list[0]), int(now_list[1]), int(now_list[2]))
                    if (now - t).days > 10:  # 创建时间大于10天的文件删除
                        self.delete_logs(file_path)
                # if len(file_list) > 10:  # 限制目录下记录文件数量
                #     file_list = file_list[0:-10]
                #     for i in file_list:
                #         file_path = os.path.join(dirPath, i)
                #         print(file_path)
                #         self.delete_logs(file_path)

    def delete_logs(self, file_path):
        try:
            os.remove(file_path)
        except PermissionError as e:
            Log().warning('删除日志文件失败：{}'.format(e))

    def __console(self, level, message):
        formatter = logging.Formatter(
            '[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s')  # 日志输出格式
        # 创建一个FileHandler，用于写到本地
        fh = RotatingFileHandler(filename=self.logName, mode='a', maxBytes=1024 * 1024 * 5, backupCount=5,
                                 encoding='utf-8')  # 使用RotatingFileHandler类，滚动备份日志
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # 创建一个StreamHandler,用于输出到控制台
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s',
            log_colors=log_colors_config)  # 日志输出格式
        ch = colorlog.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        if level == 'info':
            self.logger.info(message)
        elif level == 'debug':
            self.logger.debug(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        # 这两行代码是为了避免日志输出重复问题
        self.logger.removeHandler(ch)
        self.logger.removeHandler(fh)
        fh.close()  # 关闭打开的文件

    def debug(self, message):
        self.__console('debug', message)

    def info(self, message):
        self.__console('info', message)

    def warning(self, message):
        self.__console('warning', message)

    def error(self, message):
        self.__console('error', message)


class WrappedLogger(logging.getLoggerClass()):
    def __init__(self, name, level=logging.NOTSET):
        super(WrappedLogger, self).__init__(name, level)

    def your_function(self, msg, *args, **kwargs):
        # whatever you want to do here...
        self._log(12, msg, args, **kwargs)

    def add_handler(self, hdlr):
        pass
        # self.addHandler(hdlr)
        # return hdlr

    def findCaller(self, stack_info=False):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.

        This function comes straight from the original python one
        """
        f = currentframe()
        # On some versions of IronPython, currentframe() returns None if
        # IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            if filename == _srcfile:
                f = f.f_back
                continue
            sinfo = None
            if stack_info:
                sio = io.StringIO()
                sio.write('Stack (most recent call last):\n')
                traceback.print_stack(f, file=sio)
                sinfo = sio.getvalue()
                if sinfo[-1] == '\n':
                    sinfo = sinfo[:-1]
                sio.close()
            rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
            break
        return rv


if __name__ == "__main__":
    log = Log()
    log.debug("---测试开始----")
    log.info("操作步骤")
    log.warning("----测试结束----")
    log.error("----测试错误----")
