import sys
import os
import logging
import traceback
import io
import colorlog  # 控制台日志输入颜色

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

# _lock = threading.RLock()

# def _acquireLock():
#     """
#     Acquire the module-level lock for serializing access to shared data.
#
#     This should be released with _releaseLock().
#     """
#     if _lock:
#         _lock.acquire()
#
# def _releaseLock():
#     """
#     Release the module-level lock acquired by calling _acquireLock().
#     """
#     if _lock:
#         _lock.release()

log_colors_config = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'red',
}


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

logging.setLoggerClass(WrappedLogger)

formatter = colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s',
    log_colors=log_colors_config)  # 日志输出格式
handler = colorlog.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
# logger.info('a message using a custom level')

