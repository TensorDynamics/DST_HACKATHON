"""
General helper functions
"""
import cProfile
import datetime
from functools import wraps
import pstats
import io


def timedelta_to_humanr(td):
    form = {0: 'Days',
            1: 'Hours',
            2: 'Mins',
            3: 'Secs',
            4: 'Micro Seconds'}
    # days, hours, minutes, seconds
    output = td.days, td.seconds//3600, (td.seconds//60) % 60, td.seconds % 60, td.microseconds
    return output, form


def timer(func):  # TIMER DEC
    """ decorator for timing a function"""
    def inner_func(*args, **kwargs):
        start = datetime.datetime.now()
        rv = func(*args, **kwargs)
        end = datetime.datetime.now()
        elapsed = (end - start)
        elapsed_humanr, form = timedelta_to_humanr(elapsed)
        time_string = []
        for i, v in enumerate(elapsed_humanr):
            if v > 0:
                nz = f'{str(v)} {str(form.get(i))}'
                time_string.append(nz)
        time_string = ', '.join(time_string)
        print(f"Time taken for {func.__name__}: {time_string}")
        return rv
    return inner_func


def process_timer(process_name):
    """ decorator for timing a function with process name"""
    def dec_outer(func):
        @wraps(func)
        def dec_inner(*args, **kwargs):
            start = datetime.datetime.now()
            rv = func(*args, **kwargs)
            # ---
            end = datetime.datetime.now()
            elapsed = (end - start)
            elapsed_humanr, form = timedelta_to_humanr(elapsed)
            time_string = []
            for i, v in enumerate(elapsed_humanr):
                if v > 0:
                    nz = f'{str(v)} {str(form.get(i))}'
                    time_string.append(nz)
            time_string = ', '.join(time_string)
            msg = f"Time taken for {process_name}: {time_string}"
            print(msg)
            # ---
            return rv
        return dec_inner
    return dec_outer


def profileit(print_stats=False):
    def inner(func):
        def wrapper(*args, **kwargs):
            prof = cProfile.Profile()
            prof.enable()
            retval = prof.runcall(func, *args, **kwargs)
            prof.disable()
            s = io.StringIO()
            ps = pstats.Stats(prof, stream=s).sort_stats('tottime')
            if print_stats:
                ps.print_stats()
            with open(f'{func.__name__}.profile', 'w+') as f:
                f.write(s.getvalue())# Note use of name from outer scope
            return retval
        return wrapper
    return inner


def profiling():
    def _profiling(f):
        @wraps(f)
        def __profiling(*rgs, **kwargs):
            pr = cProfile.Profile()
            pr.enable()
            result = f(*rgs, **kwargs)
            pr.disable()
            # save stats into file
            pr.dump_stats('profile_dump')
            return result
        return __profiling
    return _profiling
