"""
General helper functions
"""
import datetime
from functools import wraps


def timedelta_to_humanr(td):
    form = {0: 'Days',
            1: 'Hours',
            2: 'Mins',
            3: 'Secs',
            4: 'MilliSeconds'}
    # days, hours, minutes, seconds
    output = td.days, td.seconds//3600, (td.seconds//60) % 60, td.seconds % 60, (td.microseconds//1000) % 1000
    return output, form


def timer(func):  # TIMER DEC
    def inner_func(*args, **kwargs):
        start = datetime.datetime.now()
        rv = func(*args, **kwargs)
        end = datetime.datetime.now()
        elapsed = (end - start)
        elapsed_humanr, form = timedelta_to_humanr(elapsed)
        time_string = []
        for i, v in enumerate(elapsed_humanr):
            if v > 0:
                nz = str(v) + ' ' + str(form.get(i))
                time_string.append(nz)
        time_string = ', '.join(time_string)
        print(f"Time taken for {func.__name__}: {time_string}")
        return rv
    return inner_func


def process_timer(process_name):  # SAMPLE TIME DEC WITH ADDITIONAL OPTION TO DEC as process_name
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
                    nz = str(v) + ' ' + str(form.get(i))
                    time_string.append(nz)
            time_string = ', '.join(time_string)
            print(f"Time taken for {process_name}: {time_string}")
            # ---
            return rv
        return dec_inner
    return dec_outer
