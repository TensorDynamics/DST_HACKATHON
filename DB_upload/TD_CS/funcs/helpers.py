"""
Helper Functions
"""
import os
import sys
import errno
import joblib
import inspect
import re
import traceback
import configparser
from ast import literal_eval
import datetime
from shutil import copytree, rmtree
import pickle
import argparse
import pathlib


def parse_args_shell():
    parser = argparse.ArgumentParser(description="WRF Py process",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-v", "--verbose",
                        default=False,
                        action='store_true',
                        help='verbose for process')

    parser.add_argument("-o", "--localoutput",
                        help="writes local outputs in a path",
                        default=False,
                        action='store_true')

    parser.add_argument("-d", "--localdir",
                        help="provide local path for output csv",
                        default=None,
                        nargs='?',
                        type=pathlib.Path)

    args = parser.parse_args()
    return vars(args)


def is_pathname_valid(pathname: str) -> bool:
    """
    :param pathname: the path to be tested for valid a path
    :type pathname: str
    :return: `True` if the passed pathname is a valid pathname for the current OS; `False` otherwise.
    :rtype: bool

    """
    # If this pathname is either not a string or is but is empty, this pathname
    # is invalid.
    try:
        if not isinstance(pathname, str) or not pathname:
            return False

        # Strip this pathname's Windows-specific drive specifier (e.g., `C:\`)
        # if any. Since Windows prohibits path components from containing `:`
        # characters, failing to strip this `:`-suffixed prefix would
        # erroneously invalidate all valid absolute Windows pathnames.
        _, pathname = os.path.splitdrive(pathname)

        # Directory guaranteed to exist. If the current OS is Windows, this is
        # the drive to which Windows was installed (e.g., the "%HOMEDRIVE%"
        # environment variable); else, the typical root directory.
        root_dirname = os.environ.get('HOMEDRIVE', 'C:') \
            if sys.platform == 'win32' else os.path.sep
        assert os.path.isdir(root_dirname)  # ...Murphy and her ironclad Law

        # Append a path separator to this directory if needed.
        root_dirname = root_dirname.rstrip(os.path.sep) + os.path.sep

        # Test whether each path component split from this pathname is valid or
        # not, ignoring non-existent and non-readable path components.
        for pathname_part in pathname.split(os.path.sep):
            try:
                os.lstat(root_dirname + pathname_part)
            # If an OS-specific exception is raised, its error code
            # indicates whether this pathname is valid or not. Unless this
            # is the case, this exception implies an ignorable kernel or
            # filesystem complaint (e.g., path not found or inaccessible).
            #
            # Only the following exceptions indicate invalid pathnames:
            #
            # * Instances of the Windows-specific "WindowsError" class
            #   defining the "winerror" attribute whose value is
            #   "ERROR_INVALID_NAME". Under Windows, "winerror" is more
            #   fine-grained and hence useful than the generic "errno"
            #   attribute. When a too-long pathname is passed, for example,
            #   "errno" is "ENOENT" (i.e., no such file or directory) rather
            #   than "ENAMETOOLONG" (i.e., file name too long).
            # * Instances of the cross-platform "OSError" class defining the
            #   generic "errno" attribute whose value is either:
            #   * Under most POSIX-compatible OSes, "ENAMETOOLONG".
            #   * Under some edge-case OSes (e.g., SunOS, *BSD), "ERANGE".
            except OSError as exc:
                if hasattr(exc, 'winerror'):
                    if exc.winerror == 'ERROR_INVALID_NAME':
                        return False
                elif exc.errno in {errno.ENAMETOOLONG, errno.ERANGE}:
                    return False
    # If a "TypeError" exception was raised, it almost certainly has the
    # error message "embedded NUL character" indicating an invalid pathname.
    except TypeError as exc:
        print(exc)
        return False
    # If no exception was raised, all path components and hence this
    # pathname itself are valid. (Praise be to the curmudgeonly python.)
    else:
        return True
    # If any other exception was raised, this is an unrelated fatal issue
    # (e.g., a bug).


def is_path_creatable(pathname: str) -> bool:
    """
    :param pathname: the path to be tested for validity for creation
    :type pathname: str
    :return: `True` if the current user has sufficient permissions to create the passed pathname; `False` otherwise.
    :rtype: bool
    """
    # Parent directory of the passed path. If empty, we substitute the current
    # working directory (CWD) instead.
    dirname = os.path.dirname(pathname) or os.getcwd()
    return os.access(dirname, os.W_OK)


def is_path_exists_or_creatable(pathname: str) -> bool:
    """
    :param pathname: the path to be tested for validity for creation
    :type pathname: str
    :return: `True` if the passed pathname is a valid pathname for the current OS _and_
        either currently exists or is hypothetically creatable; `False` otherwise.
    :rtype: bool
    """
    try:
        # To prevent "os" module calls from raising undesirable exceptions on
        # invalid pathnames, is_pathname_valid() is explicitly called first.
        return is_pathname_valid(pathname) and (
                os.path.exists(pathname) or is_path_creatable(pathname))
    # Report failure on non-fatal filesystem complaints (e.g., connection
    # timeouts, permissions issues) implying this path to be inaccessible. All
    # other exceptions are unrelated fatal issues and should not be caught here.
    except OSError:
        return False


def get_variable_name():
    """
    Returns variable name as string
    :rtype: str
    """
    pattern = re.compile(r'[\W+\w+]*get_variable_name\((\w+)\)')
    return pattern.match(traceback.extract_stack(limit=2)[0][3]).group(1)


def dump_obj_joblib(obj, name, destination=None):
    """
    Saves processed dataframes to pickle objects using joblib
    """
    if destination is None:
        joblib.dump(obj, f"{name}.pkl")
    else:
        if not os.path.isdir(destination) and is_path_creatable(destination):
            os.makedirs(destination, exist_ok=True)

        dump_as = os.path.join(destination, f"{name}.pkl")
        joblib.dump(obj, dump_as)


def dump_obj_pickle(obj, name, destination=None):
    """
    Saves processed dataframes to pickle objects using joblib
    """
    if destination is None:
        pickle.dump(obj, open(f"{name}.pkl", 'wb'))
    else:
        if not os.path.isdir(destination) and is_path_creatable(destination):
            os.makedirs(destination, exist_ok=True)

        dump_as = os.path.join(destination, f"{name}.pkl")
        pickle.dump(obj, open(dump_as, 'wb'))


def display_source(fun):
    """
    Displays the source code for a function
    :param fun: Function alias to see the source for
    :type fun: function
    """
    lines = inspect.getsource(fun)
    return print(lines)


def create_folder(destination, name):
    """
    Creates a folder in destination by name in destination
    :param destination: The destination for the folder to be created
    :type destination: path like, str

    :param name: The name of the folder to be created
    :type name: str
    """

    if is_path_creatable(destination):
        create_ = os.path.join(destination, name)
        os.makedirs(create_, exist_ok=True)
        return create_


def read_obj(destination, name):
    """
    Reads a joblib object

    :param destination: The destination of the object to be read
    :type destination: path like, str

    :param name: The name of the object to be read
    :type name: str

    :return: object, return type is based on the original class of the joblib object
    """
    obj_path = os.path.join(destination, name)
    return joblib.load(obj_path)


def read_object_from_ini(section,
                         object_name,
                         destination=None,
                         file_name='settings.ini'):
    """
    Reads the name dictionary from the settings ini file

    :param section: The section name to read from , defaults to 'section_a'
    :type section: str

    :param object_name: The object name to read from file
    :type object_name: str

    :param destination: The destination of settings ini file. Defaults to None i.e current working directory
    :type destination: valid path

    :param file_name: The file name. defaults to settings.ini
    :type file_name: str

    :return: Object expected from file
    :rtype: str

    """
    parser = configparser.ConfigParser()
    if destination is None:
        destination = os.getcwd()

    parser.read(os.path.join(destination, file_name))
    # read values from a section
    obj = parser.get(section, object_name)
    try:
        obj = literal_eval(obj)
        return obj
    except ValueError:
        return obj


def get_currenttime(string_or_date='string', format_='%d-%b-%Y %H:%M:%S'):
    """
    Returns the time at which this function is called.
    :param string_or_date: Whether to return a string or a datetime object.
        Optional, defaults to 'string'. Options are 'string' or 'date'
    :type string_or_date: str

    :param format_: The datetime format of the string to return. (strftime reference)
    :rtype format_: str
    :return: String or date as chosen
    :rtype: str/datetime
    """
    if string_or_date == 'string':
        return datetime.datetime.now().strftime(format_)
    elif 'date' in string_or_date:
        return datetime.datetime.now()


def get_execution_time(start_time, return_string=False, process_name=None):
    """
    Calculates the execution time from the start time to the time this function is called
    :param process_name:
    :param start_time: THe starting time of the program
    :type start_time: datetime

    :param return_string: Whether to return string for further use, optional: defaults to False
    :type return_string: bool

    :param process_name: The process name, when given is added in a pretty verbose
    :type process_name: str

    :return: Only when return string is set to True, else printed as verbose
    :rtype: str
    """

    if isinstance(start_time, datetime.datetime):
        # Time taken for processing
        end_time = datetime.datetime.now()
        delta = end_time - start_time

        total_mins, second = divmod(delta.seconds, 60)
        hour, minute = divmod(total_mins, 60)

        if process_name is None:
            print(f"Time taken : {hour} HRS, {minute} MINS, {second} seconds")
        else:
            print(f"Time taken for {process_name}: {hour} HRS, {minute} MINS, {second} seconds")

        if return_string:
            return f"Time taken for {process_name}: {hour} HRS, {minute} MINS, {second} seconds"


def copy_folder(from_destination,
                to_destination,
                folder_name='Input'):
    """
    Copies a directory with all it's components
    :param from_destination: Path from where folder to copy is placed
    :type from_destination: path like, str
    :param to_destination: The destination path to copy the folder
    :type to_destination: path like, str
    :param folder_name: The folder name in the from path to copy
    :type folder_name: str
    :return: None
    """
    from_ = os.path.join(from_destination, folder_name)
    to_ = os.path.join(to_destination, folder_name)
    try:
        copytree(from_, to_, symlinks=False, ignore=None)
    except FileExistsError:
        rmtree(to_)
        copytree(from_, to_, symlinks=False, ignore=None)


def file_exists(directory, file_name):
    """
    Checks if a file exists in a directory
    :param directory:
    :type directory: path like, str
    :param file_name: THe file name to check
    :type file_name: str
    :return: Whether the file_name exists in the directory
    :rtype: bool
    """
    return os.path.isfile(os.path.abspath(os.path.join(directory, file_name)))


def write_csv(data, destination, name, **kwargs):
    """
    :param data: Data to write
    :param destination: folder destination
    :param name: name of file
    """
    data.to_csv(os.path.join(destination, name), **kwargs)
