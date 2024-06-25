"""
Helper utility functions for file operations.
"""

import datetime
import logging
import os
import shutil


def touch(filename):
    """
    Create an empty file, or change the last access time on existing file. Same as the linux 'touch' bash command

    :param filename: name of the file to touch
    :return: nothing
    """
    with open(filename, 'a'):
        os.utime(filename, None)


def delete(filename, output_trace=False):
    """
    A better delete file function that does not raise errors if the file is not found, but can optionally output
    information about what is happening to the stderror. Meant mostly to be used in scripts where the output is logged.

    If the filename exists it will be deleted. If the filename is a directory it will not be deleted. If the filename
    does not exist the function will return but that fact will be output to the strerror if output_trace = True.

    :param filename: filename to delete
    :param output_trace: True to output log information to strerror
    :return: nothing
    """
    if os.path.exists(filename):
        if os.path.isfile(filename):
            try:
                os.remove(filename)
            except OSError as error:
                if output_trace:
                    print(f"Error: {error.filename} - {error.strerror}.")
        elif output_trace:
            print(f"Not a file, a directory: {filename}")
    elif output_trace:
        print(f"Can not find file: {filename}")


def delete_dir(directory, output_trace=False):
    """
    A better delete directory function that does not raise errors if the directory is not found, but can optionally
    output information about what is happening to the stderror. Meant mostly to be used in scripts where the output is
    logged.

    If the directory exists it will be deleted. If the directory is a filename it will not be deleted. If the directory
    does not exist the function will return but that fact will be output to the strerror if output_trace = True.

    :param directory: directory to delete
    :param output_trace: True to output log information to strerror
    :return: nothing
    """
    if os.path.exists(directory):
        if os.path.isdir(directory):
            try:
                shutil.rmtree(directory)
            except OSError as error:
                if output_trace:
                    print(f"Error: {error.filename} - {error.strerror}.")
        elif output_trace:
            print(f"Not a directory, a file: {directory}")
    elif output_trace:
        print(f"Can not find directory: {directory}")


def add_unique_postfix(filename, dash_separator=False):
    """
    Used to create a unique filename. Takes in a full filename (path, file and extension) and inserts the datetime in
    parentheses after filename and before the extension. The datetime is in format YYYYMMDD-HHMMSSmmmmmm where "m"s are
    the milliseconds. In case the function is called twice inside a millisecond it will check that the filename does
    not already exist, if it does will loop to create a new filename, attempting that 1,000 times.

    :param filename: full filename
    :param dash_separator: if True then use "-" to separate the unique filename, otherwise use parenthesis
    :return: filename with (YYYYMMDD-HHSSMMmmmmmm) inserted between the filename and the extension
    """
    path, name = os.path.split(filename)
    name, ext = os.path.splitext(name)

    for _ in range(1000):  # Loop 1,000 times, if cannot find a unique filename, then raise error
        now = datetime.datetime.now().strftime('%Y%m%d-%H%M%S%f')
        formatted_now = f'-{now}' if dash_separator else f'({now})'
        unique_filename = os.path.join(path, f'{name}{formatted_now}{ext}')
        if not os.path.exists(unique_filename):
            return unique_filename

    # If the entire loop expires without creating a unique filename
    raise RuntimeError('Unable to create unique filename.')


def setup_logging(console=True, filename=None):
    """
    Standard logging setup. To use properly this should be run at the module with the __main__ that is the top level.
    Then each imported module should have the following at the module level: log = logging.getLogger(__name__)

    :param console: True to output to the console, False for silent
    :param filename: filename to output log file, or None
    :return: nothing
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if filename:
        fh = logging.FileHandler(filename)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if not (console or filename):
        logger.addHandler(logging.NullHandler)


def log_filename(name):
    """
    Take the name and creates the full path to the log file.

    :param name: name without the .log at the end
    :return: string log file full location
    """
    return f'{os.getenv("LOGS")}/{name}.log'
