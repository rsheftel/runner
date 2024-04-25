"""
unit test for the File module
"""

import datetime
import logging
import os
import tempfile
from unittest import mock

import pytest

import utils.file as ufile


def test_touch():
    filename = tempfile.gettempdir() + "/touchTest" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    ufile.touch(filename)
    assert os.path.exists(filename)


def test_delete(capsys):
    filename = tempfile.gettempdir() + "/deleteTest" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    ufile.touch(filename)

    # mock an OSError
    with mock.patch('os.remove', new=mock.Mock(side_effect=OSError(2, 'testing failure'))):
        ufile.delete(filename, output_trace=False)
        ufile.delete(filename, output_trace=True)
    out, err = capsys.readouterr()
    assert out == "Error: None - testing failure.\n"

    ufile.delete(filename)
    assert not os.path.exists(filename)

    ufile.delete(filename)
    assert not os.path.exists(filename)

    ufile.delete(filename, output_trace=False)
    # capture text output that file does not exist
    ufile.delete(filename, output_trace=True)
    assert not os.path.exists(filename)
    out, err = capsys.readouterr()
    assert out == f"Can not find file: {filename}\n"

    # capture test output that it is not a file
    new_dir = tempfile.gettempdir() + "/deleteTest_dir" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    os.mkdir(new_dir)
    ufile.delete(new_dir, output_trace=False)
    # with output trade
    ufile.delete(new_dir, output_trace=True)
    out, err = capsys.readouterr()
    assert out == f"Not a file, a directory: {new_dir}\n"


def test_delete_dir(capsys):
    directory = ufile.add_unique_postfix(tempfile.gettempdir() + '/deleteTest') + '/'
    os.makedirs(directory)
    file = directory + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    ufile.touch(file)

    # mock an OSError
    with mock.patch('shutil.rmtree', new=mock.Mock(side_effect=OSError(2, 'testing failure'))):
        ufile.delete_dir(directory, output_trace=False)
        ufile.delete_dir(directory, output_trace=True)
    out, err = capsys.readouterr()
    assert out == "Error: None - testing failure.\n"

    ufile.delete_dir(directory)
    assert not os.path.exists(directory)

    ufile.delete_dir(directory)
    assert not os.path.exists(directory)

    ufile.delete_dir(directory, output_trace=False)
    # capture text output that file does not exist
    ufile.delete_dir(directory, output_trace=True)
    assert not os.path.exists(directory)
    out, err = capsys.readouterr()
    assert out == f"Can not find directory: {directory}\n"

    # capture test output that it is not a file
    new_file = ufile.add_unique_postfix(tempfile.gettempdir() + "/deleteTest.txt")
    ufile.touch(new_file)
    ufile.delete_dir(new_file, output_trace=False)
    # with output trade
    ufile.delete_dir(new_file, output_trace=True)
    out, err = capsys.readouterr()
    assert out == "Not a directory, a file: " + new_file + '\n'


def test_rename(capsys):
    source = tempfile.gettempdir() + "/renameTest" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    ufile.touch(source)

    # mock an OSError
    with mock.patch('os.rename', new=mock.Mock(side_effect=OSError(2, 'testing failure'))):
        ufile.rename(source, source + "_new", output_trace=False)
        ufile.rename(source, source + "_new", output_trace=True)
    out, err = capsys.readouterr()
    assert out == "Error: None - testing failure.\n"

    ufile.rename(source, source + "_new")
    assert not os.path.exists(source)
    assert os.path.exists(source + "_new")

    ufile.rename(source, source + "_nogo", output_trace=False)

    # without output trade
    ufile.rename(source, source + "_nogo", output_trace=False)
    # capture text output that file does not exist
    ufile.rename(source, source + "_nogo", output_trace=True)
    out, err = capsys.readouterr()
    assert out == f"Can not find file: {source}\n"

    # capture test output that it is not a file
    new_dir = tempfile.gettempdir() + "/renameTest_dir" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    os.mkdir(new_dir)
    ufile.rename(new_dir, new_dir + '_new', output_trace=False)
    # with output trade
    ufile.rename(new_dir, new_dir + '_new', output_trace=True)
    out, err = capsys.readouterr()
    assert out == "Not a file, a directory: " + new_dir + '\n'


def test_add_slash():
    assert ufile.add_slash('/tmp/') == '/tmp/'
    assert ufile.add_slash('/tmp') == '/tmp/'


def test_files_in_directory():
    inst_dir = os.path.normpath("./utils/tests/")
    files = ufile.files_in_directory(inst_dir)
    assert 'test_file.py' in files

    files = ufile.files_in_directory(inst_dir, full_path=True)
    assert os.path.join(inst_dir, 'test_file.py') in files


def test_dirs_in_directory():
    inst_dir = os.path.normpath("./utils")  # the directory of the test dir
    dirs = ufile.directories_in_directory(inst_dir)

    assert 'tests' in dirs


def test_add_unique_postfix():
    filename_in = os.path.join(tempfile.tempdir, 'test_unique_file.txt')
    filename = ufile.add_unique_postfix(filename_in)
    assert filename.split('(')[0] == os.path.normpath(tempfile.tempdir + '/test_unique_file')
    assert filename.split(')')[1] == '.txt'
    unique_part = filename.split('(')[1].split(')')[0]
    assert len(unique_part.split('-')[0]) == 8
    assert len(unique_part.split('-')[1]) == 12

    # mock the file always found
    with mock.patch('os.path.exists', new=mock.Mock(return_value=True)):
        with pytest.raises(RuntimeError):
            ufile.add_unique_postfix(filename_in)


def test_setup_logging():
    filename = tempfile.gettempdir() + "/logTest_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".txt"
    ufile.setup_logging(True, filename=filename)

    log = logging.getLogger('logging test')
    log.info('log info output')
    log.warning('log warning output')

    ufile.setup_logging(console=False)
    output = list(open(filename))
    assert output[0].split('-')[-1] == ' log info output\n'
    assert output[1].split('-')[-1] == ' log warning output\n'
