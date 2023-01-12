import contextlib
import os
import shlex
import subprocess
import sys


@contextlib.contextmanager
def use_cwd(new_wd: str):
    old_path = os.getcwd()
    try:
        os.chdir(new_wd)
        yield
    finally:
        os.chdir(old_path)


def _get_python_major_minor_version_str():
    return ".".join(map(str, sys.version_info[:2]))


@contextlib.contextmanager
def use_poetry():
    python_version = _get_python_major_minor_version_str()
    try:
        check_call(f"poetry env use {python_version}")
        yield
    finally:
        check_call(f"poetry env remove {python_version}")


def check_call(command):
    return subprocess.check_call(shlex.split(command))


def check_output(command):
    return subprocess.check_output(shlex.split(command))
