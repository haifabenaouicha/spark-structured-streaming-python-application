import os
from distutils.util import convert_path

from setuptools import setup

path_to_version_file = "version.py"


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


main_ns = {}
version_path = convert_path(path_to_version_file)
with open(version_path) as ver_file:
    exec(ver_file.read(), main_ns)
setup(
    name="pypark streaming",

    description="pypark streaming",

    version=main_ns['__version__'],

    author="Greenflex",

    packages=["source", "entities", "utils", "conf"],

    long_description=read("README.md")
)