import os
from setuptools import setup, find_packages
from turbokit import __author__, __version__, __author_email__


def __read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except IOError:
        return ''


install_requires = __read('requirements.txt').split()

setup(
    name='turbokit',
    author=__author__,
    author_email=__author_email__,
    version=__version__,
    description='Schematics + Motor ODM',
    long_description=__read('README.rst'),
    platforms=('Any'),
    packages=find_packages(),
    install_requires=install_requires,
    keywords='tornado motor odm schematics models async future'.split(),
    include_package_data=True,
    license='BSD License',
    package_dir={'turbokit': 'turbokit'},
    url='https://github.com/ExpertSystem/TurboKit',
    classifiers=[
        "Development Status :: 4 - Beta",
        'Environment :: Web Environment',
        'Framework :: Tornado',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Topic :: Utilities',
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
