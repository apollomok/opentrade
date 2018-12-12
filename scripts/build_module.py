#!/usr/bin/env python3

import setuptools
import glob

setuptools.setup(
    name='opentrade',
    version='1.0',
    ext_modules=[
        setuptools.Extension(
            'opentrade',
            glob.glob('src/opentrade/*cc'),
            extra_compile_args=['-std=c++17', '-Wno-deprecated-declarations'],
            include_dirs=[
                './src', '/usr/include/soci', '/usr/include/postgresql'
            ],
            libraries=[
                'boost_python3',
                'boost_system',
                'boost_date_time',
                'boost_program_options',
                'boost_iostreams',
                'boost_filesystem',
                'log4cxx',
                'tbb',
                'pthread',
                'crypto',
                'dl',
                'soci_core',
                'soci_postgresql',
            ],
        ),
    ],
    include_package_data=True)
