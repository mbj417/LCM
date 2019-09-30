#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2018 Telefonica S.A.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from setuptools import setup

_name = "osm_lcm"
# version is at first line of osm_lcm/html_public/version
here = os.path.abspath(os.path.dirname(__file__))
# VERSION = "4.0.1rc1"
with open(os.path.join(here, 'README.rst')) as readme_file:
    README = readme_file.read()

setup(
    name=_name,
    description='OSM Life Cycle Management module',
    long_description=README,
    version_command=('git describe --match v* --tags --long --dirty', 'pep440-git-full'),
    # version=VERSION,
    # python_requires='>3.5.0',
    author='ETSI OSM',
    author_email='alfonso.tiernosepulveda@telefonica.com',
    maintainer='Alfonso Tierno',
    maintainer_email='alfonso.tiernosepulveda@telefonica.com',
    url='https://osm.etsi.org/gitweb/?p=osm/LCM.git;a=summary',
    license='Apache 2.0',

    packages=[_name],
    include_package_data=True,
    # data_files=[('/etc/osm/', ['osm_lcm/lcm.cfg']),
    #             ('/etc/systemd/system/', ['osm_lcm/osm-lcm.service']),
    #             ],
    dependency_links=[
        'git+https://osm.etsi.org/gerrit/osm/common.git#egg=osm-common',
        'git+https://osm.etsi.org/gerrit/osm/N2VC.git#egg=n2vc',
    ],
    install_requires=[
        # 'pymongo',
        'PyYAML==3.*',
        'aiohttp==0.20.2',
        'osm-common',
        'n2vc',
        'jinja2',
        # TODO this is version installed by 'apt python3-aiohttp' on Ubuntu Sserver 14.04
        # version installed by pip 3.3.2 is not compatible. Code should be migrated to this version and use pip3
    ],
    setup_requires=['setuptools-version-command'],
)
