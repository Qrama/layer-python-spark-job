#!/usr/bin/env python3
# Copyright (C) 2017  Qrama
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# pylint: disable=c0111,c0103,c0301,c0412
import subprocess as sp
import tempfile

from charms.reactive import when_not, set_state
from charmhelpers.core.hookenv import status_set, config, service_name


@when_not('spark-job.installed')
def deploy_job():
    conf = config()
    if not conf['job_location']:
        tmp_dir = tempfile.mkdtemp()
        sp.check_call(['wget', conf['job_location'], '-P', tmp_dir])
        sp.check_call(['./bin/spark-submit', tmp_dir])
        status_set('active', 'Spark job {} is deployed and running on spark'.format(service_name()))
        set_state('spark-job.installed')
    else:
        status_set('blocked', 'Please provide a valid url to deploy job by changing job_location config')
