# Copyright 2014 Hewlett Packard
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import trove.common.cfg as cfg
from trove.common import instance
from trove.instance import models as t_models
from trove.instance.tasks import InstanceTasks
from trove.instance.models import InstanceServiceStatus
from trove.openstack.common import log as logging
from trove.openstack.common import periodic_task
from trove.taskmanager import manager

import datetime

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
FAILOVER_FAILED_TIME_DELTA = datetime.timedelta(seconds=0)
FAILOVER_ACTIVE_TIME_DELTA = datetime.timedelta(minutes=15)


class MonitoringManager(manager.Manager):

    def _engage_failover(self, service):
        now = datetime.datetime.now()
        return ((service.status == instance.ServiceStatuses.RUNNING and
                 now > service.updated_at + FAILOVER_ACTIVE_TIME_DELTA) or
                (service.status == instance.ServiceStatuses.SHUTDOWN and
                 now > service.updated_at + FAILOVER_FAILED_TIME_DELTA))

    def _create_and_auth_clients(self):
        from troveclient.v1.client import Client as tclient
        from designateclient.v1 import Client as dclient
        from novaclient.v1_1.client import Client as nclient

        self.trove_client = tclient(CONF.nova_proxy_admin_user,
                                    CONF.nova_proxy_admin_pass,
                                    CONF.nova_proxy_admin_tenant_name,
                                    auth_url=CONF.trove_auth_url,
                                    service_type='database',
                                    region_name=CONF.os_region_name)
        self.trove_client.authenticate()

        self.nova_client = nclient(CONF.nova_proxy_admin_user,
                                   CONF.nova_proxy_admin_pass,
                                   CONF.nova_proxy_admin_tenant_name,
                                   auth_url=CONF.trove_auth_url,
                                   service_type='compute',
                                   region_name=CONF.os_region_name)
        self.nova_client.authenticate()

        self.designate_client = dclient(username=CONF.dns_username,
                                        password=CONF.dns_passkey,
                                        auth_url=CONF.dns_auth_url,
                                        tenant_id=CONF.dns_account_id,
                                        region_name=CONF.dns_region)

    def _update_dns_records(self, master, slave):
        # Get compute node information
        master_compute = self.nova_client.servers.get(
            master.compute_instance_id)
        slave_compute = self.nova_client.servers.get(
            slave.compute_instance_id)

        # Find master DNS record
        master_dns_record = [
            record_id for record_id in
            self.designate_client.records.list(CONF.dns_domain_id)
            if (record_id.type == 'A' and
                master_compute.name in record_id.name)].pop()

        # Find slave DNS record
        slave_dns_record = [
            record_id for record_id in
            self.designate_client.records.list(CONF.dns_domain_id)
            if (record_id.type == 'A' and
                slave_compute.name in record_id.name)].pop()

        # Update DNS records for master and slave
        # We choose to swap them here
        master_dns_record.data, slave_dns_record.data = \
            slave_dns_record.data, master_dns_record.data

        self.designate_client.records.update(CONF.dns_domain_id,
                                             slave_dns_record)
        self.designate_client.records.update(CONF.dns_domain_id,
                                             master_dns_record)

        return master_dns_record, slave_dns_record

    def _reflect_dns_updates_in_trove(self, master, master_dns,
                                      slave, slave_dns):
        setattr(master, 'hostname', slave_dns.name)
        setattr(slave, 'hostname', master_dns.name)
        master.save()
        slave.save()

    @periodic_task.periodic_task(ticks_between_runs=2)
    def monitor_ha(self, context):
        """Monitors the status of MySQL masters to make sure they are up."""
        LOG.debug("Monitoring Trove Replica Sources (Masters)")

        db_infos = t_models.DBInstance.find_all(deleted=False)
        masters_to_watch = [(instance.slave_of_id, instance) for instance
                            in db_infos.all() if instance.slave_of_id and
                            instance.task_status == InstanceTasks.NONE]

        LOG.debug("Monitoring %s",  masters_to_watch)
        for (master_id, slave) in masters_to_watch:
            master = t_models.DBInstance.find_by(deleted=False, id=master_id)
            service = InstanceServiceStatus.find_by(instance_id=master_id)

            if self._engage_failover(service):
                LOG.debug("Engage FAILOVER from %s to %s NOW!",
                          master_id, slave.id)
                master = t_models.DBInstance.find_by(id=master_id,
                                                     deleted=False)

                self._create_and_auth_clients()

                # Failover Slave to Master by detaching replica source on slave
                self.trove_client.instances.edit(slave.id,
                                                 detach_replica_source=True)

                # Update DNS records for master and slave
                master_dns, slave_dns = self._update_dns_records(master, slave)

                # Finally update the hostnames in trove to
                # reflect the updated DNS information
                self._reflect_dns_updates_in_trove(master, master_dns,
                                                   slave, slave_dns)
