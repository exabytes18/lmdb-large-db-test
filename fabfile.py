# http://boto.readthedocs.org/en/latest/ref/ec2.html
import boto.ec2
import re
import time

# http://docs.fabfile.org/en/latest/api/core/operations.html
from fabric.api import abort, env, put, run, sudo
from fabric.context_managers import cd
from fabric.decorators import runs_once, task

env.ec2_key_pair_name = 'exabytes18@geneva'
env.ec2_region = 'us-west-1'
env.user = 'ec2-user'

env.ec2_instances = {
    'testbox': {
        'ami': 'ami-4b6f650e',
        'type': 'c3.2xlarge',
        'bid': 0.10,
        'security_groups': ['SSH Only'],
    }
}


def _launch_instance_abort_on_error(ami,
                                    bid,
                                    instance_type,
                                    security_groups):

    bdm = boto.ec2.blockdevicemapping.BlockDeviceMapping()
    bdm['/dev/sdb'] = boto.ec2.blockdevicemapping.BlockDeviceType(
        ephemeral_name='ephemeral0')
    bdm['/dev/sdc'] = boto.ec2.blockdevicemapping.BlockDeviceType(
        ephemeral_name='ephemeral1')

    ec2 = boto.ec2.connect_to_region(env.ec2_region)
    sirs = ec2.request_spot_instances(
        price=bid,
        image_id=ami,
        count=1,
        type='one-time',
        key_name=env.ec2_key_pair_name,
        instance_type=instance_type,
        block_device_map=bdm,
        security_groups=security_groups)

    instance_ids = set()
    while True:
        time.sleep(10)
        done = True
        for sir in ec2.get_all_spot_instance_requests(map(lambda x: x.id, sirs)):
            print 'State:  %s' % sir.state
            print 'Fault:  %s' % sir.fault
            print 'Status: %s' % sir.status.message
            if sir.state not in ('open', 'active'):
                abort('Failed to launch instances')
            if sir.state == 'open':
                done = False
            if sir.state == 'active':
                instance_ids.add(sir.instance_id)

        if done:
            break

    print ''
    print 'Instances:'
    for reservation in ec2.get_all_instances(list(instance_ids)):
        for instance in reservation.instances:
            print '    %s' % instance.id
            print '        type:        %s' % instance.instance_type
            print '        internal ip: %s' % instance.private_ip_address
            print '        public ip:   %s' % instance.ip_address


@task
@runs_once
def spot_prices():
    ec2 = boto.ec2.connect_to_region(env.ec2_region)
    pricing = ec2.get_spot_price_history(product_description='Linux/UNIX')

    type_az_sph = {}
    for sph in pricing:
        type_az = type_az_sph.setdefault(sph.instance_type, {})
        if sph.availability_zone not in type_az or \
                sph.availability_zone > type_az[sph.availability_zone].timestamp:
            type_az[sph.availability_zone] = sph

    def _inst_cmp(a, b):
        am = re.match(r'(.+?)(\d*)\.(\d*)(.+)', a[0])
        bm = re.match(r'(.+?)(\d*)\.(\d*)(.+)', b[0])

        a_cat, a_gen = (am.group(1), int(am.group(2)))
        b_cat, b_gen = (bm.group(1), int(bm.group(2)))

        ranks = ['micro', 'small', 'medium', 'large', 'xlarge']
        a_rank = ranks.index(am.group(4))
        b_rank = ranks.index(bm.group(4))
        a_xlarge_rank = int(am.group(3) or 0)
        b_xlarge_rank = int(bm.group(3) or 0)

        if a_cat < b_cat:
            return -1
        elif a_cat > b_cat:
            return 1
        elif a_gen < b_gen:
            return -1
        elif a_gen > b_gen:
            return 1
        elif a_rank < b_rank:
            return -1
        elif a_rank > b_rank:
            return 1
        elif a_xlarge_rank < b_xlarge_rank:
            return -1
        elif a_xlarge_rank > b_xlarge_rank:
            return 1
        else:
            return 0

    last_inst_cls = None
    print ''
    for instance_type, az_sph in sorted(type_az_sph.iteritems(), _inst_cmp):
        inst_cls = instance_type.partition('.')[0]
        if last_inst_cls != inst_cls and last_inst_cls is not None:
            print '    ' + '-' * 25

        print '    %s' % instance_type
        for az, sph in sorted(az_sph.iteritems(), lambda a, b: cmp(a[0], b[0])):
            print '        %s: %f' % (az, sph.price)

        last_inst_cls = inst_cls


@task
@runs_once
def launch(name):
    config = env.ec2_instances[name]
    _launch_instance_abort_on_error(
        ami=config['ami'],
        bid=config['bid'],
        instance_type=config['type'],
        security_groups=config['security_groups'])


@task
def configure_disks():
    #sudo('! mountpoint -q /media/ephemeral0 || umount /media/ephemeral0 && rm -rf /media/ephemeral0')
    #sudo('dd if=/dev/zero bs=1M | tee /dev/xvdb > /dev/xvdc') # not necessary on i2, r3, or hi1 instances
    sudo('mdadm --create /dev/md0 --level=0 -c256 --raid-devices=2 /dev/xvdb /dev/xvdc')
    sudo('echo "DEVICE /dev/xvdb /dev/xvdc" > /etc/mdadm.conf')
    sudo('mdadm --detail --scan >> /etc/mdadm.conf')
    sudo('mkfs.xfs -f /dev/md0')
    sudo('mkdir -p /mnt/data')
    sudo('mount -t xfs -o noatime,nodiratime /dev/md0 /mnt/data')


@task
def configure_instance():
    sudo('yum update')
    sudo('yum install autoconf automake gcc git sysstat xfsprogs')
    execute(configure_disks)


@task
def install_lmdb():
    run('git clone -b mdb.master git://git.openldap.org/openldap.git')
    with cd('openldap/libraries/liblmdb'):
        run('make clean')
        run('make install')


@task
def compile_load_generator():
    put('load-generator', '.', mirror_local_mode=True)
    with cd('load-generator'):
        run('./bootstrap')
        run('./configure LDFLAGS="-L/usr/local/lib" CPPFLAGS="-I/usr/local/include"')
        run('make clean')
        run('make')


@task
def run_benchmarks():
    put('benchmarks.sh', '.', mode=0755)
    put('run.sh', '.', mode=0755)
    sudo('./run.sh && sleep 1')

