"""
openQ
"""


from __future__ import absolute_import
from datetime import timedelta


__all__ = ['DEFAULT_CONFIG', 'CYBERLAMP_QUEUES', 'ACI_QUEUES', 'GPU_GROUPS',
           'CLUSTER_QUEUE_LIMITS']


DEFAULT_CONFIG = '~jll1062/openQ/config.ini'
CYBERLAMP_QUEUES = ['default', 'cl_open', 'cl_gpu', 'cl_higpu', 'cl_himem',
                    'cl_debug', 'cl_phi']
ACI_QUEUES = ['dfc13_a_g_sc_default', 'dfc13_a_t_bc_default', 'open']
GPU_GROUPS = {
    'cyberlamp': ('cyberlamp_collab', 'dfc13_collab', 'dfc13_cyberlamp')
}
CLUSTER_QUEUE_LIMITS = {
    ('cyberlamp', 'default'): dict(
        jobs=dict(soft=200, hard=500), total_cpus=dict(soft=512, hard=1024),
        gpus=0, cpus_per_job=20, walltime=timedelta(hours=169),
        preemptible=False,
    ),
    ('cyberlamp', 'open'): dict(
        jobs=dict(soft=512, hard=1024), total_cpus=dict(soft=1024, hard=2048),
        gpus=4, cpus_per_job=20, walltime=timedelta(hours=96),
        preemptible=True,
    ),
    ('cyberlamp', 'cl_gpu'): dict(
        jobs=dict(soft=200, hard=400), total_cpus=dict(soft=128, hard=256),
        gpus=1, cpus_per_job=20, walltime=timedelta(hours=169),
        preemptible=False,
    ),
    ('cyberlamp', 'cl_higpu'): dict(
        jobs=dict(soft=200, hard=400), total_cpus=dict(soft=128, hard=256),
        gpus=4, cpus_per_job=20, walltime=timedelta(hours=169),
        preemptible=False,
    ),
    ('cyberlamp', 'cl_himem'): dict(
        jobs=dict(soft=100, hard=200), total_cpus=dict(soft=128, hard=256),
        gpus=4, cpus_per_job=20, walltime=timedelta(hours=96),
        preemptible=True,
    ),

    ('aci', 'open'): dict(
        jobs=dict(soft=260, hard=260), total_cpus=dict(soft=260, hard=260),
        gpus=0, cpus_per_job=20, walltime=timedelta(hours=24),
        preemptible=False,
    ),
    ('aci', 'dfc13_a_g_sc_default'): dict(
        jobs=dict(soft=20, hard=20*4), total_cpus=dict(soft=20, hard=20*4),
        gpus=0, cpus_per_job=20, walltime=timedelta(hours=169),
        preemptible=False,
    ),
    ('aci', 'dfc13_a_t_bc_default'): dict(
        jobs=dict(soft=180, hard=180*4), total_cpus=dict(soft=180, hard=180*4),
        gpus=0, cpus_per_job=20, walltime=timedelta(hours=169),
        preemptible=False,
    ),
}
