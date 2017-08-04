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
NODES = dict(
    # NOTE: "standard" nodes
    #   https://wikispaces.psu.edu/display/CyberLAMP/System+information
    # are divided into what I'm calling the "cl_default" configuration (24 CPU,
    # 0 GPU) and the "cl_gpu" configuration (4 CPUs and 1 GPU).
    cl_default=dict(
        cpus=24,
        gpus=0,
        memory=242*(1024**3),
        node_count=77+6
    ),
    cl_gpu=dict(
        cpus=4,
        gpus=1,
        memory=1001*(1024**3),
        node_count=77
    ),
    cl_higpu=dict(
        cpus=28,
        gpus=4,
        memory=242*(1024**3),
        node_count=6
    ),
    cl_himem=dict(
        cpus=56,
        gpus=0,
        memory=1001*(1024**3),
        node_count=4
    ),
    cl_phi=dict(
        cpus=68,
        gpus=0,
        memory=160*(1024**3),
        node_count=24
    ),

    aci_basic=dict(
        cpus=24,
        gpus=0,
        memory=125*(1024**3),
        node_count=240
    ),
    aci_stmem0=dict(
        cpus=20,
        gpus=0,
        memory=251*(1024**3),
        node_count=240
    ),
    aci_stmem1=dict(
        cpus=24,
        gpus=0,
        memory=251*(1024**3),
        node_count=240
    ),
    aci_himem=dict(
        cpus=40,
        gpus=0,
        memory=1003*(1024**3),
        node_count=24
    )
)

CLUSTER_QUEUE_LIMITS = {
    #
    # Queues on CyberLAMP cluster
    #
    ('cyberlamp', 'default'): dict(
        jobs=dict(soft=250, hard=500),
        total_cpus=dict(soft=512, hard=1024),
        walltime=timedelta(hours=169),
        eligible_nodes=[
            NODES['cl_default']
        ],
        preemptible=False,
        time_to_queue=0.1,
    ),
    ('cyberlamp', 'open'): dict(
        jobs=dict(soft=512, hard=1024),
        total_cpus=dict(soft=1024, hard=2048),
        walltime=timedelta(hours=96),
        eligible_nodes=[
            NODES['cl_default'],
            NODES['cl_gpu'],
            NODES['cl_higpu'],
            NODES['cl_himem']
        ],
        preemptible=True,
        time_to_queue=0.1,
    ),
    ('cyberlamp', 'cl_gpu'): dict(
        jobs=dict(soft=200, hard=400),
        total_cpus=dict(soft=128, hard=256),
        walltime=timedelta(hours=169),
        eligible_nodes=[
            NODES['cl_gpu']
        ],
        preemptible=False,
        time_to_queue=0.1,
    ),
    ('cyberlamp', 'cl_higpu'): dict(
        jobs=dict(soft=200, hard=400),
        total_cpus=dict(soft=128, hard=256),
        walltime=timedelta(hours=169),
        eligible_nodes=[
            NODES['cl_higpu']
        ],
        preemptible=False,
        time_to_queue=0.1,
    ),
    ('cyberlamp', 'cl_himem'): dict(
        jobs=dict(soft=100, hard=200),
        total_cpus=dict(soft=128, hard=256),
        walltime=timedelta(hours=96),
        eligible_nodes=[
            NODES['cl_himem']
        ],
        preemptible=True,
        time_to_queue=0.1,
    ),
    ('cyberlamp', 'cl_phi'): dict(
        jobs=dict(soft=100, hard=200),
        total_cpus=dict(soft=128, hard=256),
        walltime=timedelta(hours=169),
        eligible_nodes=[
            NODES['cl_phi']
        ],
        preemptible=True,
        time_to_queue=0.1,
    ),

    #
    # Queues on ACI cluster
    #

    ('aci', 'open'): dict(
        jobs=dict(soft=260, hard=260),
        total_cpus=dict(soft=260, hard=260),
        walltime=timedelta(hours=24),
        eligible_nodes=[
            NODES['aci_basic'],
            NODES['aci_stmem0'],
            NODES['aci_stmem1'],
            NODES['aci_himem']
        ],
        preemptible=True,
        time_to_queue=0.1,
    ),
    ('aci', 'dfc13_a_t_bc_default'): dict(
        jobs=dict(soft=180, hard=180*4),
        total_cpus=dict(soft=180, hard=180*4),
        walltime=timedelta(days=365),
        eligible_nodes=[
            NODES['aci_basic']
        ],
        preemptible=False,
        time_to_queue=1.3,
    ),
    ('aci', 'dfc13_a_g_sc_default'): dict(
        jobs=dict(soft=20, hard=20*4),
        total_cpus=dict(soft=20, hard=20*4),
        walltime=timedelta(days=365),
        eligible_nodes=[
            NODES['aci_stmem0'],
            NODES['aci_stmem1'],
        ],
        preemptible=False,
        time_to_queue=1.3,
    ),
}
