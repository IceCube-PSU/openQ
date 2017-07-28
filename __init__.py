"""
openQ
"""


__all__ = [
    'DEFAULT_CONFIG', 'DEFAULT_GROUP', 'DEFAULT_CACHE_DIR',
    'CYBERLAMP_QUEUES', 'ACI_QUEUES', 'GPU_GROUPS'
]


DEFAULT_CONFIG = '~jll1062/openQ/config.ini'
DEFAULT_GROUP = 'dfc13_collab'
DEFAULT_CACHE_DIR = '/gpfs/group/dfc13/default/qstat_out'
CYBERLAMP_QUEUES = ['default', 'cl_open', 'cl_gpu', 'cl_higpu', 'cl_himem',
                    'cl_debug', 'cl_phi']
ACI_QUEUES = ['dfc13_a_g_sc_default', 'dfc13_a_t_bc_default', 'open']
GPU_GROUPS = {
    'cyberlamp': ('cyberlamp_collab', 'dfc13_collab', 'dfc13_cyberlamp')
}
