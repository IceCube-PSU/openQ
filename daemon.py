import ConfigParser
import getpass
import os, stat
import random
import time

class daemon(object):
    def __init__(self,configfile):
        self.config = ConfigParser.ConfigParser()
        self.config.read(configfile)
        self.users = self.config.get('Users','list').split(',')
        self.myusername = getpass.getuser()
        assert(self.myusername in self.users)
        self.gid = int(self.config.get('Users','gid'))
        for key,_ in self.config.items('Directories'):
            self.setup_dir(key)
        self.queue_stat = {'q':0,'r':0}
        self.n_run = int(self.config.get('Queue','n_run'))
        self.n_queue = int(self.config.get('Queue','n_queue'))

    def getpath(self,dir,usr):
        basedir = self.config.get('Directories','basedir').replace('<!User!>',usr)
        if dir == 'basedir':
            return basedir
        else:
            return os.path.join(basedir,self.config.get('Directories',dir))

    def mkdir(self,path):
        # makedir if doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
        # change group owner
        os.chown(path, -1, self.gid)
        # give group (and user) permissions 
        os.chmod(path, stat.S_IRWXG | stat.S_IRWXU)
    
    def setup_dir(self,dir):
        path = self.getpath(dir,self.myusername)
        self.mkdir(path)

    @property
    def busy(self):
        self.queue_stat['q'] = int(os.popen('qstat | grep "Q open" -c').read().rstrip('\n'))
        self.queue_stat['r'] = int(os.popen('qstat | grep "R open" -c').read().rstrip('\n'))
        # decide if queue is busy
        if self.queue_stat['r'] < self.n_run and self.queue_stat['q'] < self.n_queue:
            return False
        else:
            return True

    def submit(self):
        if not self.busy:
            found_task = False
            while not found_task:
                # choose a user
                usr = random.choice(self.users)
                dir = self.getpath('job',usr)
                if os.path.exists(dir):
                    jobs = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
                    if len(jobs) > 0:
                        found_task = True
                time.sleep(1)
            self.qsub(usr,jobs[0])

    def qsub(self,usr,job):
        os.popen('qsub %s'%os.path.join(self.getpath('job',usr),job))
        try:
            # move job to submitted directory
            os.rename(os.path.join(self.getpath('job',usr),job), os.path.join(self.getpath('sub',usr),job))
        except:
            # another worker might have moved it already at the same time
            pass

    def serve_forever(self):
        while True:
            self.submit() 
            time.sleep(1)


if __name__ == '__main__':
    my_daemon = daemon('/storage/home/pde3/openQ/config.ini')
    my_daemon.serve_forever()
