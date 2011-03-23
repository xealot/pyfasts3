#!/usr/bin/env python
"""
This module is designed to implment a FUSE mount which is a
fast mirror of an S3 volume expressly for the purpose of serving 
files from said S3 volume in a web/webserver environment.

FastS3 subclasses a FUSE Loopback class so it acts like a normal file system 
mirrored to another directory. FastS3 has the main bits that make this 
module smart.

The tricky part here is to coalesce the events in an intelligent way, most 
applications abuse the file system because they are designed with a 
local lag-free interface in mind. They are not wrong to do this, however 
it makes my job harder.

If we edit a file local to the filesystem for instance about 45 events are 
generated. Access checks, opens, reads, releases and forget about it if 
you're using an application like vi which attempts to create and delete a multitude 
of swap files.
"""

import sys, logging, hashlib, base64, \
    traceback, signal, os, time, mimetypes
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.prefix import Prefix
from boto.s3.key import Key
from optparse import OptionParser
from collections import namedtuple, defaultdict
from threading import Lock, Thread
from Queue import Queue
#FUSE STUFF
#ENOENT No such file or dir.
#EACCES Permission Denied
from errno import EACCES 
from os.path import realpath, split, join, isdir, isfile, exists
from fuse import FUSE, Operations

#DEFAULTS
READ_CACHE_EXPIRY = 5
MAX_SYNC_THREADS = 10

#Initiate Logger
logging.basicConfig(format='(%(threadName)-10s) %(message)s',)
log = logging.getLogger('PyFastS3')
log.setLevel(logging.DEBUG)
fslog = logging.getLogger('FUSE')
fslog.setLevel(logging.INFO)

sync_pool = None #In Main
operation_cache = None #In Main

class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True #Why? Freezes without
        self.start()
    
    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try: 
                func(*args, **kargs)
            except Exception, e: 
                print e
            self.tasks.task_done()


class ThreadPool(object):
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        #self.tasks = Queue(num_threads) #Not STRICTLY necessary
        self.tasks = Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)
        
    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))
        
    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()


class OpCache(object):
    def __init__(self, timeout=READ_CACHE_EXPIRY):
        self.timeout = timeout
        self.cache = dict()
        self.timer = dict()
        #memory fragmentation might also kill us here.
        #self.cleaner = Thread(target=OpCache.clean, args=(self.cache, self.timeout))
        #self.cleaner.daemon = True
        #self.cleaner.run()
    
    @staticmethod
    def clean(cache, timeout):
        while True:
            log.debug('Cleaning OPCACHE')
            for k, v in list(cache.items()):
                if cache.get((k, v), 0) + timeout < time.time():
                    del cache[(k, v)]
            time.sleep(10)
    
    def get(self, op, path):
        k = (op, path)
        if self.timer.get(k, 0) > time.time():
            return self.cache.get(k, True)
        return None
    
    def set(self, op, path, value=True, timeout=None):
        k = (op, path)
        self.timer[k] = time.time() + (timeout or self.timeout)
        self.cache[k] = value
        
    def debounce(self, op, path, timeout=None):
        if self.get(op, path) is None:
            self.set(op, path, timeout=timeout)
            return True
        return False
        


class FileLock(object):
    """
    The original loopback file system used a global lock on the read and 
    write methods regardless of which file was being written or read. This 
    is a quick and dirty file lock class which keeps locks based on class.
    """
    LOCKS = {}
    
    def __init__(self, path, blocking=True):
        self.path = path
        self.blocking = blocking
    
    def __enter__(self):
        return self.acquire()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
    
    def acquire(self):
        if self.path in self.LOCKS:
            return self.LOCKS[self.path].acquire(self.blocking)
        self.LOCKS[self.path] = Lock()
        return self.LOCKS[self.path].acquire(self.blocking)
    
    def release(self):
        if self.path in self.LOCKS:
            self.LOCKS[self.path].release()
            #del self.LOCKS[self.path] Keep the locks, memory frag will be worse in the long run.
        else:
            raise Exception('lock is missing')


class LoggingMixIn:
    def __call__(self, op, path, *args):
        fslog.debug('-> %s %s %s' % (op, path, repr(args)))
        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self, op)(path, *args)
            return ret
        except OSError, e:
            ret = str(e)
            raise
        finally:
            fslog.debug('<- %s %s' % (op, repr(ret)))


class Loopback(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        #self.rwlock = Lock() #If this was PER FILE it would be much better
    
    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)
    
    def access(self, path, mode):
        if not os.access(path, mode):
            raise OSError(EACCES)
    
    chmod = os.chmod
    chown = os.chown
    
    def create(self, path, mode):
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)
    
    def flush(self, path, fh):
        return os.fsync(fh)
    
    def fsync(self, path, datasync, fh):
        return os.fsync(fh)
                
    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
    
    getxattr = None
    
    def link(self, target, source):
        return os.link(source, target)
    
    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open
        
    def read(self, path, size, offset, fh):
        with FileLock('local**'+path):
            os.lseek(fh, offset, 0)
            return os.read(fh, size)
    
    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)
        
    readlink = os.readlink
    
    def release(self, path, fh):
        return os.close(fh)
        
    def rename(self, old, new):
        return os.rename(old, self.root + new)
    
    rmdir = os.rmdir
    
    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
    
    def symlink(self, target, source):
        return os.symlink(source, target)
    
    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)
    
    unlink = os.unlink
    utimens = os.utime
    
    def write(self, path, data, offset, fh):
        with FileLock('local**'+path):
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


class FastS3(Loopback):
    """
    This is an attempt to create an eventually consistent file system 
    backed by S3.
    
    If the file exists locally a non blocking check is done in the background 
    to fetch the new version.
    
    If the file doesn't exist locally a blocking check is done in the foreground 
    to fetch the file.
    
    Directory lists are always blocking and in sync.
    
    Writing files happens in the background after the file is released. This includes 
    deleting files.
    """
    def __init__(self, root, bucket):
        super(FastS3, self).__init__(root)
        self.coalesce_thread = Thread()
        self.bucket = bucket
        self.opqueue = defaultdict(set) #For smartly queueing the appropriate action.
        self.opcache = operation_cache #For debouncing and caching methods.

    def getattr(self, path, *args):
        """ We override this method directly since reading a file that isn't 
        present should block until it is fetched from S3. 
        
        The function "read" is not overridden because getattr occurs first and 
        determines whether the path exists or not, thus returning the appropriate 
        error to the OS on failure. """
        if exists(path):
            return super(FastS3, self).getattr(path, *args)
        
        #File does not exist, verify this on S3 before continuing.
        with FileLock(path):
            checked = self.opcache.get('getattr-check', path)
            if checked is None:
                filename = FastS3.local_to_s3(self.root, path)
                log.debug('Checking for missing file on S3: %s' % filename)
                key = bucket.get_key(filename)
                if key is not None:
                    key.get_contents_to_filename(path)
                else:
                    #Check for directory
                    log.debug('Checking for missing dir on S3: %s' % filename+'/')
                    key = bucket.get_key(filename+'/')
                    if key is not None:
                        os.makedirs(path)
                self.opcache.set('getattr-check', path, True)
        return super(FastS3, self).getattr(path, *args)

    def read(self, path, *args):
        """ Queue a verifying read to occur after the file is released 
        to make sure that it is the newest version of the read file. """
        self.opqueue[path].add('read')
        return super(FastS3, self).read(path, *args)
    
    def write(self, path, *args):
        """ Queue a an S3 update to occur after the file is released. """
        self.opqueue[path].add('write')
        return super(FastS3, self).write(path, *args)
    
    def release(self, path, *args):
        """ The application is finished reading or writing the file, now 
        check the queue for any pending actions and add them to workers. """
        actions = self.opqueue.pop(path, ())
        # Write takes priority over read, so check for it first.
        if 'write' in actions:
            sync_pool.add_task(FastS3.s3write, self.bucket, self.root, path)
        elif 'read' in actions:
            if self.opcache.debounce('read', path):
                sync_pool.add_task(FastS3.s3read, self.bucket, self.root, path)
        return super(FastS3, self).release(path, *args)
    
    def unlink(self, path, *args):
        #Remove file from S3 also.
        self.opqueue.pop(path, ()) #Clean queue of any pending actions.
        sync_pool.add_task(FastS3.s3delete, self.bucket, self.root, path)
        return super(FastS3, self).unlink(path, *args)
    
    def mkdir(self, path, *args):
        """ Blocking call to create a directory on S3 """
        FastS3.s3mkdir(self.bucket, self.root, path)
        return super(FastS3, self).mkdir(path, *args)

    def rmdir(self, path, *args):
        """ Non-Blocking call to remove a directory on S3 """
        sync_pool.add_task(FastS3.s3rmdir, self.bucket, self.root, path)
        return super(FastS3, self).rmdir(path, *args)

    def readdir(self, path, *args):
        """ We should always use the S3 version of the directory list and then 
        compare the output with our local FS to see if an update should occur. """
        listing = self.opcache.get('readdir', path)
        if listing is not None:
            return listing
        listing = ['.', '..'] + FastS3.s3readdir(self.bucket, self.root, path)
        self.opcache.set('readdir', path, listing, timeout=3)
        return listing
    
    def rename(self, path, new, *args):
        sync_pool.add_task(FastS3.s3rename, self.bucket, self.root, path, new)
        return super(FastS3, self).rename(path, new, *args)
    
    @staticmethod
    def local_to_s3(root, path):
        if path.startswith(root):
            path = path[len(root):]
        if path.startswith('/'):
            path = path[1:]
        return path
        
    @staticmethod
    def s3write(bucket, root, path):
        #key.set_metadata('mode', str(stat[0]))
        #key.set_metadata('gid', str(stat[5]))
        #key.set_metadata('uid', str(stat[4]))
        #key.set_metadata('mtime', str(stat[8]))
        # GUESS MIME TYPE
        
        #if is_dir:
        #    key.set_contents_from_string("", headers={'Content-Type': 'application/x-directory'})
        
        s3name = FastS3.local_to_s3(root, path)
        log.debug('Executing S3 WRITE BACK for %s' % s3name)
        with FileLock(path):
            key = bucket.get_key(s3name)
            if key is None:
                #New File
                content_type, encoding = mimetypes.guess_type(s3name)
                key = bucket.new_key(s3name)
                log.debug('Writing new file to S3 %s' % s3name)
                key.set_contents_from_filename(path, replace=True,
                                               headers={'Content-Type': content_type})
            else:
                #Check existing file
                content_type, encoding = mimetypes.guess_type(s3name)
                with open(path, 'rb') as f:
                    localmd5 = key.compute_md5(f)[0]
                if key.etag[1:-1] != localmd5:
                    log.info('Overwriting File on S3 %s.' % s3name)
                    key.set_contents_from_filename(path, replace=True,
                                                   headers={'Content-Type': content_type})
        log.debug('S3 WRITE BACK complete')
                
    @staticmethod
    def s3read(bucket, root, path):
        s3name = FastS3.local_to_s3(root, path)
        log.debug('Executing S3 READ CHECK for %s' % s3name)
        with FileLock(path):
            key = bucket.get_key(s3name)
            if key is not None:
                if isfile(path):
                    with open(path, 'rb') as f:
                        localmd5 = key.compute_md5(f)[0]
                    if key.etag[1:-1] != localmd5:
                        log.info('Overwriting File %s from S3.' % s3name)
                        key.get_contents_to_filename(path)
                    else:
                        log.debug('File up to date %s' % s3name)
                else:
                    log.debug('Adding file from S3 %s' % s3name)
                    key.get_contents_to_filename(path)
            else:
                log.debug('Local file does not exist on S3 %s' % s3name)
                sync_pool.add_task(FastS3.s3write, bucket, root, path)
        log.debug('S3 READ CHECK complete')
    
    @staticmethod
    def s3delete(bucket, root, path):
        s3name = FastS3.local_to_s3(root, path)
        with FileLock(path):
            log.debug('Removing %s' % s3name)
            key = bucket.get_key(s3name)
            if key is not None:
                log.info('Cleaned S3 of %s' % s3name)
                key.delete()
            else:
                log.debug('No S3 version to remove %s' % s3name)
    
    @staticmethod
    def s3rename(bucket, root, path, new):
        s3name = FastS3.local_to_s3(root, path)
        s3new = FastS3.local_to_s3(root, new)
        log.debug('Executing S3 RENAME from %s to %s' % (s3name, s3new))
        log.debug('NOT IMPLEMENTED, MOVE BACK')

    @staticmethod
    def s3readdir(bucket, root, path):
        s3name = FastS3.local_to_s3(root, path)
        if s3name != '':
            s3name += '/'
        files = bucket.list(delimiter='/', prefix=s3name)
        listing = []
        for f in files:
            name = str(f.name[len(s3name):]) #Make relative to this dir.
            if isinstance(f, Prefix):
                name = name[:-1] #Remove trailing /
            if name != '':
                listing.append(name)
        log.debug('Listing Directory on S3 "%s" -> %s' % (s3name, listing))
        return listing
    
    @staticmethod
    def s3mkdir(bucket, root, path):
        s3name = FastS3.local_to_s3(root, path)
        with FileLock(path):
            log.debug('Creating Directory on S3 %s' % s3name)
            key = bucket.new_key(s3name+'/')
            key.set_contents_from_string("", headers={'Content-Type': 'application/x-directory'})
    
    @staticmethod
    def s3rmdir(bucket, root, path):
        """ Delete the directory. The OS filesystem empties 
        the directory before removing it so we don't have to do it here. """
        s3name = FastS3.local_to_s3(root, path)
        with FileLock(path):
            log.debug('Removing Directory on S3 %s' % s3name)
            key = bucket.get_key(s3name+'/')
            key.delete()


def sync(root, bucket):
    """ Sync root local FS with specified bucket """
    log.info('Performing synchronization of %s -> %s.' % (root, bucket.name))
    sync_pool = ThreadPool(5)
    
    def check_and_sync(key):
        filename = realpath(join(root, key.name))
        path, name = split(filename)
        if not key.name.endswith('/'):
            if not isfile(filename):
                log.info('Creating File %s.' % name)
                key.get_contents_to_filename(filename)
            else:
                #Check file for correct content.
                with open(filename, 'rb') as f:
                    localmd5 = key.compute_md5(f)[0]
                if key.etag[1:-1] != localmd5:
                    log.info('Overwriting File %s from S3.' % name)
                    key.get_contents_to_filename(filename)
    
    files = bucket.list()
    for key in files:
        #Create local directory and files structure based on S3 bucket.
        filename = realpath(join(root, key.name))
        path, name = split(filename)
        if not isdir(path):
            log.info('Creating Directory %s.' % path)
            os.makedirs(path)
        sync_pool.add_task(check_and_sync, key)
    sync_pool.wait_completion()


if __name__ == '__main__':
    #Create tuple for argslist.
    ArgList = namedtuple('ArgList', 'key secret bucket cache mount')

    #Create option parser.
    parser = OptionParser(usage="usage: %prog [options] AWS_KEY AWS_SECRET BUCKET CACHE_DIRECTORY MOUNT_DIRECTORY",
                          version="%prog 0.1")
    parser.add_option("-s", "--sync", dest="sync", action="store_true", 
                      default=False, help="Sync on start")
    parser.add_option("-d", "--debug", dest="debug", action="store_true", 
                      default=False, help="Print DEBUG output")
    parser.add_option("-w", "--max-wait", dest="maxwaits", action="store_true", 
                      metavar="BACKGROUND_CHECKS", default=100, 
                      help="Maximum number of files to block and fetch until returning an error.")
    (options, args) = parser.parse_args()

    if len(args) != 5:
        print 'Invalid Usage'
        parser.print_help()
        sys.exit(1)

    sync_pool = ThreadPool(MAX_SYNC_THREADS)
    fslog.setLevel(logging.DEBUG if options.debug else logging.INFO)

    arglist = ArgList(key=args[0], secret=args[1], bucket=args[2], cache=args[3], mount=args[4])

    #Open S3 connection and bucket.
    conn = S3Connection(arglist.key, arglist.secret)
    bucket = Bucket(connection=conn, name=arglist.bucket)
    
    if options.sync is True:
        sync(arglist.cache, bucket)

    operation_cache = OpCache()

    signal.signal(signal.SIGINT, signal.SIG_DFL) #Restores CTRL+C functionality
    fuse = FUSE(FastS3(arglist.cache, bucket), arglist.mount, foreground=True)









