"""
Using nipype with persistence and lazy recomputation but without explicit 
name-steps pipeline: getting back scope in command-line based programming.
"""

import os
import hashlib
import pickle
import time
import shutil
import glob

from nipype.interfaces.base import BaseInterface
from nipype.interfaces.traits_extension import File
from nipype.pipeline.engine import Node
try:
    from traits.api import List
except ImportError:
    from enthought.traits.api import List

################################################################################
# Functions for hashing input arguments

def hash_file(filename, exists=False):
    "Return the hash of a file from its file name"
    if exists:
        stat_results = os.stat(filename)
        return filename, stat_results.st_mtime
    return filename


def _hash(value, trait):
    """"Return the static representation to hash from the inputs values
    and specification"""
    if isinstance(trait.trait_type, File):
        return hash_file(value, exists=trait.trait_type.exists)
    elif isinstance(trait.trait_type, List):
        return [_hash(v, trait.inner_traits[0]) for v in value]
    else:
        return value


def hash_inputs(inputs, input_spec, func_name):
    out = dict()
    traits = input_spec.class_traits()
    for name in inputs:
        value = inputs[name]
        if not name in traits:
            raise ValueError('Invalid parameter %s for %s' % (name, func_name))
        trait = traits[name]
        out[name] = _hash(value, trait)
    return out 



################################################################################
# PipeFunc object: callable interface to nipype.interface objects

class PipeFunc(object):
    """ Callable interface to nipype.interface objects

        Use this to wrap nipype.interface object and call them
        specifying their input with keyword arguments::

            fsl_merge = PipeFunc(fsl.Merge, base_dir='.')
            out = fsl_merge(in_files=files, dimension='t')
    """

    def __init__(self, interface, base_dir, callback=None):
        """

            Parameters
            ===========
            interface: a nipype interface class
                The interface class to wrap
            base_dir: a string
                The directory in which the computation will be
                stored
            callback: a callable
                An optional callable called each time after the function
                is called.
        """
        if not (isinstance(interface, type) 
                                and issubclass(interface, BaseInterface)):
            raise ValueError('the interface argument should be a nipype ' 
                             'interface class, but %s (type %s) was passed.' % 
                             (interface, type(interface)))
        self.interface = interface
        base_dir = os.path.abspath(base_dir)
        if not os.path.exists(base_dir) and os.path.isdir(base_dir):
            raise ValueError('base_dir should be an existing directory')
        self.base_dir = base_dir
        doc = '%s\n%s' % (self.interface.__doc__,
                            self.interface.help(returnhelp=True))
        self.__doc__ = doc
        self.callback = callback

    def __call__(self, **kwargs):
        interface = self.interface()
        # Set the inputs early to get some argument checking
        interface.inputs.set(**kwargs)
        # Make a name for our node
        inputs = hash_inputs(kwargs, interface.input_spec,
                                interface.__class__)
        hasher = hashlib.new('md5')
        hasher.update(pickle.dumps(inputs))
        name = '%s-%s_%s' % (interface.__class__.__module__.replace('.', '-'),
                             interface.__class__.__name__, 
                             hasher.hexdigest())
        node = Node(interface, name=name)
        node.base_dir = self.base_dir
        out = node.run()
        if self.callback is not None:
            self.callback(name)
        return out
        
    def __repr__(self):
        return '%s(%s.%s, base_dir=%s)' % (self.__class__.__name__, 
                           self.interface.__module__,
                           self.interface.__name__,
                           self.base_dir)
    

################################################################################
# Memory manager: provide some tracking about what is computed when, to
# be able to flush the disk

class _MemoryCallback(object):
    "An object to avoid closures and have everything pickle"

    def __init__(self, memory):
        self.memory = memory

    def __call__(self, name):
        self.memory._log_name(name)


class Memory(object):

    def __init__(self, base_dir):
        base_dir = os.path.join(os.path.abspath(base_dir), 'nipype_mem')
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)
        elif not os.path.isdir(base_dir):
            raise ValueError('base_dir should be a directory')
        self.base_dir = base_dir
        open(os.path.join(base_dir, 'log.current'), 'w')

    def cache(self, interface):
        """ Returns a cached and callable interface
        """
        return PipeFunc(interface, self.base_dir, _MemoryCallback(self))

    def _log_name(self, name):
        """ Increment counters tracking which cached function get executed.
        """
        base_dir = self.base_dir
        # Every counter is a file opened in append mode and closed
        # immediately to avoid race conditions in parallel computing: 
        # file appends are atomic
        open(os.path.join(base_dir, 'log.current'), 
            'a').write('%s\n' % name)
        t = time.localtime()
        year_dir = os.path.join(base_dir, 'log.%i' % t.tm_year)
        try:
            os.mkdir(year_dir)
        except OSError:
            "Dir exists"
        month_dir = os.path.join(year_dir, '%02i' % t.tm_mon)
        try:
            os.mkdir(month_dir)
        except OSError:
            "Dir exists"
        open(os.path.join(month_dir, '%02i.log' % t.tm_mday), 
            'a').write('%s\n' % name)
     
    def clear_previous_runs(self, warn=True):
        """ Remove all the cache that where not used in the latest run of 
            the memory object: i.e. since the corresponding Python object 
            was created.
        """
        base_dir = self.base_dir
        latest_runs = set(l[:-1] for l in
            open(os.path.join(base_dir, 'log.current'), 'r'))
        self._clear_all_but(latest_runs, warn=warn)

    def clear_runs_since(self, day=None, month=None, year=None, warn=True):
        """ Remove all the cache that where not used since the given date 
        """
        t = time.localtime()
        day = day if day is not None else t.tm_mday
        month = month if month is not None else t.tm_mon
        year = year if year is not None else t.tm_year
        base_dir = self.base_dir
        recently_run = set()
        cut_off_file = '%s/log.%i/%02i/%02i.log' % (base_dir,
                    year, month, day)
        logs_to_flush = list()
        for log_name in glob.glob('%s/log.*/*/*.log' % base_dir):
            if log_name < cut_off_file:
                logs_to_flush.append(log_name)
            else:
                recently_run = recently_run.union(l[:-1] 
                                            for l in open(log_name, 'r'))
        self._clear_all_but(recently_run, warn=warn)
        for log_name in logs_to_flush:
            os.remove(log_name)

    def _clear_all_but(self, runs, warn=True):
        """ Remove all the runs appart from those given to the function
            input.
        """
        dirs = os.listdir(self.base_dir)
        dirs = [d for d in dirs if not d.startswith('log.')]
        old_dirs = list(set(runs).symmetric_difference(dirs))
        for dirname in old_dirs:
            dirname = os.path.join(self.base_dir, dirname)
            if os.path.exists(dirname):
                if warn:
                    print '%s: removing directory: %s' % (self, dirname)
                shutil.rmtree(dirname)

    def __repr__(self):
        return '%s(base_dir=%s)' % (self.__class__.__name__, 
                           self.base_dir)
 
