"""
Using nipype in an imperative way, but without dealing with explicit file naming:
bringing back scope to command-line programming.

Note that in the following example, we are calling command-lines with disk I/O that 
persists across runs, but we never have to worry about the file names or the 
directories.

The disk location of the persistence is encoded by hashes. To find out where an 
operation has been persisted, simply look in it's output variable::

    out.runtime.cwd
"""

################################################################################
# Setup the FSL environment to work with neurodebian
import os
FSL_BASE = '/usr/share/fsl/4.1'
os.environ['FSLDIR'] = FSL_BASE
os.environ['PATH'] = '%s:%s/bin' % (os.environ['PATH'], FSL_BASE)
os.environ['LD_LIBRARY_PATH'] = '%s:/usr/lib/fsl/4.1/' % (
                        os.environ.get('LD_LIBRARY_PATH', ''))
from nipype.interfaces import fsl
fsl.FSLCommand.set_default_output_type('NIFTI')

################################################################################
from nipeep import Memory
import glob
in_files = glob.glob(os.path.join(FSL_BASE, 'data', 
                                        'standard', 'MNI152_T1_2mm*')) 

################################################################################
mem = Memory('.')

threshold = [mem.cache(fsl.Threshold)(in_file=f, thresh=i)
                        for i, f in enumerate(in_files)]

out_merge = mem.cache(fsl.Merge)(dimension="t",
                            in_files=[t.outputs.out_file for t in threshold], 
                        )

out_mean = mem.cache(fsl.MeanImage)(in_file=out_merge.outputs.merged_file)

# Avoid having increasing disk size: keep only what was touch in this run
mem.clear_previous_runs()

#mem.clear_runs_since(year=2011)



