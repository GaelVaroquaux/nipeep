Draft project to hash out implicite pipeline with nipype.

If we are not interested in workflows or provenance tracking, the nipype
engine is overkill and forces to use unPythonic style. However it brings
us two nice features:

 a. Not heaving to worry too much about filenames
 b. No recomputations

To implement these, it requires explicit naming of each operation and
writing the control flow as a data-flow program with tracked variable.
These explicit steps are necessary to do the independence matching 
between the disk and the computation. Indeed, the disk-persistence has 
no notion of variables in and out of scope: everything is global. On the 
opposite, the computation is executed on the stack, and is thus fully 
defined by the values of the local variables.

Here we replace these by automatically generated names using hashs. This 
solution forgoes provenance tracking but opens the door to using imperative 
Python code to assemble the steps.
