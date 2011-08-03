Draft project to hash out implicite pipeline with nipype.

If we are not interested in workflows or provenance tracking, the nipype
engine is overkill and forces to use unPythonic style. However it gives
us two nice features:

 a. Not heaving to worry too much about filenames
 b. No recomputations

To implement these, it requires explicite naming of each operation and
writing the control flow as a data-flow program with tracked variable.
This is necessary to do the independence matching between the disk, that
has no notion of variables in and out of scope, and the computation, that
is fully defined by the values of the local variable.

Here, we replace these by automatically generated names using hashs. This
kills provenance tracking but opens the door to using imperative Python
code to assemble the steps.
