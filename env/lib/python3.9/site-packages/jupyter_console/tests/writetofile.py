#-----------------------------------------------------------------------------
# Copyright (C) 2012 The IPython Development Team
#
# Distributed under the terms of the BSD License. The full license is in
# the file LICENSE, distributed as part of this software.
#-----------------------------------------------------------------------------

"""
Copy data from input file to output file for testing.

Command line usage:

    python writetofile.py INPUT OUTPUT

Binary data from INPUT file is copied to OUTPUT file.
If INPUT is '-', stdin is used.

"""

if __name__ == '__main__':
    import sys
    (inpath, outpath) = sys.argv[1:]

    if inpath == '-':
        infile = sys.stdin.buffer
    else:
        infile = open(inpath, 'rb')

    open(outpath, 'w+b').write(infile.read())
