# Website: http://www.javacardos.com

# Date time: 16:57 2015/12/31
Changes:
    1 ADD: Both windows and linux platform supported;

# Date time: 11:28 2015/11/24

Usage:
    1 Copy globalplatform.pyd to Python27\DLLs directory;
    2 Copy all DLL files to system PATH directory;
    3 Use APIs in globalplatformlib.py.
      Example, List reader names:
        import globalplatform as gp
        context = gp.establishContext()
        readernames = gp.listReaders(context)
        for readername in readernames:
            print readername
        gp.releaseContext(context)

# Note:
    1 Python version: python27
    2 System version: Windows x86 platform
