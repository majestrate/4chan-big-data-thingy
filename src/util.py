import traceback, sys

def err():
    """
    print traceback and exit
    """
    traceback.print_exc()
    sys.exit()
