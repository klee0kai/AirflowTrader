import sys, os
from datetime import date, datetime
import utils.inet


def chmodForAll(root, cmod_dir, cmod_file):
    for shortName in os.listdir(root):
        fileName = os.path.join(root, shortName)
        if os.path.isdir(fileName):
            chmodForAll(fileName, cmod_dir, cmod_file)
            os.chmod(fileName, cmod_dir)
        else:
            os.chmod(fileName, cmod_file)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
