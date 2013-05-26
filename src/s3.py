import boto
from ConfigParser import ConfigParser as CP


def s3_open(fname):
    if fname is None or len(fname) == 0:
        raise Exception('no credfile specfied')
    cp = CP()
    if fname in cp.read(fname):
        access = cp.get('Credentials','aws_access_key_id')
        secret = cp.get('Credentials','aws_secret_access_key')
        return boto.connect_s3(aws_access_key_id=access,aws_secret_access_key=secret)
    else:
        raise Exception('failed to load credentials')

