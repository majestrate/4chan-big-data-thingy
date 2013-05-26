import generic, s3, time

class s3db(generic.generic_db,generic.generic_jobqueue):
    def __init__(self,fname,bucket_name,folder_name='scrapedata'):
        generic.generic_db.__init__(self)
        generic.generic_jobqueue.__init__(self)
        self._s3 = s3.s3_open(fname)
        self._folder = folder_name
        self.bucket = self._s3.get_bucket(bucket_name)

    def put_item(self,fname,data):
        """
        put one item
        """
        def func():
            key = self.bucket.new_key(self._folder+'/%s'%fname)
            if key is not None:
                key.set_contents_from_string(data)
            else:
                self.log('failed to put to '+fname)
        self.put_job('s3-put-%s'%fname,func)

    def update(self):
        time.sleep(.5)

    def close(self):
        pass

    def connect(self):
        return self
