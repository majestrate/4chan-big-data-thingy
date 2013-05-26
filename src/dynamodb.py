import generic
from dynamodb_mapper.model import DynamoDBModel as Model
from bs4 import BeautifulSoup as BS
import os, time, threading

for k in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY' ]:
    assert k in os.environ

class ChanPost(Model):
    __table__ = u'chan_posts'
    __hash_key__ = u'board_name'
    __range_key__ = u'post_no'
    __schema__ = {
        u'board_name' : unicode,
        u'post_no' : int,
        u'name' : unicode,
        u'subject' : unicode,
        u'email' : unicode,
        u'body' : unicode,
        u'reply_to' : int,
        u'pic_h' : int,
        u'pic_w' : int,
        u'date_posted' : int,
        u'pic_fname' : unicode,
        u'pic_fsize' : int,
        u'pic_md5' : unicode
        }

    def populate(self,post):
        j = post
        name = 'name' in j and j['name'] or u'Anonymous'
        trip = 'trip' in j and j['trip'] or None
        self.board_name = unicode(j['board_name'])
        self.name = trip and unicode(name+trip) or name
        self.post_no = j['no']
        self.subject = 'sub' in j and j['sub'] or u''
        self.email = 'email' in j and j['email'] or u''
        self.body = 'com' in j and unicode(BS(j['com'].replace('<br>','\n')).text) or u''
        self.reply_to = j['resto']
        self.pic_w = 'w' in j and j['w'] or -1
        self.pic_h = 'h' in j and j['h'] or -1
        self.date_posted = j['time']
        self.pic_fname = 'filename' in j and unicode(j['filename']+j['ext']) or u''
        self.pic_fsize = 'fsize' in j and j['fsize'] or -1
        self.pic_md5 = 'md5' in j and j['md5'] or u''
        return self

    

class chan_db(generic.generic_jobqueue):
    """
    ratelimited database driver
    maybe we'll stay in free tier
    """
    def __init__(self):
        generic.generic_jobqueue.__init__(self)
        self._post_lock = threading.Lock()
        self.sleep = lambda: time.sleep(1.5)
        self._posts = []
        self.limit = 7

    def add_post(self,post):
        """
        add a post to be saved
        """
        self._post_lock.acquire()
        self._posts.append(ChanPost().populate(post))
        self._post_lock.release()
        
    def _commit(self):
        """
        save a few posts
        """
        self._post_lock.acquire()
        for n in range(self.limit):
            if len(self._posts) > 0:
                self._posts.pop().save()
            else:
                break
        self._post_lock.release()

    def update(self):
        # put jobs on that save posts
        # 1 job executed every interval
        self._post_lock.acquire()
        for n in range(int(len(self._posts) / self.limit)):
            self.put_job('db-save-%s'%self.limit,self._commit)
        self._post_lock.release()

def init():
    from dynamodb_mapper.model import ConnectionBorg
    conn = ConnectionBorg()
    conn.create_table(ChanPost,10,10,wait_for_active=True)

if __name__ == '__main__':
    init()
