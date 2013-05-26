from sqlalchemy import create_engine, MetaData
from bs4 import BeautifulSoup as BS
import requests
import util
import threading, time, json, datetime, sys


class generic_db:
    """
    generic database object
    """
    def __init__(self):
        """
        initialize database and construct
        url tells sqlalchemy how to connect to the database
        """
        self._conn = None
        self.tables = dict()

    def err(self):
        self.stop()
        util.err()

    def open(self):
        """
        open database connection
        """
        self.close()
        self._conn = self.connect()

    def connect(self):
        """
        return some kind of connection
        must have attributes: close()
        """
        raise NotImplemented()

    def close(self):
        """
        close database connection
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def db_exec(self,table,func):
        """
        execute database action on table
        func is given 1 parameter, an open connection
        """
        # open database
        self.open()
        try:
            # grab table
            if table is not None and table in self.tables:
                table = self.tables[table]
            # execute function
            func(self._conn,table)
        except:
            self.err()
        # close database
        self.close()

class generic_jobqueue:
    """
    generic scraper with no implementation
    """
    def __init__(self):
        self._q = []
        self._qlock = threading.Lock()
        self.sleep = lambda : None
        self.err = util.err
        self._thread = None

    def log(self,msg,stream=sys.stderr):
        stream.write('['+str(datetime.datetime.now())+'] '+str(msg)+'\n')
        stream.flush()

    def put_job(self,name,func):
        """
        put a job into the job queue
        """
        self._qlock.acquire()
        self._q.append((name,func))
        self._qlock.release()

    def update(self):
        """
        add jobs to scraper
        update state
        """
        raise NotImplemented()

    def run(self):
        """
        mainloop
        """
        self.on = True
        try:
            # while run
            while self.on:
                # if we have jobs
                if len(self._q) > 0:
                    # pop off job
                    self._qlock.acquire()
                    name, j = self._q.pop()
                    self._qlock.release()
                    try:
                        # log execution as needed
                        if name is not None:
                            self.log('exec '+name)
                        # execute job
                        j()
                    except KeyboardInterrupt:
                        raise
                    except:
                        self.err()
                # no jobs
                else:
                    # update state and add more jobs
                    self.update()
                self.sleep()
        except KeyboardInterrupt:
            raise

    def start(self):
        """
        start scraper
        fork off
        """
        self.stop()
        self._thread = threading.Thread(target=self.run,args=())
        self._thread.start()

    def stop(self):
        """
        stop scraper
        """
        self.on = False
        if self._thread is not None:
            self._thread.join()
            self._thread = None

class generic_scraper(generic_jobqueue):

    def __init__(self):
        generic_jobqueue.__init__(self)

    def get(self,url):
        """
        http get request
        """
        try:
            return requests.get(url,headers={'User-Agent':'4chan BIG DATA thingy'}).text
        except:
            return None
        
    def html(self,url):
        """
        http get request 
        get output as html
        """
        try:
            return BS(self.get(url))
        except:
            return None

    def json(self,url):
        """
        http get request
        get output as json
        """
        try:
            return json.loads(self.get(url))
        except:
            return None
    

class sql_db(generic_db):

    def __init__(self,url):
        generic_db.__init__(self)
        # create engine
        self._eng = create_engine(url)
        # generate and create tables
        meta = MetaData()
        self.tables = self.gen_tables(meta)
        meta.create_all(self._eng)
        self.connect = self._eng.connect

    def gen_tables(self,meta):
        """
        generate database tables
        return dict mapping table name to tables
        """
        raise NotImplemented()
