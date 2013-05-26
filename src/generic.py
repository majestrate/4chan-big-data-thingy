from bs4 import BeautifulSoup as BS
import requests
import util
import threading, time, json, datetime, sys


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
    
