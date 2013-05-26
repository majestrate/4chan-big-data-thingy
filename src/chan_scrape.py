import generic
from bs4 import BeautifulSoup as BS
import time, json


class base_scraper(generic.generic_scraper):
    """
    base 4chan scraper
    """
    def __init__(self,boards,db):
        """
        construct scraper, scrape boards given in list, store to a database backend
        
        boards:
            list of boards to scrape, i.e. ['a','g','b'] for /a/ /g/ and /b/
            
        db:
            database backend object
        """
        generic.generic_scraper.__init__(self)
        self.db = db
        self._boards = dict()
        self.boards = boards
        # sleep for 1.5 seconds between requests
        # obey api rules
        self.sleep = lambda : time.sleep(1.5)

    def update(self):
        """
        issue board updates
        """
        for b in self.boards:
            self.check_board(b)

    def check_board(self,board):
        """
        check all threads on a board for updates
        cache alive threads
        issue updates for newly modified threads
        save and expunge dead threads
        """
        def func():
            # grab threads
            j = self.json('https://api.4chan.org/%s/threads.json'%board)
            if not j:
                self.log('got empty response for board '+board)
                return
            # add board to cache as needed
            if board not in self._boards:
                self._boards[board] = dict()

            # dict for holding alive threads
            alive = dict()

            # iterate pages
            for page in j: 
                # iterate threads on page
                for thread in page['threads']:
                    alive[thread['no']] = None
                    no = thread['no']
                    
                    # add new thread as needed
                    if no not in self._boards[board]:
                        t = dict()
                        t['last_mod'] = 0
                        t['posts'] = dict()
                        self._boards[board][no] = t
                    
                    # check for modified
                    if self._boards[board][no]['last_mod'] < thread['last_modified']: 
                        # if modified update last modified
                        self._boards[board][no]['last_mod'] = thread['last_modified']
                        # update thread 
                        self.update_thread(board,no)
            
            # check for dead threads
            for t in self._boards[board]:
                # if thread is dead
                if t not in alive: 
                    # save and expunge dead threads
                    self.save_thread(board,t)
        
        self.put_job('check-'+board,func)

    def save_thread(self,board,no):
        """
        save a cached thread to the database
        expunge from cache
        """
        def func(): 
            # pop off old thread and save it to the database
            thread = self._boards[board].pop(no)['posts'].values()
            thread_name = '%s-%s'%(board,no)
            for post in thread:
                post['board_name'] = board
            if len(thread) > 0:
                self.put_thread(thread_name,thread)
        self.put_job('save-%s-%s'%(board,no),func)

    def update_thread(self,board,no):
        """
        update cached thread
        """
        def func(): 
            # grab updated thread
            j = self.json('https://api.4chan.org/%s/res/%d.json'%(board,no))
            # if no error
            if j:
                # iterate over posts
                for post in j['posts']:
                    # if new post
                    if post['no'] not in self._boards[board][no]['posts']:
                        # put post in cache
                        self._boards[board][no]['posts'][post['no']] = post
            else:
                self.save_thread(board,no)
        self.put_job('update-%s-%s'%(board,no),func)

    def put_thread(self,name,thread):
        """
        put thread to backend
        IMPLEMENT IN SUBCLASS
        """
        raise NotImplemented()


class chan_scraper(base_scraper):
    """
    4chan scraper
    """
    def put_thread(self,name,thread):
        """
        for each post, add to database 
        """
        def func(db,thread):
            for post in thread:
                db.add_post(post)
        self.db.put_job('db-save-%s'%name,lambda: func(self.db,thread))
