import generic
from sqlalchemy import Table,Column,String,Integer        
from bs4 import BeautifulSoup as BS
import requests
import time, json

__doc__ = """
scraper for scraping 4chan
"""

class chan_db(generic.sql_db):
    """
    database definition for chan scraping
    """

    def gen_tables(self,meta): 
        posts = Table('chan_posts',meta,
                      Column('id',Integer,primary_key=True),
                      Column('post_no',Integer),
                      Column('board_name',String),
                      Column('name',String),
                      Column('subject',String),
                      Column('email',String),
                      Column('body',String),
                      Column('date_posted',Integer),
                      Column('reply_to',Integer),
                      Column('pic_fname',String),
                      Column('pic_fsize',Integer),
                      Column('pic_w',Integer),
                      Column('pic_h',Integer),
                      Column('pic_md5',String)
                      )
        return {'posts':posts}


class chan_scraper(generic.generic_scraper):
    """
    4chan scraper
    """
    def __init__(self,boards,db):
        generic.generic_scraper.__init__(self)
        self.db_exec = db.db_exec
        self._boards = dict()
        self.boards = boards
        # sleep for 1.5 seconds between requests
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
            thread = self._boards[board].pop(no)
            self.put_thread(board,thread['posts'].values())
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

    def put_thread(self,board,thread):
        """
        put cached thread into memory
        """
        def func(conn,table):
            # for each post in thread
            for post in thread:
                post['board_name'] = board
                conn.execute(
                    table.insert(),
                    self.serialize_post(post)
                    )
        self.db_exec('posts',func)

    def serialize_post(self,post):
        j = post
        p = dict()
        name = 'name' in j and j['name'] or 'Anonymous'
        trip = 'trip' in j and j['trip'] or None
        p['name'] = trip and name+trip or name
        p['post_no'] = j['no']
        p['subject'] = 'sub' in j and j['sub'] or None
        p['email'] = 'email' in j and j['email'] or None
        p['body'] = 'com' in j and BS(j['com'].replace('<br>',' ')).text or None
        p['reply_to'] = j['resto']
        p['pic_w'] = 'w' in j and j['w'] or -1
        p['pic_h'] = 'h' in j and j['h'] or -1
        p['date_posted'] = j['time']
        p['pic_fname'] = 'filename' in j and j['filename']+j['ext'] or None
        p['pic_fsize'] = 'fsize' in j and j['fsize'] or -1
        p['pic_md5'] = 'md5' in j and j['md5'] or None
        return p

class amazon_chan_scraper(chan_scraper):
    
    def put_post(self,conn,table):
        pass

    def put_thread(self,board,thread):
        if len(thread) > 0:
            # serialize thread
            for j in thread:
                j['board_name'] = board

            thread = map(self.serialize_post,thread)
            func = lambda con,table:self.put_post(con,table,thread)
            op = thread[0]
            self.db_exec('%s_%s.json'%(board,op['post_no']),func)


class s3_chan_scraper(amazon_chan_scraper):

    def put_post(self,conn,table,thread):
        conn.put_item(table,json.dumps(thread))

class sql_chan_scraper(amazon_chan_scraper):

    def put_thread(self,board,thread):
        if len(thread) > 0:
            def func(conn,table):
                
