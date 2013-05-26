import generic
from bs4 import BeautifulSoup as BS
from jsonrpc.proxy import JSONRPCProxy as RPC
import urlparse, time, hashlib

class generic_web_scraper(generic.generic_scraper):
    """
    generic webscraper , scrapes web pages
    asks a controller for urls to scape
    gives controller found urls, can have repeats
    """
    
    def __init__(self,rpc_url,db):
        generic.generic_scraper.__init__(self)
        self.db_exec = db.db_exec
        self.found_urls = []
        self.got_url = lambda url : url and self.found_urls.append(url)
        self.rpc = RPC.from_url(rpc_url)
        self.sleep = lambda: time.sleep(1)

    def _filter_url(self,base,data):
       
        # truncate comment in data
        if '#' in data:
            data = data[:data.index('#')]
        # length of data
        dl = len(data)
        # absolute url
        if dl < 1:
            return None
        if data[0] != '/' or data[1] == '/':
            return None
        data = base.scheme+'://'+base.netloc+data
        return urlparse.urlparse(data).geturl()
                
    def got_hit(self,base,bs):
        if bs is not None:
            self.got_text(base,bs)
            # find all tags
            for tag in bs.find_all('a'):
                href = tag.get('href')
                if href is not None:
                    url = self._filter_url(base,href)
                    self.got_url(url)

    def update(self):
        if len(self.found_urls) > 0:
            self.rpc.got(urls=self.found_urls)
        self.found_urls = []
        for url in self.rpc.poll(20):
            if url is None:
                continue
            base = urlparse.urlparse(url)
            self.put_job('scrape-'+url,lambda : self.got_hit(base,self.html(url)))
            
    def hash(self,text):
        h = hashlib.new('sha256')
        h.update(text)
        return h.hexdigest()

    def got_text(self,base,bs):
        text = bs.text
        text_hash = self.hash(text.encode('utf-8',errors='replace'))
        text = unicode(text)
        def func(conn,table):
            conn.put_item(table,text)
        fname = '%s.txt' % text_hash
        self.db_exec(fname,func)

    
