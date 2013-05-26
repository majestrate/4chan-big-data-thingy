#!/usr/bin/env python
# coding: utf-8

import cherrypy
import cpjsonrpcserver as jsonrpc


class url_jsonrpc(jsonrpc.JsonRpcMethods):

    urls = {}

    @cherrypy.expose
    def got(self,urls=[]):
        for url in urls:
            self.add_url(url)
        return 'Okay'

    def add_url(self,url):
        if url not in self.urls:
            self.urls[url] = (False,0)
        else:
            scraped, seen = self.urls[url]
            self.urls[url] = ( scraped , seen + 1 )

    @cherrypy.expose
    def poll(self,num):
        ret = []
        for url in self.urls:
            scraped , seen = self.urls[url]
            if scraped:
                continue
            self.urls[url] = (True, seen+1)
            ret.append(url)
            num -= 1
            if num == 0:
                break
        return ret


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('seeds',metavar='seed',type=str,nargs='+',help='seed urls')
    args = ap.parse_args()
    rpc =  url_jsonrpc()
    for url in args.seeds:
        rpc.add_url(url)
    cherrypy.quickstart(rpc)


if __name__ == "__main__":
    main()
