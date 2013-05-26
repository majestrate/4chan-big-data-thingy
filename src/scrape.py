#!/usr/bin/env python
import webscraper, chan_scrape, multi, s3_db, os


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('boards', metavar='board', type=str, nargs='+',
                   help='boards to scrape')
    #ap.add_argument('--rpc',type=str,help='controller rpc url')
    ap.add_argument('--bucket',type=str,help='s3 bucket')
    #ap.add_argument('--db-url',type=str,help='database url',
    #                default='postgresql://ubuntu:ubuntu@localhost/ubuntu')
    #ap.add_argument('--s3',action='store_const',default=False,const=True)
    ap.add_argument('--s3-cfg',default=os.path.join(os.environ['HOME'],'boto.cfg'))
    #ap.add_argument('--jobs',type=int,default=1,help='number of scraper jobs')
    args = ap.parse_args()
    cfg = args.s3_cfg
    bucket = args.bucket
    db = s3_db.s3db(cfg,bucket) 
    #cs_class = args.s3 and chan_scrape.s3_chan_scraper or chan_scrape.chan_scraper
    #scrape_class = webscraper.generic_web_scraper 
    scraper_class = chan_scrape.s3_chan_scraper
    jqs = [ scraper_class(args.boards,db) ]
    #jqs = [ scrape_class(args.rpc,db) for n in range(args.jobs) ]
    jqs.append(db)
    multi.multi_wrapper(jqs).mainloop()


if __name__ == '__main__':
    main()
