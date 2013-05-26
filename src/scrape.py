#!/usr/bin/env python
import chan_scrape, multi, dynamodb

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('boards', metavar='board', type=str, nargs='+',
                   help='boards to scrape')
    args = ap.parse_args()

    db = dynamodb.chan_db()
    scraper = chan_scrape.chan_scraper(args.boards,db)

    jqs = [ scraper, db ]

    multi.multi_wrapper(jqs).mainloop()


if __name__ == '__main__':
    main()
