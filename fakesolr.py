#!/usr/bin/python

import argparse
import hashlib
import json
import sys
import urllib
import urllib2
import web

SOLR_URL='http://localhost:8080/solr/citeseerx/select'
OPENSEARCH_URL='http://localhost:5000'

urls = (
  '/select', 'select'
)


class select:
    def GET(self):
        solrquery = web.input()
        solr_result = self.query_solr(solrquery)
        os_result = self.query_opensearch(solrquery)
        return self.merge_results(solr_result, os_result)


    def query_solr(self, solrquery):
        params = urllib.urlencode(solrquery)
        url = SOLR_URL + '?' + params
        print "URL: %s" % url
        return json.load(urllib2.urlopen(url))


    def cleanup(self, query):
        s = urllib.unquote_plus(query)
        s = ' '.join(s.split(','))
        s = ' '.join(s.split())
        s = s.lower()
        return s


    def generate_site_query_id(self, query):
        return hashlib.sha1(query).hexdigest()


    def query_opensearch(self, solrquery):
        query = solrquery['q']
        query = self.cleanup(query)
        site_qid = self.generate_site_query_id(query)
        key = web.ctx.key

        # GET /api/site/ranking/(key)/(site_qid)
        url = '/'.join([OPENSEARCH_URL, 'api/site/ranking', key, site_qid])
        print "URL: %s" % url


    def merge_results(self, solr_result, os_result):
        #return json.dumps(solr_result, indent=4, separators=(',', ': '))
        return json.dumps(solr_result)


class MyApplication(web.application):
    def run(self, port=8080, *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))


KEY=''


def global_variable_processor(handler):
    web.ctx.key = KEY
    return handler()


if __name__ == "__main__":
    print sys.argv
    parser = argparse.ArgumentParser(description='Integrate query results of Solr and TREC OpenSearch and act like a Solr server.')
    parser.add_argument('-k', '--key', type=str, required=True, help='Provide a user key.')
    parser.add_argument('port', help='Port number')

    args = parser.parse_args()
    KEY = args.key
    port = int(args.port)
    app = MyApplication(urls, globals())
    app.add_processor(global_variable_processor)
    app.run(port=port)
