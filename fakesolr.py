#!/usr/bin/python

import json
import urllib
import urllib2
import web

SOLR_URL='http://localhost:8080/solr/citeseerx/select'

urls = (
  '/select', 'select'
)

def query_solr(solrquery):
    params = urllib.urlencode(solrquery)
    url = SOLR_URL + '?' + params
    print "URL: %s" % url
    return json.load(urllib2.urlopen(url))


def query_opensearch(solrquery):
    # GET /api/site/ranking/(key)/(site_qid)
    '/api/site/ranking/'


def merge_results(solr_result, os_result):
    return json.dumps(solr_result, indent=4, separators=(',', ': '))


class select:
    def GET(self):
        solrquery = web.input()
        solr_result = query_solr(solrquery)
        os_result = query_opensearch(solrquery)
        return merge_results(solr_result, os_result)


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
