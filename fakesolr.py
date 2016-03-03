#!/usr/bin/python

import argparse
import ConfigParser
import hashlib
import json
import random
import sys
import urllib
import urllib2
import web


urls = (
  '/select', 'select'
)


def generate_site_query_id(query):
    return hashlib.sha1(query).hexdigest()


def process_queries_file(queries_file):
    qids = set()
    with open(queries_file) as f:
        for line in f:
            query = line.strip()
            qid = generate_site_query_id(query)
            qids.add(qid)
    return qids


class select:
    def GET(self):
        self.qids = web.ctx.qids
        self.solr_url = web.ctx.solr_url
        self.opensearch_url = web.ctx.opensearch_url
        self.opensearch_key = web.ctx.opensearch_key

        solrquery = web.input()
        if 'start' in solrquery:
            start = int(solrquery['start'])
        else:
            start = 0

        if 'rows' in solrquery:
            rows = int(solrquery['rows'])
        else:
            rows = 10

        if start != 0:
            solrquery['start'] = 0
            solrquery['rows'] = start + rows

        print solrquery
        solr_result = self.query_solr(solrquery)

        #    response = solr_result['response']
        #    rows = len(response['docs'])
        #print 'ROWS', rows

        os_result = self.query_opensearch(solrquery)
        return self.merge_results(solr_result, os_result, start, rows)


    def query_solr(self, solrquery):
        params = urllib.urlencode(solrquery)
        url = self.solr_url + '?' + params
        print "URL: %s" % url
        return json.load(urllib2.urlopen(url))

    def query_solr_for_doi(self, doi):
        f = {
            'q': '*:*',
            'wt': 'json',
            'fq': 'doi:' + doi,
        }
        param = urllib.urlencode(f)
        url = self.solr_url + '?' + param
        print "URL for %s: %s" % (doi, url)
        solr_result = json.load(urllib2.urlopen(url))
        response = solr_result['response']
        docs = response['docs']
        if not docs:
            return None
        else:
            return docs[0]

    def cleanup(self, query):
        s = urllib.unquote_plus(query)
        s = ' '.join(s.split(','))
        s = ' '.join(s.split())
        s = s.lower()
        return s


    def query_opensearch(self, solrquery):
        query = solrquery['q']
        query = self.cleanup(query)
        site_qid = generate_site_query_id(query)

        if self.qids is not None and site_qid not in self.qids:
            return {}

        # GET /api/site/ranking/(key)/(site_qid)
        url = '/'.join([self.opensearch_url, 'api/site/ranking', self.opensearch_key, site_qid])
        print "URL: %s" % url
        try:
            return json.load(urllib2.urlopen(url))
        except urllib2.URLError as e:
            print 'OpenSearch: not found'
            return {}


    def fix_solr_result(self, solr_result, start, rows):
        header = solr_result['responseHeader']
        params =  header['params']
        params['start'] = start
        params['rows'] = rows

        response = solr_result['response']
        response['start'] = start
        response['rows'] = rows
        response['docs'] = response['docs'][start:start+rows]


    def assign_default_team(self, solr_result):
        response = solr_result['response']
        for doc in response['docs']:
            doc['team'] = 'd'


    def merge_doclists(self, solr_doclist, os_doclist, max_len):
        # team draft interleaving algorithm
        solr_docs_map = {doc['doi']: doc for doc in solr_doclist if 'doi' in doc}
        os_doi_list = [doc['site_docid'] for doc in os_doclist]
        new_doclist = []
        solr_i = 0
        os_i = 0
        solr_team = []
        os_team = []
        selected= []
        while solr_i < len(solr_doclist):
            doc = solr_doclist[solr_i]
            if 'doi' not in doc:
                break
            if doc['doi'] != os_doi_list[os_i]:
                break
            doc['team'] = 'x'
            new_doclist.append(doc)
            solr_i += 1
            os_i += 1

        while len(new_doclist) < max_len:
            if solr_i >= len(solr_doclist) and os_i >= len(os_doi_list):
                break
            if solr_i >= len(solr_doclist) or len(solr_team) > len(os_team):
                solr_turn = False
            elif os_i >= len(os_doi_list) or len(solr_team) < len(os_team):
                solr_turn = True
            else:
                solr_turn = bool(random.getrandbits(1))

            if solr_turn:
                doc = solr_doclist[solr_i]
                solr_i += 1
                if 'doi' in doc:
                    doi = doc['doi']
                    if doi in selected:
                        continue
                    selected.append(doi)
                    solr_team.append(doi)
                    print "Solr pick (%s) %s" % (solr_i, doi)
                else:
                    # there should always be id field
                    solr_team.append(doc['id'])
                    print "Solr pick (%s) %s" % (solr_i, doc['id'])
                doc['team'] = 's'
            else:
                doi = os_doi_list[os_i]
                os_i += 1
                if doi in selected:
                    continue

                print "Open pick (%s) %s" % (os_i, doi)
                selected.append(doi)
                os_team.append(doi)
                if doi in solr_docs_map:
                    doc = solr_docs_map[doi]
                else:
                    print 'need to query solr for doi', doi
                    doc = self.query_solr_for_doi(doi)
                    if not doc:
                        print 'OpenSearch picked', doi, 'not foudn in solr (weird)'
                        continue
                doc['team'] = 'p'
            new_doclist.append(doc)
        return new_doclist


    def json_dumps(self, data):
        return json.dumps(data, indent=4, separators=(',', ': '))
        return json.dumps(data)


    def merge_results(self, solr_result, os_result, start, rows):
        if not os_result:
            self.assign_default_team(solr_result)
            self.fix_solr_result(solr_result, start, rows)
            return self.json_dumps(solr_result)

        response = solr_result['response']
        solr_doclist = response['docs']
        os_doclist = os_result['doclist']
        os_sid = os_result['sid']
        response['docs'] = self.merge_doclists(solr_doclist, os_doclist, start+rows)
        response['ossid'] = os_sid

        self.fix_solr_result(solr_result, start, rows)
        return self.json_dumps(solr_result)


class MyApplication(web.application):
    def run(self, port=8080, *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))


qids = None
solr_url = ''
opensearch_url = ''
opensearch_key = ''

def global_variable_processor(handler):
    web.ctx.qids = qids
    web.ctx.solr_url = solr_url
    web.ctx.opensearch_url = opensearch_url
    web.ctx.opensearch_key = opensearch_key
    return handler()


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    port = config.getint('fakesolr', 'port')
    solr_url = config.get('solr', 'url')
    opensearch_url = config.get('opensearch', 'url')
    opensearch_key = config.get('opensearch', 'key')

    parser = argparse.ArgumentParser(description='Integrate query results of Solr and TREC OpenSearch and act like a Solr server.')
    parser.add_argument('-q', '--queries_file', help='specify queries file')

    args = parser.parse_args()
    if args.queries_file:
        qids = process_queries_file(args.queries_file)

    app = MyApplication(urls, globals())
    app.add_processor(global_variable_processor)
    app.run(port)
