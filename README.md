# fakesolr
middle layer that sits between front end and solr to integrate opensearch rankings

## config

```
$ cp config.ini.template config.ini
```

## run server

```
$ python2.7 fakesolr.py -q queries.txt
```

# access

http://localhost:8888/select
