# coding: utf8

import json

class ctaSyncManager(object):
    filePath = 'strategySync.json'
    def __init__(self):
        super(ctaSyncManager, self).__init__()
        pass

    def dbToCSV(self):
        dic = {'a':1,
                'b':2,
                'c':3,
                'x':4,
                'y':5,
                'z':6}
        dicJson = json.dumps(dic)
        f = open(self.filePath, 'w')
        f.write(dicJson)
        f.close()

    def csvToDb(self):
        pass

if __name__ == '__main__':
    manager = ctaSyncManager()
    manager.dbToCSV()