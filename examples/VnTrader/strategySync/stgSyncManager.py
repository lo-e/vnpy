# coding: utf8

import json
from pymongo import MongoClient
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.app.ctaStrategy.ctaBase import *
from datetime import datetime

class ctaSyncManager(object):
    filePath = 'stgSync.json'
    def __init__(self):
        super(ctaSyncManager, self).__init__()
        self.dbClient = MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'], connectTimeoutMS=500)
        self.db = self.dbClient[POSITION_DB_NAME]
        # 调用server_info查询服务器状态，防止服务器异常并未连接成功
        self.dbClient.server_info()

    def dbToJson(self):
        dataList = []
        for collectionName in self.db.collection_names():
            collectionDic = {}
            collectionList = []

            collection = self.db[collectionName]
            syncList = collection.find()
            for syncData in syncList:
                if '_id' in syncData:
                    del syncData['_id']
                collectionList.append(syncData)
            collectionDic[collectionName] = collectionList
            dataList.append(collectionDic)

        dataJson = json.dumps(dataList)
        f = open(self.filePath, 'w')
        f.write(dataJson)
        f.close()

    def jsonToDb(self):
        f = open(self.filePath)
        dataList = json.load(f)
        for collectionDic in dataList:
            for collectionName in collectionDic:
                syncList = collectionDic[collectionName]
                for syncDic in syncList:
                    collection = self.db[collectionName]
                    flt = {'name':syncDic['name'], 'vtSymbol':syncDic['vtSymbol']}
                    collection.update_many(flt, {'$set': syncDic}, upsert=True)
        f.close()


if __name__ == '__main__':
    manager = ctaSyncManager()
    # 数据库同步到json
    manager.dbToJson()

    # json同步到数据库
    #manager.jsonToDb()