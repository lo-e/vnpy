# coding: utf-8

import os
import csv
import collections

class stgEarningManager(object):
    def __init__(self):
        super(stgEarningManager, self).__init__()
        # 项目路径
        # self.dirPath = os.path.dirname(os.path.realpath(__file__)) + '\\csv\\'
        # icloud drive路径
        self.dirPath = ''
        walkingDic = 'C:\\Users'
        for root, subdirs, files in os.walk(walkingDic):
            for sub in subdirs:
                if sub == 'stgEarningCSV':
                    print root
                    self.dirPath = root + '\\stgEarningCSV\\'
        #self.dirPath = 'C:\\Users\\loe\\iCloudDrive\\com~apple~Numbers\\ctaStrategy\\stgEarningCSV\\'

    def loadDailyEarning(self, name = ''):
        result = []
        if (not len(name)) or ( not len(self.dirPath)):
            return result

        # 文件路径
        filePath = self.dirPath + name + '.csv'
        if not os.path.exists(filePath):
            return result

        with open(filePath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                result.append(row)

        return  result

    def updateDailyEarning(self, name = '', content = {}):
        if (not len(name)) or (not len(content) or ( not len(self.dirPath))):
            return

        # 检查是否有文件记录
        loadResult = self.loadDailyEarning(name)

        fieldNames = content.keys()
        # 文件路径
        filePath = self.dirPath + name + '.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()

            # 历史记录重载
            newList = []
            for hisData in loadResult:

                newData = hisData.copy()
                for hisKey in hisData:
                    if not (hisKey in fieldNames):
                        del newData[hisKey]

                for field in fieldNames:
                    if not (field in hisData):
                        newData[field] = ''

                newList.append(newData)

            # 添加新的纪录
            newList.append(content)
            # 写入csv文件
            writer.writerows(newList)

if __name__ == '__main__':
    manager = stgEarningManager()

    print u'1: load \n2: update'
    type = int(raw_input('action:'))
    if type == 1:
        result = manager.loadDailyEarning(name = 'test')
        print result
    elif type == 2:
        # 每日盈亏记录
        fileName = 'test'
        hisData = manager.loadDailyEarning(fileName)
        toltalEarning = 0.0
        offsetEarning = 2.0
        if len(hisData):
            toltalEarning = float(hisData[-1]['累计盈亏'])
        toltalEarning += offsetEarning

        content = collections.OrderedDict()
        content['日期'] = '2018-01-09 06:06:06.666'
        content['开仓价'] = 3806
        content['头寸'] = '多'
        content['平仓价'] = 3808
        content['盈亏'] = offsetEarning
        content['累计盈亏'] = toltalEarning
        manager.updateDailyEarning(fileName, content)