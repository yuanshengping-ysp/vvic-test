# -*- coding: utf-8 -*-
import json,requests,sys


def dingding(message):
    #url = "https://oapi.dingtalk.com/robot/send?access_token=d231022d9ec9baec13bd3d0a9b87bd96c9962708330c2c2152eebda1728023f1"
    url='https://oapi.dingtalk.com/robot/send?access_token=524bd40d0942c3159dc9b6d8f6009f73d1b2f04d54c762e56da7044b86417e2d'
    header = {'Content-Type': 'application/json','charset': 'utf-8'}
    data = {'msgtype': 'text','text': {'content': u'作业告警：'+message+u'失败，请及时处理！'}}
    sendData = json.dumps(data)
    request = requests.post(url,data = sendData,headers = header)


if __name__ == '__main__':
    dingding(sys.argv[1])
