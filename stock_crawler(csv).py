# -*- coding: utf-8 -*-
"""
Created on Sat Apr 21 11:35:09 2018

@author: Allen
"""

import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import csv
import pandas as pd
import os
from collections import defaultdict

class stockCrawler():
    # 建構子
    def __init__(self, path = 'data'):
        print("stockCrawler Start!")
        self.path = path
    
    # 初始化 scraping 時間    
    def time(self):
        return time.time()
    
    # 計算scraping時間
    def elapsedTime(self, start, end):
        return end - start
    
    # 日期格式化處理, 106/05/01
    def dateFormat(self, year, month, day):
        return '{0}/{1:02d}/{2:02d}'.format(year - 1911, month, day)
    
    # 日期格式化處理, 20170501
    def dateFormat2(self, year, month, day):
        return '{0}{1:02d}{2:02d}'.format(year, month, day)
    
    # getTseBasicData 因為日期區間不同, 故需要不同的table順序
    def checkTseBasicData(self, date):
        #2004/02/11 ~ 2008/12/31
        if datetime(2004, 2, 11) <= date <= datetime(2008, 12, 31) :
            return 1
        #2009/01/06 ~ 2011/07/29
        elif datetime(2009, 1, 6) <= date <= datetime(2011, 7, 29) :
            return 3
        #2011/08/02 ~ today
        else:
            return 4
    
    # data 格式化處理    
    def dataFormat(self, string):
        return string.strip().replace(',', '')
    
    # 將 - 轉成 0
    def symbolToZero(self, string):
        if string == '-' or string == 'N/A':
            string = 0
        return string
    
    # 定義 requests.post
    def defineRequest(self, url, payload, tableSerial):
        
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36"}
        
        html = requests.post(url, headers=headers, data=payload)
        
        #20170414 修正
        #sp = BeautifulSoup(html.text, 'html.parser')
        sp = BeautifulSoup(html.text, 'lxml')
        
        tables = sp.select("table > tbody")
        
        if(len(tables) >= tableSerial):
            return tables[tableSerial].find_all('tr')
        else:
            return 0
        
    def writeCSV(self, stockId, dataRow):
        f = open('{}/{}.csv'.format(self.path, stockId), 'a')
        cw = csv.writer(f, lineterminator='\n')
        if os.stat('{}/{}.csv'.format(self.path, stockId)).st_size == 0:
            title = ['日期', '股數', '開盤價', '最高價', '最低價', '收盤價', '漲跌差', '本益比', '殖利率', '股價淨值比', '外資買賣超', '投信買賣超', '自營商買賣超', '融資買進', '融資賣出', '融資變化', '融資餘額', '融券買進', '融券賣出', '融券變化', '融券餘額']
            cw.writerow(title)
        cw.writerow(dataRow)
        f.close()
    """
    上市 基本資料
    """  
    # 撈取上市 基本資料
    def getTseBasicData(self, date):
        # 106/05/29
        dateString = self.dateFormat(date.year, date.month, date.day)
        # 20170529
        dateString2 = self.dateFormat2(date.year, date.month, date.day)
        
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + dateString2 + '&type=ALLBUT0999'
        
        payload = {
        }
        
        tableSerial = self.checkTseBasicData(date)
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        data = {}
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            """
            if  (stockId != '2330' and stockId != '2317'):
                continue
            """
            # 股票名稱
            #stockName = self.dataFormat(tds[1].text)
            # 成交股數
            stockShare = self.dataFormat(tds[2].text)
            # 開盤價
            stockOpeningPrice = self.dataFormat(tds[5].text)
            # 最高價
            stockHighestPrice = self.dataFormat(tds[6].text)
            # 最低價
            stockLowestPrice = self.dataFormat(tds[7].text)
            # 收盤價
            stockClosingPrice = self.dataFormat(tds[8].text)
            # 漲跌
            sign = self.dataFormat(tds[9].text)
            #print(sign)
            #sign = '-' if len(sign) == 1 and sign[0] == u'－' else ''
            # 漲跌差
            diff = sign + self.dataFormat(tds[10].text)
            # '日期', '股數', '開盤價', '最高價', '最低價', '收盤價', '漲跌差'
            tmp = [dateString, stockShare, stockOpeningPrice, stockHighestPrice, stockLowestPrice, stockClosingPrice, diff]
            data[stockId] = tmp
            #self.writeCSV(stockId, data)
        return data

    """
    上市 三大法人買賣超日報
    """               
    def getTseLegalByAndSell(self, date):
        # 106/05/29
        #dateString = self.dateFormat(date.year, date.month, date.day)
        # 20170529
        dateString2 = self.dateFormat2(date.year, date.month, date.day)
        
        url = 'http://www.twse.com.tw/fund/T86?response=html&date=' + dateString2 + '&selectType=ALLBUT0999'
        
        payload = {
        }
        
        tableSerial = 0
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        data = {}
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            """
            if  (stockId != '2330' and stockId != '2317'):
                continue
            """
            # 外資買賣超股數
            foreignTotal = self.dataFormat(tds[4].text)
            # 投信買賣超股數
            trustTotal = self.dataFormat(tds[7].text)
            # 自營商買賣超股數
            dealerTotal = self.dataFormat(tds[8].text)
            # '外資買賣超', '投信買賣超', '自營商買賣超'
            tmp = [foreignTotal, trustTotal, dealerTotal]
            data[stockId] = tmp
        return data
    
    """
    上市 融資券餘額
    """   
    def getTseMargin(self, date):
        # 106/05/29
        #dateString = self.dateFormat(date.year, date.month, date.day)
        # 20170529
        dateString2 = self.dateFormat2(date.year, date.month, date.day)
        
        url = 'http://www.twse.com.tw/exchangeReport/MI_MARGN?response=html&date=' + dateString2 + '&selectType=ALL'
        
        payload = {
        }
        
        tableSerial = 1
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        data = {}
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            """
            if  (stockId != '2330' and stockId != '2317'):
                continue
            """
            # 股票名稱
            #stockName = self.dataFormat(tds[1].text)
            # 融資買進
            financingIn = self.dataFormat(tds[2].text)
            # 融資賣出
            financingOut = self.dataFormat(tds[3].text)
            # 融資變化
            financingDiff = int(financingIn) - int(financingOut)
            # 融資前日餘額
            #financingBefore = self.dataFormat(tds[5].text)
            # 融資今日餘額
            financingToday = self.dataFormat(tds[6].text)
            # 融券買進
            marginIn = self.dataFormat(tds[8].text)
            # 融券賣出
            marginOut = self.dataFormat(tds[9].text)
            # 融券變化
            marginDiff = int(marginIn) - int(marginOut)
            # 融券前日餘額
            #marginBefore = self.dataFormat(tds[11].text)
            # 融券今日餘額
            marginToday = self.dataFormat(tds[12].text)
            # '融資買進', '融資賣出', '融資變化', '融資餘額', '融券買進', '融券賣出', '融券變化', '融券餘額'
            tmp = [financingIn, financingOut, financingDiff, financingToday, marginIn, marginOut, marginDiff, marginToday]
            
            data[stockId] = tmp
        return data
 
    """
    上市 本益比, 殖利率, 股價淨值比
    """           
    def getTsePeratioData(self, date):
        # 106/05/29
        #dateString = self.dateFormat(date.year, date.month, date.day)
        # 20170529
        dateString2 = self.dateFormat2(date.year, date.month, date.day)
        
        url = 'http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date=' + dateString2 + '&selectType=ALL'
        
        payload = {
        }
        
        tableSerial = 0
        
        data = {}
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            """
            if  (stockId != '2330' and stockId != '2317'):
                continue
            """
            # 本益比 pe
            # 殖利率 yieldRate
            # 股價淨值比 pbr
            if date <= datetime(2017, 4, 12) :
                pe = self.dataFormat(tds[2].text)
                yieldRate = self.dataFormat(tds[3].text)
                pbr = self.dataFormat(tds[4].text)
            else :
                pe = self.dataFormat(tds[4].text)
                yieldRate = self.dataFormat(tds[2].text)
                pbr = self.dataFormat(tds[5].text)
                
            pe = self.symbolToZero(pe)
            yieldRate = self.symbolToZero(yieldRate)
            pbr = self.symbolToZero(pbr)
            # '本益比', '殖利率', '股價淨值比'
            tmp = [pe, yieldRate, pbr]
            
            data[stockId] = tmp
        return data
    
    """
    上櫃 基本資料
    """               
    def getOtcBasicData(self, date):
        # 106/05/29
        dateString = self.dateFormat(date.year, date.month, date.day)
        
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_print.php?l=zh-tw&d={}&s=0,asc,0'.format(dateString)

        payload = {}
        
        tableSerial = 0
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        data = {}
        
        # rows - 1 扣除多餘資料
        for i in range(0, len(rows) - 1):
            tds = rows[i].find_all('td')
            if tds[0].text[:1] == '7' and len(tds[0].text) > 4:
                continue
            else:
                # 股票代號
                stockId = self.dataFormat(tds[0].text)
                # 避開當天暫停交易之股票
                if stockId == '管理股票':
                    continue
                # 股票名稱
                #stockName = self.dataFormat(tds[1].text)
                # 成交股數
                stockShare = self.dataFormat(tds[8].text)
                # 開盤價
                stockOpeningPrice = self.dataFormat(tds[4].text)
                # 最高價
                stockHighestPrice = self.dataFormat(tds[5].text)
                # 最低價
                stockLowestPrice = self.dataFormat(tds[6].text)
                # 收盤價
                stockClosingPrice = self.dataFormat(tds[2].text)
                # 漲跌差
                sign = tds[3].text[:1]
                if sign == '+':
                    sign = ''
                    diff = sign + self.dataFormat(tds[3].text[1:])
                elif sign == '-':
                    sign = '-'
                    diff = sign + self.dataFormat(tds[3].text[1:])
                else:
                    diff = self.dataFormat(tds[3].text)
            # '日期', '股數', '開盤價', '最高價', '最低價', '收盤價', '漲跌差'
            tmp = [dateString, stockShare, stockOpeningPrice, stockHighestPrice, stockLowestPrice, stockClosingPrice, diff]
            data[stockId] = tmp
        return data
    """
    上櫃 本益比, 殖利率, 股價淨值比
    """             
    def getOtcPeratioData(self, date):
        # 106/05/29
        dateString = self.dateFormat(date.year, date.month, date.day)
        
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_print.php?l=zh-tw&d={}&c=&s=0,asc,0'.format(dateString)
        
        payload = {}
        
        tableSerial = 0
        
        data = {}
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            # 本益比
            pe = self.dataFormat(tds[2].text)
            pe = self.symbolToZero(pe)
            # 殖利率
            yieldRate = self.dataFormat(tds[4].text)
            yieldRate = self.symbolToZero(yieldRate)
            # 股價淨值比
            pbr = self.dataFormat(tds[5].text)
            pbr = self.symbolToZero(pbr)
            # '本益比', '殖利率', '股價淨值比'
            tmp = [pe, yieldRate, pbr]
            
            data[stockId] = tmp
        return data
    """
    上櫃 三大法人買賣超日報
    """            
    def getOtcLegalByAndSell(self, date):   
        # 106/05/29
        dateString = self.dateFormat(date.year, date.month, date.day)
        
        url = 'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_print.php?l=zh-tw&se=EW&t=D&d={}&s=0,asc'.format(dateString)
        
        payload = {}
        
        data = {}
        
        tableSerial = 0
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            # 外資買賣超股數
            foreignTotal = self.dataFormat(tds[4].text)
            # 投信買賣超股數
            trustTotal = self.dataFormat(tds[7].text)
            # 自營商買賣超股數
            dealerTotal = self.dataFormat(tds[8].text)
            # '外資買賣超', '投信買賣超', '自營商買賣超'
            tmp = [foreignTotal, trustTotal, dealerTotal]
            data[stockId] = tmp
        return data
    
    """
    上櫃 融資券餘額
    """
    def getOtcMargin(self, date): 
        # 106/05/29
        dateString = self.dateFormat(date.year, date.month, date.day)
        
        url = 'http://www.tpex.org.tw/web/stock/margin_trading/margin_sbl/margin_sbl_print.php?l=zh-tw&d={}&s=0,asc,0'.format(dateString)
        
        payload = {}
        
        tableSerial = 0
        
        data = {}
        
        rows = self.defineRequest(url, payload, tableSerial)
        
        for i in range(0, len(rows)):
            tds = rows[i].find_all('td')
            # 股票代號
            stockId = self.dataFormat(tds[0].text)
            # 股票名稱
            #stockName = self.dataFormat(tds[1].text)
            # 融資買進
            financingIn = self.dataFormat(tds[4].text)
            # 融資賣出
            financingOut = self.dataFormat(tds[3].text)
            # 融資變化
            financingDiff = int(financingIn) - int(financingOut)
            # 融資前日餘額
            #financingBefore = self.dataFormat(tds[2].text)
            # 融資今日餘額
            financingToday = self.dataFormat(tds[6].text)
            # 融券買進
            marginIn = self.dataFormat(tds[10].text)
            # 融券賣出
            marginOut = self.dataFormat(tds[9].text)
            # 融券變化
            marginDiff = int(marginIn) - int(marginOut)
            # 融券前日餘額
            #marginBefore = self.dataFormat(tds[8].text)
            # 融券今日餘額
            marginToday = self.dataFormat(tds[12].text)
            # '融資買進', '融資賣出', '融資變化', '融資餘額', '融券買進', '融券賣出', '融券變化', '融券餘額'
            tmp = [financingIn, financingOut, financingDiff, financingToday, marginIn, marginOut, marginDiff, marginToday]
            
            data[stockId] = tmp
        return data


        
def main():
    # 2017/04/01, Crawling from 2008/01/02 to 2011/07/29
    # 2017/04/01, Crawling from 2011/08/02 to 2017/04/01
    
    
    #lastDate = datetime.today()
    #nowDate = datetime.today()
    
    #lastDate = datetime(2018, 5, 4)
    #20180506 12:44 crawl 2008~2010
    #20180506 17:00 crawl 2011~2011
    #20180506 19:27 crawl 2012~2012
    #20180512 22:45 crawl 2013~2014
    #20180513 08:51 crawl 2015~2016
    #20180513 14:00 crawl 2017~2018/05/11
    lastDate = datetime(2019, 3, 26)
    nowDate = datetime(2019, 3, 25)
    
    legalNum = 3
    marginNum = 8
    peratioNum = 3
    
    crawler = stockCrawler()
    
    
    maxError = 15
    errorTimes = 0
    
    startTime = crawler.time()
    
    result = defaultdict(dict)
    while errorTimes < maxError and nowDate <= lastDate:
        # 文字訊息用
        showDate = crawler.dateFormat2(nowDate.year, nowDate.month, nowDate.day)
        keyDate = crawler.dateFormat(nowDate.year, nowDate.month, nowDate.day)
        try:
            print('---- Crawling Starts On ' + showDate + ' ----')
            """
            tseBasicData = {}
            tseLegelData = {}
            tseMarginData = {}
            tsePeratioData = {}
            otcBasicData = {}
            otcLegelData = {}
            otcMarginData = {}
            otcPeratioData = {}
            """
            stockBasicData = {}
            stockLegelData = {}
            stockMarginData = {}
            stockPeratioData = {}
            # 撈取上市 個股基本資料
            print('getTseBasicData Starts!')
            #tseBasicData = crawler.getTseBasicData(nowDate)
            stockBasicData.update(crawler.getTseBasicData(nowDate))
            print('getTseBasicData Finished!')
            time.sleep(3)
            # 撈取上櫃 個股基本資料
            print('getOtcBasicData Starts!')
            #otcBasicData = crawler.getOtcBasicData(nowDate)
            stockBasicData.update(crawler.getOtcBasicData(nowDate))
            print('getOtcBasicData Finished!')
            time.sleep(3)
            # 撈取上市 本益比, 殖利率, 股價淨值比
            if nowDate >= datetime(2005, 9, 1):
                print('getTsePeratioData Starts!')
                #tsePeratioData = crawler.getTsePeratioData(nowDate)
                stockPeratioData.update(crawler.getTsePeratioData(nowDate))
                print('getTsePeratioData Finished!')
                time.sleep(3)
            # 撈取上櫃 本益比, 殖利率, 股價淨值比
            if nowDate >= datetime(2007, 1, 2):
                print('getOtcPeratioData Starts!')
                #otcPeratioData = crawler.getOtcPeratioData(nowDate)
                stockPeratioData.update(crawler.getOtcPeratioData(nowDate))
                print('getOtcPeratioData Finished!')
                time.sleep(3) 
            # 撈取上市 三大法人買賣超日報
            if nowDate >= datetime(2012, 5, 2):
                print('getTseLegalByAndSell Starts!')
                #tseLegelData = crawler.getTseLegalByAndSell(nowDate)
                stockLegelData.update(crawler.getTseLegalByAndSell(nowDate))
                print('getTseLegalByAndSell Finished!')
                time.sleep(3)
            # 撈取上櫃 三大法人買賣超日報
            if nowDate >= datetime(2014, 12, 1):
                print('getOtcLegalByAndSell Starts!')
                #otcLegelData = crawler.getOtcLegalByAndSell(nowDate)
                stockLegelData.update(crawler.getOtcLegalByAndSell(nowDate))
                print('getOtcLegalByAndSell Finished!')
                time.sleep(3) 
            # 撈取上市 個股融資券
            if nowDate >= datetime(2001, 1, 2):
                print('getTseMargin Starts!')
                #tseMarginData = crawler.getTseMargin(nowDate)
                stockMarginData.update(crawler.getTseMargin(nowDate))
                print('getTseMargin Finished!')
                time.sleep(3)
            # 撈取上櫃 個股融資券
            if nowDate >= datetime(2012, 10, 2):
                print('getOtcMargin Starts!')
                #otcMarginData = crawler.getOtcMargin(nowDate)
                stockMarginData.update(crawler.getOtcMargin(nowDate))
                print('getOtcMargin Finished!')                
            print('---- Crawling Finished On ' + showDate + ' ----')
            #print(tseBasicData)
            #print(tsePeratioData)
            #print(tseLegelData)
            #print(tseMarginData)
            for stockId, basicData in stockBasicData.items():
                tmpList = []
                tmpDict = {}
                # append TSE basic data to tmpList
                for basic in basicData:
                    tmpList.append(basic)
                # if stockPeratioData has data
                if stockPeratioData.get(stockId) is not None:
                    for peratioData in stockPeratioData[stockId]:
                        tmpList.append(peratioData)
                else:
                    for i in range(peratioNum):
                        tmpList.append('')   
                # if stockLegelData has data
                if stockLegelData.get(stockId) is not None:
                    for legelData in stockLegelData[stockId]:
                        tmpList.append(legelData)
                else:
                    for i in range(legalNum):
                        tmpList.append('')    
                        
                # if stockMarginData has data
                if stockMarginData.get(stockId) is not None:
                    for marginData in stockMarginData[stockId]:
                        tmpList.append(marginData)
                else:
                    for i in range(marginNum):
                        tmpList.append('')
                
                tmpDict[keyDate] = tmpList 
                
                result[stockId].update(tmpDict)
            """
            # TSE
            for stockId, basicData in tseBasicData.items():
                tmpList = []
                tmpDict = {}
                # append TSE basic data to tmpList
                for basic in basicData:
                    tmpList.append(basic)
                # if tsePeratioData has data
                if tsePeratioData.get(stockId) is not None:
                    for peratioData in tsePeratioData[stockId]:
                        tmpList.append(peratioData)
                else:
                    for i in range(peratioNum):
                        tmpList.append('')   
                # if tseLegelData has data
                if tseLegelData.get(stockId) is not None:
                    for legelData in tseLegelData[stockId]:
                        tmpList.append(legelData)
                else:
                    for i in range(legalNum):
                        tmpList.append('')    
                        
                # if tseMarginData has data
                if tseMarginData.get(stockId) is not None:
                    for marginData in tseMarginData[stockId]:
                        tmpList.append(marginData)
                else:
                    for i in range(marginNum):
                        tmpList.append('')
                
                tmpDict[keyDate] = tmpList 
                
                result[stockId].update(tmpDict)
                """
            errorTimes = 0
        except:
            print('No data on ' + showDate)
            errorTimes += 1
            continue
        finally:
            nowDate += timedelta(1)
            
    endTime = crawler.time()
    
    elapsedTime = crawler.elapsedTime(startTime, endTime)
    #print(result)
    #print(len(result))
    #print(len(stockBasicData))
    #print(len(otcBasicData))
    for stockId, stockData in result.items():
        #print(stockId)
        for stockDate, stockDetail in stockData.items():
            #print(stockDate, stockDetail)
            crawler.writeCSV(stockId, stockDetail)
    print('Finished ! Time elapsed {}'.format(elapsedTime))

    
if __name__ == '__main__':
    main()
        