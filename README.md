# Stock_Crawler
Stock crawler for Taiwan Stock (csv version)

2018年完成，2019年仍有在使用

--各標的csv資料--

![image](https://github.com/allen8299/Stock_Crawler/blob/master/list.JPG)

--以台積電舉例內容--

![image](https://github.com/allen8299/Stock_Crawler/blob/master/2330.JPG)

--主程式 main--

nowDate = datetime(2020, 1, 1) // 起日

lastDate = datetime(2020, 10, 15) // 訖日

--函式介紹--

撈取上市 基本資料 getTseBasicData

撈取上市 三大法人買賣超日報 getTseLegalByAndSell

撈取上市 融資券餘額 getTseMargin

撈取上市 本益比, 殖利率, 股價淨值比 getTsePeratioData

撈取上櫃 基本資料 getOtcBasicData

撈取上櫃 三大法人買賣超日報 getOtcLegalByAndSell

撈取上櫃 融資券餘額 getOtcMargin

撈取上櫃 本益比, 殖利率, 股價淨值比 getOtcPeratioData
