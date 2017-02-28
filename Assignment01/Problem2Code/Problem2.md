

```python

# coding: utf-8

import sys
import datetime as dt
import pandas as pd
import urllib.request
import zipfile, io
import csv
    
#Default Values and Constants
outputFileName = 'finalOutput.csv'
summaryFileName = 'summary.csv'
#Validate input argument and set year to 2003 if validation fails
try:
    if len(sys.argv) >= 2:    
        #if isinstance(sys.argv[1],int):
        year=int(sys.argv[1])
        print(year)
        if not (2003 <= year <= 2016):
            year=2003
    else:
        year=2003        
except Exception as err:
    year=2003
print(year)

def generateURLList(year):
    urlList = []
    for m in range(1, 13):
        quarter = pd.Timestamp(dt.date(year, m, 1)).quarter
        #print(quarter)
        link='http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/'+str(year)+'/Qtr'+str(quarter)+'/log'+str(year)+format(m,'02d')+'01.zip'
        urlList.append(link)
    return urlList

def handleMissingData(df):
    #Remove row if ip is null
    df=df[~ df['ip'].isnull()]
    #Forward Fill or Bottom Fill date if null
    df.date.fillna(method='ffill',inplace=True)
    df.date.fillna(method='bfill',inplace=True)
    #Forward Fill or Bottom Fill time if null
    df.time.fillna(method='bfill',inplace=True)
    df.time.fillna(method='ffill',inplace=True)
    #Forward Fill zone or Bottom Fill zone if null
    df.zone.fillna(method='ffill',inplace=True)
    df.zone.fillna(method='bfill',inplace=True)
    #Concat Accession Number if extention doen't contain file name
    toConcat = df.extention.str.startswith('.',na=False)
    addedExtention = df.accession[toConcat] + df.extention[toConcat]
    normalExtention = df.extention[~toConcat]
    df.extention = addedExtention.append(normalExtention)
    #Set Code to 404 if null
    df.code.fillna(404.0, inplace=True)
    #Set File size to zero if null
    df['size'].fillna(0.0, inplace=True)
    #Set File size to zero if null
    df.idx.fillna(0.0, inplace=True)
    #Set refer to zero if null
    df.norefer.fillna(1.0, inplace=True)
    #Set agent to zero if null
    df.noagent.fillna(1.0, inplace=True)
    #Set find to zero if null
    df.find.fillna(0.0, inplace=True)
    #Set crawler to zero if null
    df.crawler.fillna(0.0, inplace=True)
    #Set browser to unknown if null
    df.browser.fillna('unknown', inplace=True)
    return df

urlList = generateURLList(int(year))
for fileURL in urlList:
    print(fileURL)


appendWrite = True
for fileURL in urlList:
    try:
        response = urllib.request.urlopen(fileURL)
        if response.getcode()==200:
            data = response.read()
            if zipfile.is_zipfile(io.BytesIO(data)) == True:
                z = zipfile.ZipFile(io.BytesIO(data))
                for file in z.namelist():
                    if file.endswith('.csv'):
                        if appendWrite:
                            csvFile = z.read(file)
                            df = pd.read_csv(io.BytesIO(csvFile),header=0)
                            df = handleMissingData(df)
                            df.to_csv(outputFileName,header ='column_names')
                            appendWrite = False
                        if not appendWrite:
                            csvFile = z.read(file)
                            df_new = pd.read_csv(io.BytesIO(csvFile),header=0)
                            df_new = handleMissingData(df_new)
                            # df.to_csv(outputFileName,header=False)
                            df_new.to_csv(outputFileName,mode = 'a',header=False)
                            #df.append(df_new, ignore_index=True)
                            df = pd.concat([df,df_new])
                            print(df.shape)
            else:
                print("[ERROR] Invalid ZIP File found at " + fileURL)
        else:
            print("[ERROR] Invalid URL, URL( " + fileURL + " ) returned a bad HTTP reponse code of " + str(response.getcode()))
        response.close()
    except Exception as err:
        print("Error occured, possibly an interrupted Internet connection")


# In[169]:

#Added a coulumn count to perform group by
df['count'] = 1


# In[230]:


#browsers and count in year
df['browserRequestCount'] = 1
browser_count = df.groupby('browser').aggregate({'browserRequestCount':'sum'}).reset_index()
browser_count.to_csv(outputFileName, mode='a',index=False)
browser_count.to_csv(summaryFileName, mode='w',index=False)




# In[231]:

#Number of request in a month
df['perMonthRequestCount'] = 1
requestsByMonth = df.groupby('date').aggregate({'perMonthRequestCount':'sum'}).reset_index()
requestsByMonth.to_csv(outputFileName, mode='a',index=False)
requestsByMonth.to_csv(summaryFileName, mode='a',index=False)


# In[232]:

#For each date, total reuqests per response code
dateCodeCount = df.groupby(['date','code']).aggregate({'count':'sum'}).reset_index()
dateCodeCount.to_csv(summaryFileName, mode='a',index=False)
dateCodeCount.to_csv(outputFileName, mode='a',index=False)


# In[233]:

#For each date, calculate the min, max and mean file sizes requested
dateCodeSize = df.groupby(['date','code']).aggregate({'size':['mean','min','max']}).reset_index()
dateCodeSize
dateCodeSize.to_csv(summaryFileName, mode='a',index=False)
dateCodeSize.to_csv(outputFileName, mode='a',index=False)


# In[234]:

#Month, distinct IP's Count
temp_df = df.groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
temp_df['distinctIPcount'] = 1
monthDistinctIPCount = temp_df.groupby(['date']).aggregate({'distinctIPcount':'sum'}).reset_index()
monthDistinctIPCount.to_csv(summaryFileName, mode='a',index=False)
monthDistinctIPCount.to_csv(outputFileName, mode='a',index=False)

#Number of crawlers that tried to scrap data every month
df_crawler = df[df.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler['crawler_count'] = 1
df_crawler = df_crawler.groupby(['date']).aggregate({'crawler_count':'sum'}).reset_index()
#Number of crawlers without 404 that tried to scrap data every month
df_crawler_without404 = df[df.code != 404.0]
df_crawler_without404 = df_crawler_without404[df_crawler_without404.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler_without404['crawler_count_without404'] = 1
df_crawler_without404 = df_crawler_without404.groupby(['date']).aggregate({'crawler_count_without404':'sum'}).reset_index()

df_crawler['crawler_count_without404'] = df_crawler_without404.crawler_count_without404
df_crawler.to_csv(summaryFileName, mode='a',index=False)
df_crawler.to_csv(outputFileName, mode='a',index=False)

#This Excel file is being used to be imported in Tableau
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('summary.xlsx', engine='xlsxwriter')
browser_count.to_excel(writer, sheet_name='BowserCount',index=False)
requestsByMonth.to_excel(writer, sheet_name='RequestsPerMonth',index=False)
dateCodeCount.to_excel(writer, sheet_name='ResponseCodePerMonth',index=False)
dateCodeSize.to_excel(writer, sheet_name='FileSizeRequestedPerMonth')
monthDistinctIPCount.to_excel(writer, sheet_name='DistinctIPPerMonth',index=False)
df_crawler.to_excel(writer, sheet_name='CrawlerCountByMonth',index=False)
writer.save()
```

    2003
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr1/log20030101.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr1/log20030201.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr1/log20030301.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr2/log20030401.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr2/log20030501.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr2/log20030601.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr3/log20030701.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr3/log20030801.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr3/log20030901.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr4/log20031001.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr4/log20031101.zip
    http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr4/log20031201.zip
    (0, 15)
    (0, 15)
    (43458, 15)
    (304747, 15)
    (596337, 15)
    (711009, 15)
    (1084258, 15)
    (1323023, 15)
    (1386883, 15)
    (1506723, 15)
    (1517053, 15)
    (1683399, 15)



```python
#Number of rows after merging all the 12 CSV files
df.shape
```




    (1683399, 18)




```python
#browsers and count in year
df['browserRequestCount'] = 1
browser_count = df.groupby('browser').aggregate({'browserRequestCount':'sum'}).reset_index()
browser_count
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>browser</th>
      <th>browserRequestCount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>iem</td>
      <td>17</td>
    </tr>
    <tr>
      <th>1</th>
      <td>lin</td>
      <td>1344</td>
    </tr>
    <tr>
      <th>2</th>
      <td>mac</td>
      <td>1847</td>
    </tr>
    <tr>
      <th>3</th>
      <td>mie</td>
      <td>202984</td>
    </tr>
    <tr>
      <th>4</th>
      <td>opr</td>
      <td>660</td>
    </tr>
    <tr>
      <th>5</th>
      <td>saf</td>
      <td>10</td>
    </tr>
    <tr>
      <th>6</th>
      <td>unknown</td>
      <td>564290</td>
    </tr>
    <tr>
      <th>7</th>
      <td>win</td>
      <td>912247</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Number of request in a month
df['perMonthRequestCount'] = 1
requestsByMonth = df.groupby('date').aggregate({'perMonthRequestCount':'sum'}).reset_index()
requestsByMonth
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>perMonthRequestCount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>43458</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-04-01</td>
      <td>261289</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-05-01</td>
      <td>291590</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-06-01</td>
      <td>114672</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-07-01</td>
      <td>373249</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-08-01</td>
      <td>238765</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-09-01</td>
      <td>63860</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-10-01</td>
      <td>119840</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-11-01</td>
      <td>10330</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-12-01</td>
      <td>166346</td>
    </tr>
  </tbody>
</table>
</div>




```python
#For each date, total reuqests per response code
dateCodeCount = df.groupby(['date','code']).aggregate({'count':'sum'}).reset_index()
dateCodeCount
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>code</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>200.0</td>
      <td>37706</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-03-01</td>
      <td>206.0</td>
      <td>398</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-03-01</td>
      <td>302.0</td>
      <td>8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-03-01</td>
      <td>304.0</td>
      <td>5325</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-03-01</td>
      <td>404.0</td>
      <td>21</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-04-01</td>
      <td>200.0</td>
      <td>221283</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-04-01</td>
      <td>206.0</td>
      <td>1881</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-04-01</td>
      <td>302.0</td>
      <td>5016</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-04-01</td>
      <td>304.0</td>
      <td>32523</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-04-01</td>
      <td>403.0</td>
      <td>5</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2003-04-01</td>
      <td>404.0</td>
      <td>277</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2003-04-01</td>
      <td>416.0</td>
      <td>304</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2003-05-01</td>
      <td>200.0</td>
      <td>252470</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2003-05-01</td>
      <td>206.0</td>
      <td>1107</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2003-05-01</td>
      <td>302.0</td>
      <td>112</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2003-05-01</td>
      <td>304.0</td>
      <td>37094</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2003-05-01</td>
      <td>400.0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2003-05-01</td>
      <td>404.0</td>
      <td>462</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2003-05-01</td>
      <td>416.0</td>
      <td>343</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2003-06-01</td>
      <td>200.0</td>
      <td>108357</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2003-06-01</td>
      <td>206.0</td>
      <td>343</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2003-06-01</td>
      <td>304.0</td>
      <td>5961</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2003-06-01</td>
      <td>404.0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2003-07-01</td>
      <td>200.0</td>
      <td>336447</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2003-07-01</td>
      <td>206.0</td>
      <td>1460</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2003-07-01</td>
      <td>302.0</td>
      <td>464</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2003-07-01</td>
      <td>304.0</td>
      <td>34266</td>
    </tr>
    <tr>
      <th>27</th>
      <td>2003-07-01</td>
      <td>400.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2003-07-01</td>
      <td>404.0</td>
      <td>120</td>
    </tr>
    <tr>
      <th>29</th>
      <td>2003-07-01</td>
      <td>416.0</td>
      <td>491</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>33</th>
      <td>2003-08-01</td>
      <td>304.0</td>
      <td>23770</td>
    </tr>
    <tr>
      <th>34</th>
      <td>2003-08-01</td>
      <td>400.0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>35</th>
      <td>2003-08-01</td>
      <td>404.0</td>
      <td>508</td>
    </tr>
    <tr>
      <th>36</th>
      <td>2003-08-01</td>
      <td>416.0</td>
      <td>862</td>
    </tr>
    <tr>
      <th>37</th>
      <td>2003-09-01</td>
      <td>200.0</td>
      <td>52309</td>
    </tr>
    <tr>
      <th>38</th>
      <td>2003-09-01</td>
      <td>206.0</td>
      <td>767</td>
    </tr>
    <tr>
      <th>39</th>
      <td>2003-09-01</td>
      <td>302.0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>40</th>
      <td>2003-09-01</td>
      <td>304.0</td>
      <td>10311</td>
    </tr>
    <tr>
      <th>41</th>
      <td>2003-09-01</td>
      <td>400.0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>42</th>
      <td>2003-09-01</td>
      <td>404.0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>43</th>
      <td>2003-09-01</td>
      <td>416.0</td>
      <td>456</td>
    </tr>
    <tr>
      <th>44</th>
      <td>2003-10-01</td>
      <td>200.0</td>
      <td>101490</td>
    </tr>
    <tr>
      <th>45</th>
      <td>2003-10-01</td>
      <td>206.0</td>
      <td>671</td>
    </tr>
    <tr>
      <th>46</th>
      <td>2003-10-01</td>
      <td>302.0</td>
      <td>95</td>
    </tr>
    <tr>
      <th>47</th>
      <td>2003-10-01</td>
      <td>304.0</td>
      <td>17350</td>
    </tr>
    <tr>
      <th>48</th>
      <td>2003-10-01</td>
      <td>400.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>49</th>
      <td>2003-10-01</td>
      <td>404.0</td>
      <td>72</td>
    </tr>
    <tr>
      <th>50</th>
      <td>2003-10-01</td>
      <td>416.0</td>
      <td>161</td>
    </tr>
    <tr>
      <th>51</th>
      <td>2003-11-01</td>
      <td>200.0</td>
      <td>9136</td>
    </tr>
    <tr>
      <th>52</th>
      <td>2003-11-01</td>
      <td>206.0</td>
      <td>56</td>
    </tr>
    <tr>
      <th>53</th>
      <td>2003-11-01</td>
      <td>302.0</td>
      <td>6</td>
    </tr>
    <tr>
      <th>54</th>
      <td>2003-11-01</td>
      <td>304.0</td>
      <td>1105</td>
    </tr>
    <tr>
      <th>55</th>
      <td>2003-11-01</td>
      <td>400.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>56</th>
      <td>2003-11-01</td>
      <td>404.0</td>
      <td>26</td>
    </tr>
    <tr>
      <th>57</th>
      <td>2003-12-01</td>
      <td>200.0</td>
      <td>138075</td>
    </tr>
    <tr>
      <th>58</th>
      <td>2003-12-01</td>
      <td>206.0</td>
      <td>1344</td>
    </tr>
    <tr>
      <th>59</th>
      <td>2003-12-01</td>
      <td>302.0</td>
      <td>82</td>
    </tr>
    <tr>
      <th>60</th>
      <td>2003-12-01</td>
      <td>304.0</td>
      <td>26533</td>
    </tr>
    <tr>
      <th>61</th>
      <td>2003-12-01</td>
      <td>400.0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>62</th>
      <td>2003-12-01</td>
      <td>404.0</td>
      <td>308</td>
    </tr>
  </tbody>
</table>
<p>63 rows × 3 columns</p>
</div>




```python
#For each date, calculate the min, max and mean file sizes requested
dateCodeSize = df.groupby(['date','code']).aggregate({'size':['mean','min','max']}).reset_index()
dateCodeSize
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>date</th>
      <th>code</th>
      <th colspan="3" halign="left">size</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th></th>
      <th>mean</th>
      <th>min</th>
      <th>max</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>200.0</td>
      <td>145957.434493</td>
      <td>113.0</td>
      <td>14396718.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-03-01</td>
      <td>206.0</td>
      <td>133525.394472</td>
      <td>100.0</td>
      <td>2195453.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-03-01</td>
      <td>302.0</td>
      <td>279.375000</td>
      <td>275.0</td>
      <td>287.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-03-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-03-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-04-01</td>
      <td>200.0</td>
      <td>174301.892626</td>
      <td>0.0</td>
      <td>52595445.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-04-01</td>
      <td>206.0</td>
      <td>211130.877193</td>
      <td>6.0</td>
      <td>5975500.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-04-01</td>
      <td>302.0</td>
      <td>286.195774</td>
      <td>273.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-04-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-04-01</td>
      <td>403.0</td>
      <td>3632.000000</td>
      <td>3632.0</td>
      <td>3632.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2003-04-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2003-04-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2003-05-01</td>
      <td>200.0</td>
      <td>127960.519555</td>
      <td>0.0</td>
      <td>38232485.0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2003-05-01</td>
      <td>206.0</td>
      <td>297269.870822</td>
      <td>4.0</td>
      <td>37578419.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2003-05-01</td>
      <td>302.0</td>
      <td>286.250000</td>
      <td>275.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2003-05-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2003-05-01</td>
      <td>400.0</td>
      <td>287.000000</td>
      <td>287.0</td>
      <td>287.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2003-05-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2003-05-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2003-06-01</td>
      <td>200.0</td>
      <td>132190.129258</td>
      <td>0.0</td>
      <td>20267015.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2003-06-01</td>
      <td>206.0</td>
      <td>153036.790087</td>
      <td>19.0</td>
      <td>3470356.0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2003-06-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2003-06-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2003-07-01</td>
      <td>200.0</td>
      <td>86689.252821</td>
      <td>0.0</td>
      <td>55908929.0</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2003-07-01</td>
      <td>206.0</td>
      <td>277605.381507</td>
      <td>0.0</td>
      <td>18917380.0</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2003-07-01</td>
      <td>302.0</td>
      <td>287.217672</td>
      <td>275.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2003-07-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>27</th>
      <td>2003-07-01</td>
      <td>400.0</td>
      <td>372.000000</td>
      <td>372.0</td>
      <td>372.0</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2003-07-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>29</th>
      <td>2003-07-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>33</th>
      <td>2003-08-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>34</th>
      <td>2003-08-01</td>
      <td>400.0</td>
      <td>299.000000</td>
      <td>299.0</td>
      <td>299.0</td>
    </tr>
    <tr>
      <th>35</th>
      <td>2003-08-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>36</th>
      <td>2003-08-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>37</th>
      <td>2003-09-01</td>
      <td>200.0</td>
      <td>141140.593818</td>
      <td>0.0</td>
      <td>14396718.0</td>
    </tr>
    <tr>
      <th>38</th>
      <td>2003-09-01</td>
      <td>206.0</td>
      <td>185857.564537</td>
      <td>1.0</td>
      <td>9740716.0</td>
    </tr>
    <tr>
      <th>39</th>
      <td>2003-09-01</td>
      <td>302.0</td>
      <td>287.666667</td>
      <td>287.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>40</th>
      <td>2003-09-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>41</th>
      <td>2003-09-01</td>
      <td>400.0</td>
      <td>299.000000</td>
      <td>299.0</td>
      <td>299.0</td>
    </tr>
    <tr>
      <th>42</th>
      <td>2003-09-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>43</th>
      <td>2003-09-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>2003-10-01</td>
      <td>200.0</td>
      <td>140345.940369</td>
      <td>0.0</td>
      <td>64288350.0</td>
    </tr>
    <tr>
      <th>45</th>
      <td>2003-10-01</td>
      <td>206.0</td>
      <td>262421.980626</td>
      <td>1.0</td>
      <td>12096669.0</td>
    </tr>
    <tr>
      <th>46</th>
      <td>2003-10-01</td>
      <td>302.0</td>
      <td>285.410526</td>
      <td>273.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>47</th>
      <td>2003-10-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>48</th>
      <td>2003-10-01</td>
      <td>400.0</td>
      <td>374.000000</td>
      <td>374.0</td>
      <td>374.0</td>
    </tr>
    <tr>
      <th>49</th>
      <td>2003-10-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>50</th>
      <td>2003-10-01</td>
      <td>416.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>51</th>
      <td>2003-11-01</td>
      <td>200.0</td>
      <td>138444.307903</td>
      <td>290.0</td>
      <td>8771996.0</td>
    </tr>
    <tr>
      <th>52</th>
      <td>2003-11-01</td>
      <td>206.0</td>
      <td>449667.053571</td>
      <td>77.0</td>
      <td>4995709.0</td>
    </tr>
    <tr>
      <th>53</th>
      <td>2003-11-01</td>
      <td>302.0</td>
      <td>286.833333</td>
      <td>286.0</td>
      <td>287.0</td>
    </tr>
    <tr>
      <th>54</th>
      <td>2003-11-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>55</th>
      <td>2003-11-01</td>
      <td>400.0</td>
      <td>287.000000</td>
      <td>287.0</td>
      <td>287.0</td>
    </tr>
    <tr>
      <th>56</th>
      <td>2003-11-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
    <tr>
      <th>57</th>
      <td>2003-12-01</td>
      <td>200.0</td>
      <td>134165.042578</td>
      <td>0.0</td>
      <td>28411979.0</td>
    </tr>
    <tr>
      <th>58</th>
      <td>2003-12-01</td>
      <td>206.0</td>
      <td>226358.571429</td>
      <td>1.0</td>
      <td>7464194.0</td>
    </tr>
    <tr>
      <th>59</th>
      <td>2003-12-01</td>
      <td>302.0</td>
      <td>285.426829</td>
      <td>274.0</td>
      <td>288.0</td>
    </tr>
    <tr>
      <th>60</th>
      <td>2003-12-01</td>
      <td>304.0</td>
      <td>0.000000</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>61</th>
      <td>2003-12-01</td>
      <td>400.0</td>
      <td>287.000000</td>
      <td>287.0</td>
      <td>287.0</td>
    </tr>
    <tr>
      <th>62</th>
      <td>2003-12-01</td>
      <td>404.0</td>
      <td>3451.000000</td>
      <td>3451.0</td>
      <td>3451.0</td>
    </tr>
  </tbody>
</table>
<p>63 rows × 5 columns</p>
</div>




```python
#Month, distinct IP's Count
temp_df = df.groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
temp_df['distinctIPcount'] = 1
monthDistinctIPCount = temp_df.groupby(['date']).aggregate({'distinctIPcount':'sum'}).reset_index()
monthDistinctIPCount
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>distinctIPcount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>5059</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-04-01</td>
      <td>17306</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-05-01</td>
      <td>15948</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-06-01</td>
      <td>4866</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-07-01</td>
      <td>15238</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-08-01</td>
      <td>13385</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-09-01</td>
      <td>6120</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-10-01</td>
      <td>11846</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-11-01</td>
      <td>1877</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-12-01</td>
      <td>14925</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Number of crawlers that tried to scrap data every month
df_crawler = df[df.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler['crawler_count'] = 1
df_crawler = df_crawler.groupby(['date']).aggregate({'crawler_count':'sum'}).reset_index()
df_crawler

```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>crawler_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>19</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-04-01</td>
      <td>66</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-05-01</td>
      <td>72</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-06-01</td>
      <td>17</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-07-01</td>
      <td>80</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-08-01</td>
      <td>57</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-09-01</td>
      <td>22</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-10-01</td>
      <td>42</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-11-01</td>
      <td>12</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-12-01</td>
      <td>54</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Number of crawlers without 404 that tried to scrap data every month
df_crawler_without404 = df[df.code != 404.0]
df_crawler_without404 = df_crawler_without404[df_crawler_without404.crawler == 1.0].groupby(['date','ip']).aggregate({'count':'sum'}).reset_index()
df_crawler_without404['crawler_count_without404'] = 1
df_crawler_without404 = df_crawler_without404.groupby(['date']).aggregate({'crawler_count_without404':'sum'}).reset_index()
df_crawler['crawler_count_without404'] = df_crawler_without404.crawler_count_without404
df_crawler
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>crawler_count</th>
      <th>crawler_count_without404</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2003-03-01</td>
      <td>19</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2003-04-01</td>
      <td>66</td>
      <td>18</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2003-05-01</td>
      <td>72</td>
      <td>25</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003-06-01</td>
      <td>17</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2003-07-01</td>
      <td>80</td>
      <td>17</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2003-08-01</td>
      <td>57</td>
      <td>20</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2003-09-01</td>
      <td>22</td>
      <td>14</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2003-10-01</td>
      <td>42</td>
      <td>16</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2003-11-01</td>
      <td>12</td>
      <td>11</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2003-12-01</td>
      <td>54</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Number of crawlers that tried to scrap data for 2003
df_crawler_2003 = df[df.crawler == 1.0].groupby(['ip']).aggregate({'count':'sum'}).reset_index()
df_crawler_2003['crawler_count'] = 1
df_crawler_2003.shape

```




    (363, 3)




```python
#Number of crawlers without 404 that tried to scrap data every month
df_crawler_without404_2003 = df[df.code != 404.0]
df_crawler_without404_2003 = df_crawler_without404_2003.reset_index()
df_crawler_without404_2003 = df_crawler_without404_2003[df_crawler_without404_2003.crawler == 1.0].groupby(['ip']).aggregate({'count':'sum'}).reset_index()
df_crawler_without404_2003.shape

```




    (103, 2)


