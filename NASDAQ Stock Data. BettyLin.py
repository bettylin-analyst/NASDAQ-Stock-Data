import requests
import warnings
import datetime
import pandas as pd
from bs4 import BeautifulSoup

#stores NASDAQ100 URL in a variable and filters the warnings from entering the website (if any)
NASDAQ100URL = 'https://www.advfn.com/nasdaq/nasdaq.asp'
warnings.filterwarnings('ignore')

#scrapes URL for table and stores data in a list
data = []
def getTableFromURL(URL, data):
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', attrs={'class':'market tab1'})
    rows = table.find_all('tr')
    #loop through all table rows and pull out text
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        #filter out incomplete rows and title rows
        if len(cols)>1:
            data.append([ele for ele in cols])

#populate list with web scraped data
getTableFromURL(NASDAQ100URL, data)

#cuts out columns that do not have pandas compatible data (ie. charts)
for rowNum in range(len(data)):
    data[rowNum] = data[rowNum][:2] + data[rowNum][3:5]

#stores data in a pandas dataframe and names columns
df1 = pd.DataFrame(data, columns = ['Company Name','Symbol','Price','Change'])

#indicates stock ticker symbols that will be summarized
Symbols = df1['Symbol'].tolist()

#creates a dictionary of lists corresponding to each column
dataf = {"Date": [],
         "Symbol": [],
         "Open Price": [],
         "High": [],
         "Low": [],
         "Close Price": [],
         "Volume": []}

#stores URL and headers (such as the key and host) in variables to be used during API request calls
url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-chart"
headers = {
    "x-rapidapi-key": "aa8c0040aamshdf1dbef3566f5e6p15c7bfjsn593cb42d9f5a",
    "x-rapidapi-host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

#initializes dataframe using the dictionary above
df2 = pd.DataFrame(dataf)

#loops through the stock ticker symbols indicated earlier
for symbol in Symbols:
    querystring = {"interval":"1d","symbol":symbol,"range":"1d"}
    #uses request library to retrieve data from API
    response = requests.request("GET", url, headers=headers, params=querystring)
    #converts data from API to json dictionary format
    data = response.json()

    #loops through data for each stock symbol and stores data in dataframe created earlier
    for i in range(len(data["chart"]["result"][0]["timestamp"])):
        new_row = pd.Series(data={
        "Date": datetime.datetime.fromtimestamp(data["chart"]["result"][0]["timestamp"][i]),
        "Symbol": symbol,
        "Open Price": data["chart"]["result"][0]["indicators"]["quote"][0]["open"][i],
        "High": data["chart"]["result"][0]["indicators"]["quote"][0]["high"][i],
        "Low": data["chart"]["result"][0]["indicators"]["quote"][0]["low"][i],
        "Close Price": data["chart"]["result"][0]["indicators"]["quote"][0]["close"][i],
        "Volume": data["chart"]["result"][0]["indicators"]["quote"][0]["volume"][i]},
        name='x')
        #adds new row of data for dataframe
        df2 = df2.append(new_row, ignore_index=False)

#merges data from df1 and df2 based on the column titled "Symbol"
df3 = pd.merge(df1, df2, left_on = 'Symbol', right_on = 'Symbol', how = 'inner')

#print merged dataframe
print(df3)

#extract numeric columns and ensures numeric data type
outputDF = df3[["Price", "Change","Open Price", "High", "Low", "Close Price", "Volume"]].apply(pd.to_numeric)
#creates and prints statistics for dataframe
outputDescription = outputDF.describe(include='all').loc[['count','mean','std','min','25%','50%','75%','max']]
print(outputDescription)
print("\nThe dataframe has been exported.")

#creates CSV from dataframe
df3.to_csv("Stock Data Merged Summary.csv", index = False)
