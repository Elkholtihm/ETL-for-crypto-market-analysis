# Processes ETL for Cryptocurencies market Analysis

## Overview
this project ...

---

## 📂 Repository Structure

```
├── hive.py                 # script to create the hiev datawarehouse schema
├── process_data.py         # script to create the snowflake datawarehouse schema
├── sql.py                  # script to create the mysql datawarehouse schema
├── Dashboard               # contains power bi dashboard and script file of streamlit dashboard
└── docker-hive-master      # contains files to conatenrize hive in docker 

```

### CODE/
```  
├── ETL/  
│   ├── 
│   ├── etl.py                  # script that use functions and connect to database used and manage the workflow
│   ├── load.py                 # functions to load to data warehouse
│   └── extract_transform.py    # conatins functions to extract from MYSQL and transform to data warehouse format
│  
└── streaming/  
    ├── coins_data              # contains python scripts to get blockchain data, dominanace, exchange rateand price of cryptocurencies
    ├── economic_data           # get data gold price, interest rate and stock price 
    ├── tweets_news             # contains script python to get news about crypto from websites NEWS api and X
    ├── data_ingestor.py        # script that use functions to extract from multiple APIs
```

---

## Used Technologies
The project leverages the following technologies:

- ![hive](https://img.shields.io/badge..)
- ![snowflake](https://img.shields.io/badge..)
- ![MYSQL](https://img.shields.io/badge..)
- ![pandas](https://img.shields.io/badge..)
- ![streamlit](https://img.shields.io/badge..)
- ![power bi](https://img.shields.io/badge..)
- ![python](https://img.shields.io/badge..)


---


## Collaboration
This project was created in collaboration with **[Anouar Bouzhar](https://github.com/anouarbouzhar)**.

---

## 🚀 Using the Project
to use the project get the API from website as shown in the architecture images and create a .env file where you need to provide the connection informations and APIs
as follow : 

```  
user_snowflake=""
password_snowflake=""
snowflake_account=""

hive_host=''
hive_port=
hive_database=''
hive_username = ''

SQLhost='localhost'
SQLuser='root'
SQLpassword=''
SQLdatabase=''

blockchainAPI = '' # CoinmarketCap 
exchange_rateAPI = '' # 
crypto_pricesAPI = '' # 
newsAPI = '' # news
XTOKEN = '' # X

```
### websites link : 
- ![NEWS](https://newsapi.org/)
- ![X](https://developer.x.com/en)
- ![MYSQL](https://pro.coinmarketcap.com/api/v1/#)
- ![CoinMarketCap](https://img.shields.io/badge..)
- ![binance](https://www.binance.com/fr/binance-api)
- ![coingecko](https://www.coingecko.com/en/api)
- ![cryptocompare](https://min-api.cryptocompare.com/)

1. Clone the repository:
```bash
   git clone [repo]
```

2. Install the required packages:
```bash
  pip install ultralytics
  pip install opencv-python requests matplotlib
```

3. Navigate to the code directory:
```bash
  cd code
```

4. Navigate to the code directory:
```bash
  python FraudSender.py
```