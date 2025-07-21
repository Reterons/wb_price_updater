import os
import requests
import pandas as pd
from io import StringIO
import re
import csv
from dotenv import load_dotenv
from time import sleep
import logging
import json
import datetime
import numpy as np
from ftplib import FTP
import xml.etree.ElementTree as ET
from collections import defaultdict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import pytz

def update_wb_prices(API_KEY, gm):
    url = 'https://discounts-prices-api.wildberries.ru/api/v2/upload/task'
    for i in gm:
        payload = {
            "data": [
                {
                    "nmID": int(i['wb_articles']),
                    "price": int(round(i['final_price_without_discount'])),
                    "discount": int(round(i['wb_discount']))
                }
            ]
        }

        headers = {
            'Authorization': API_KEY,
            'Content-Type': 'application/json'
        }

        response = requests.post(url, headers=headers, json=payload)
        sleep(0.61)

def get_data_gamma():
    def get_chrt_ids_by_imt_id(api_key, imt_id, all_cards):
        chrt_id = 0
        subjectName = 0
        value = 0
        wb_art = 0
        for card in all_cards:
            if str(card["vendorCode"]) == str(imt_id):
                chrt_id = card["sizes"][0]["skus"][0]
                subjectName = card["subjectName"]
                value = card["dimensions"]['width'] * card["dimensions"]['height'] *card ["dimensions"]['length'] * 0.001
                wb_art = card["nmID"]
        return chrt_id, subjectName, value, wb_art
    WB_WAREHOUSE_ID = "1030859"
    WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODAxNjk0MCwiaWQiOiIwMTk3ZmEyYy1mOGEzLTdiNzgtYTJmOS1mNjM1ZTE5ZjRkYmMiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5NzQ3MTQsInMiOjE2MTI2LCJzaWQiOiIyMDBjMWJmMC0xM2UxLTQ4MmEtYTM1MS01NjlhNzgxN2NiMmQiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.FUZNpgyRFT7HTar-l860MUJGr2WBVRvcoVmU9n72-t22NmSeeKqeRa5Mmn7I7zB0WU-P-IWpDnDjpp0mNpo3yQ"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    STOCKS_URL = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{WB_WAREHOUSE_ID}"
    SUPPLIER_URL="https://shop.firma-gamma.ru/api/v1.0/stock.php?type=csv"
    response = requests.get(
            SUPPLIER_URL,
            auth=requests.auth.HTTPBasicAuth("natalia-b2005", "Holst110878"),
        )
    response.raise_for_status()
    content = response.content.decode("windows-1251", errors='replace')
    column_names = [
        'id',
        'conversion_factor',
        'wholesale_price_retail_pack',
        'min_recommended_retail_price',
        'name',
        'availability_status',
        'wholesale_price_wholesale_pack'
    ]
    df = pd.read_csv(
        StringIO(content),  
        sep='\t',            
        header=None,                
        names=column_names,           
        quoting=csv.QUOTE_MINIMAL,     
        doublequote=True,              
        escapechar=None,               
    )
    def get_all_cards(api_key):
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }
        
        all_cards = []
        limit = 100 
        cursor = None  
        
        while True:
            sleep(5)
            
            payload = {
                "settings": {
                    "cursor": {
                        "limit": limit
                    },
                    "filter": {
                        "withPhoto": -1 
                    }
                }
            }
            
            if cursor:
                payload["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
                payload["settings"]["cursor"]["nmID"] = cursor["nmID"]
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code != 200:
                print(f"Ошибка {response.status_code}: {response.text}")
                break
            
            data = response.json()
            cards = data.get("cards", [])
            total = data.get("total", 0)  
            
            if not cards:
                break  
            
            all_cards.extend(cards)
            print(f"Загружено: {len(all_cards)} / {total}")
            
          
            if len(cards) < limit:
                break
            
   
            last_card = cards[-1]
            cursor = {
                "updatedAt": last_card["updatedAt"],
                "nmID": last_card["nmID"]
            }
        
        return all_cards
    
    all_cards = get_all_cards(WB_API_KEY)
    products = df.groupby('id').agg({
        'name': 'first',
        'wholesale_price_wholesale_pack': 'sum',
        'conversion_factor': 'sum'
        }).reset_index()
    
    stocks_data = []
    barcodes = []
    titles = []
    volumes = []
    wb_arts = []
    for i in products["id"]:
        chrt, title, volume, wb_art = get_chrt_ids_by_imt_id(WB_API_KEY, i, all_cards)
        if chrt:
            barcodes.append(chrt)
            titles.append(title)
            volumes.append(volume)
            wb_arts.append(str(wb_art))
        else:
            barcodes.append(np.nan)
            titles.append(np.nan)
            volumes.append(np.nan)
            wb_arts.append(np.nan)
    products = products.assign(barcode=barcodes, title=titles, volume=volumes, wb_articles=wb_arts)
    products = products.dropna()
    products = products.reset_index()
    products = products.drop("index", axis=1)
    url = "https://docs.google.com/spreadsheets/d/1dnnhSbIb7kSMu4jDKsdqwsOpIEWVqFK8x1omO6OJhOk/export?format=csv"
    df2 = pd.read_csv(url)
    mrrc_foreign = []
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df2["Внутренний id"]):    
            if n == i:
                mrrc_foreign.append(df2["РРЦ, руб"][j])
                flag = 1
        if flag == 0:
            mrrc_foreign.append(0)
    mrrc = []
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df["id"]):
            if mrrc_foreign[k] == 0:
                if i == n:
                    mrrc.append(df["min_recommended_retail_price"][j]*df["conversion_factor"][j])
            else:
                break
        if mrrc_foreign[k] != 0:
            mrrc.append(0)
    mrrc_discount_foreign_brands = []
    url = "https://docs.google.com/spreadsheets/d/1aTu5tToBy6MYct6oFXr3u3CGIA8NGJFlJHcD5TshXxE/export?format=csv"
    df3 = pd.read_csv(url)
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df3["Артикул продавца"]):
            if i == n and type(df3["спец скидка"][j]) == str:
                char = 1 + (float(df3["спец скидка"][j].strip('%')) / 100)
                mrrc_discount_foreign_brands.append(char)
                flag = 1
        if mrrc_foreign[k] and flag == 0:
            mrrc_discount_foreign_brands.append(1.2)
        elif mrrc_foreign[k] == 0 and flag == 0:
            mrrc_discount_foreign_brands.append(0)
    mrrc_final = []
    for i in range(len(products["id"])):
        if mrrc[i]:
            mrrc_final.append(mrrc[i])
        elif mrrc_foreign[i]:
            mrrc_final.append(mrrc_foreign[i]*mrrc_discount_foreign_brands[i])
    products = products.assign(mrrc=mrrc, mrrc_foreign=mrrc_foreign, mrrc_discount_foreign_brands=mrrc_discount_foreign_brands, mrrc_final=mrrc_final)
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json",
                "locale": "ru"
            }
    response = requests.get(url=url, headers=headers)
    data = response.json()
    commisions = []
    for i in products["title"]:
        for j in data["report"]:
            if i == j["subjectName"]:
                commisions.append(j['kgvpMarketplace'])
    products = products.assign(commision=commisions)
    margin = [round(x/10, 2) if x>500 else 50 for x in products["wholesale_price_wholesale_pack"]]
    url = 'https://common-api.wildberries.ru/api/v1/tariffs/box'
    url2 = 'https://common-api.wildberries.ru/api/v1/tariffs/pallet'
    res = str(datetime.date.today())
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    params = {"date":res}
    response1 = requests.get(url=url, headers=headers, params=params)
    data = response1.json()
    for i in data['response']['data']['warehouseList']:
        if i['warehouseName'] == "Маркетплейс":
            all_logistic_costs = [float(i['boxDeliveryBase'].replace(",", '.')) + float(i['boxDeliveryLiter'].replace(",", '.'))*(j-1) if j>1 else float(i['boxDeliveryBase'].replace(",", '.')) for j in products["volume"]]
    products = products.assign(margin=margin, full_logistic_price=all_logistic_costs)
    return products

def get_data_gela():
    def get_chrt_ids_by_imt_id(api_key, imt_id, all_cards):
        chrt_id = 0
        subjectName = 0
        value = 0
        wb_art = 0
        for card in all_cards:
            if str(card["vendorCode"]) == str(imt_id):
                chrt_id = card["sizes"][0]["skus"][0]
                subjectName = card["subjectName"]
                value = card["dimensions"]['width'] * card["dimensions"]['height'] *card ["dimensions"]['length'] * 0.001
                wb_art = card["nmID"]
        return chrt_id, subjectName, value, wb_art
   
    FTP_HOST = "www.gela.ru"
    FTP_USER = "vigruzka"
    FTP_PASS = "KpS943etp1"
    LOCAL_DIR = "downloaded_files"

   
    os.makedirs(LOCAL_DIR, exist_ok=True)

    try:
        
        with FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASS) as ftp:
            print("Успешное подключение к серверу")

           
            ftp.set_pasv(True)

            
            files = ftp.nlst()
            print(f"Найдено файлов на сервере: {len(files)}")

            
            for filename in files:
                local_path = os.path.join(LOCAL_DIR, filename)

                try:
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f"RETR {filename}", f.write)
                    print(f"Файл '{filename}' успешно скачан")

                except Exception as e:
                    print(f"Ошибка при скачивании '{filename}': {e}")

    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        print("Завершение работы скрипта")
    def clean_price(price_str):
        """Очистка ценовых значений от лишних символов"""
        if not price_str:
            return None
        
        cleaned = price_str.replace('\xa0', '').replace(' ', '').replace(',', '.')
        cleaned = ''.join(c for c in cleaned if c.isdigit() or c in '.-')
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    def parse_mpprice(xml_file):
        """Парсер для поиска MPPrice с обработкой специальных символов"""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
        except Exception as e:
            print(f"Ошибка чтения файла: {e}")
            return None  

        print("=== Анализ MPPrice ===")

        shop = root.find('shop')
        if not shop:
            print("Ошибка: отсутствует секция <shop>")
            return None

        offers = shop.find('offers')
        if not offers:
            print("В файле отсутствуют товарные предложения (offers)")
            return None

        
        mpprice_aliases = [
            'mpprice', 
            'mp_price',
            'wholesale_price',
            'opt_price',
            'wholesale',
            'minimal_price',
            'min_price'
        ]

        
        stats = {
            'total_offers': 0,
            'with_valid_mpprice': 0,
            'invalid_prices': 0,
            'found_fields': set(),
            'examples': []
        }

        
        products_data = []

        for offer in offers.findall('offer'):
            stats['total_offers'] += 1
            mpprice = None
            regular_price = None
            used_field = None

           
            price_str = offer.findtext('price')
            regular_price = clean_price(price_str)

       
            for field in mpprice_aliases:
                if offer.findtext(field):
                    mpprice = clean_price(offer.findtext(field))
                    used_field = field
                    break

     
            product_info = {
                'id': offer.get('id'),
                'name': offer.findtext('name', 'Без названия').strip(),
                'price': regular_price,
                'mpprice': mpprice
            }

           
            products_data.append(product_info)

            if mpprice is not None:
                stats['with_valid_mpprice'] += 1
                stats['found_fields'].add(used_field)

               
                if len(stats['examples']) < 5:
                    stats['examples'].append({
                        'id': product_info['id'],
                        'name': product_info['name'],
                        'price': regular_price if regular_price is not None else 'N/A',
                        'mpprice': mpprice,
                        'field': used_field
                    })
            else:
                stats['invalid_prices'] += 1

       
        print(f"\nВсего товаров: {stats['total_offers']}")
        print(f"Найдено валидных MPPrice: {stats['with_valid_mpprice']} ({stats['with_valid_mpprice']/stats['total_offers']:.1%})")
        print(f"Некорректных цен: {stats['invalid_prices']}")
        print(f"Используемые поля: {', '.join(stats['found_fields']) or 'не найдены'}")

        if stats['examples']:
            print("\nПримеры товаров с MPPrice:")
            for example in stats['examples']:
                print(f"\nID: {example['id']}")
                print(f"Название: {example['name']}")
                print(f"Обычная цена: {example['price']}")
                print(f"MPPrice ({example['field']}): {example['mpprice']}")
                if isinstance(example['price'], (int, float)):
                    print(f"Разница: {float(example['price']) - float(example['mpprice']):.2f}")

        if not stats['found_fields'] and stats['total_offers'] > 0:
            print("\nРекомендации:")
            print("1. Проверьте используемые названия для оптовых цен в вашей системе")
            print("2. Добавьте дополнительные варианты в список mpprice_aliases")
            print("3. Пример тега с ценой:")
            print(ET.tostring(offers.find('offer')[0], encoding='unicode')[:200] + "...")

       
        return products_data

 
    products_gela = parse_mpprice('downloaded_files/gela_rrc_price.xml')
    xd = pd.read_excel("))))).xlsx")
    xddd = xd.groupby('id').agg({
        'name': 'first',
        'barcode': 'sum',
        'ruboptprice': 'sum',
        'rubretailprice': 'sum',
        'Сопутствующие товары': 'first'
    }).reset_index()
    WB_WAREHOUSE_ID = "853064"
    WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODc4MDQzNywiaWQiOiIwMTk4MjdhZi0wMDk4LTc4NDAtYTRkOS0zMDk5ODQzNmQxYmUiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5MjQxMTIsInMiOjE2MTI2LCJzaWQiOiI1N2MyZGM2Zi0xMzFlLTRiNWQtOWQ2Ni0xYzBjYjkzYjk3ZmIiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.omhKkKF-ytYwoejdr1xxwYaCkgD-hw2uSKN37Xfk5Mq3LmEW193c25pUDrb4JRw-Ih4Q7rxB4sMSc4eSpvDuhg"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    STOCKS_URL = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{WB_WAREHOUSE_ID}"

    def get_all_cards(api_key):
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }

        all_cards = []
        limit = 100  
        cursor = None  

        while True:
            sleep(6)
           
            payload = {
                "settings": {
                    "cursor": {
                        "limit": limit
                    },
                    "filter": {
                        "withPhoto": -1 
                    }
                }
            }

           
            if cursor:
                payload["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
                payload["settings"]["cursor"]["nmID"] = cursor["nmID"]
           
            response = requests.post(url, headers=headers, json=payload)

           
            if response.status_code != 200:
                print(f"Ошибка {response.status_code}: {response.text}")
                break

            data = response.json()
            cards = data.get("cards", [])
            total = data.get("total", 0) 

            if not cards:
                break  

            all_cards.extend(cards)
            print(f"Загружено: {len(all_cards)} / {total}")

            
            if len(cards) < limit:
                break

            
            last_card = cards[-1]
            cursor = {
                "updatedAt": last_card["updatedAt"],
                "nmID": last_card["nmID"]
            }

        return all_cards


    all_cards2 = get_all_cards(WB_API_KEY)
    gela_products = xddd.groupby("id").agg({
        "ruboptprice": "sum",
        "rubretailprice": 'sum'
    }).reset_index()
    new_column = []
    for i in gela_products["id"]:
        flag = 0
        for j in all_cards2:
            if str(i) == j["vendorCode"] and flag == 0:
                new_column.append("наш товар")
                flag = 1
        if flag == 0:
            new_column.append(np.nan)
    gela_products = gela_products.assign(need=new_column)
    gela_products = gela_products.dropna()
    gela_products = gela_products.reset_index()
    gela_products = gela_products.drop(columns=["index", "need", 'ruboptprice', 'rubretailprice'])
    opt_price = []
    mp_price = []
    for i in gela_products["id"]:
        flag = 0
        for j in products_gela:
            if str(i) == j["id"]:
                opt_price.append(j["price"])
                mp_price.append(j["mpprice"])
                flag = 1
                break
        if flag == 0:
            opt_price.append(np.nan)
            mp_price.append(np.nan)
    gela_products = gela_products.assign(price = opt_price, mrrc_final=mp_price)
    barcodes = []
    titles = []
    volumes = []
    wb_arts = []
    for j, i in enumerate(gela_products["id"]):
        chrt, title, volume, wb_art = get_chrt_ids_by_imt_id(WB_API_KEY, i, all_cards2)
        if chrt:
            barcodes.append(chrt)
            titles.append(title)
            volumes.append(volume)
            wb_arts.append(str(wb_art))
    gela_products = gela_products.assign(barcode=barcodes, title=titles, volume=volumes, wb_articles=wb_arts)
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json",
                "locale": "ru"
            }
    response = requests.get(url=url, headers=headers)
    data = response.json()
    commisions = []
    for i in gela_products["title"]:
        for j in data["report"]:
            if i == j["subjectName"]:
                commisions.append(j['kgvpMarketplace'])
    gela_products = gela_products.assign(commision=commisions)
    
    margin = [round(x/10, 2) if x>500 else 50 for x in gela_products["price"]]
    url = 'https://common-api.wildberries.ru/api/v1/tariffs/box'
    url2 = 'https://common-api.wildberries.ru/api/v1/tariffs/pallet'
    res = str(datetime.date.today())
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    params = {"date":res}
    response1 = requests.get(url=url, headers=headers, params=params)
    data = response1.json()
    for i in data['response']['data']['warehouseList']:
        if i['warehouseName'] == "Маркетплейс":
            all_logistic_costs = [float(i['boxDeliveryBase'].replace(",", '.')) + float(i['boxDeliveryLiter'].replace(",", '.'))*(j-1) if j>1 else float(i['boxDeliveryBase'].replace(",", '.')) for j in gela_products["volume"]]
    gela_products = gela_products.assign(margin=margin, full_logistic_price=all_logistic_costs)
    gela_products = gela_products[gela_products["price"].notna()]
    gela_products = gela_products.reset_index()
    gela_products = gela_products.drop(columns=["index"])
    return gela_products

def get_data_gamma2():
    def get_chrt_ids_by_imt_id(api_key, imt_id, all_cards):
        chrt_id = 0
        subjectName = 0
        value = 0
        wb_art = 0
        for card in all_cards:
            if str(card["vendorCode"]) == str(imt_id):
                chrt_id = card["sizes"][0]["skus"][0]
                subjectName = card["subjectName"]
                value = card["dimensions"]['width'] * card["dimensions"]['height'] *card ["dimensions"]['length'] * 0.001
                wb_art = card["nmID"]
        return chrt_id, subjectName, value, wb_art
    WB_WAREHOUSE_ID = "853064"
    WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODc4MDQzNywiaWQiOiIwMTk4MjdhZi0wMDk4LTc4NDAtYTRkOS0zMDk5ODQzNmQxYmUiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5MjQxMTIsInMiOjE2MTI2LCJzaWQiOiI1N2MyZGM2Zi0xMzFlLTRiNWQtOWQ2Ni0xYzBjYjkzYjk3ZmIiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.omhKkKF-ytYwoejdr1xxwYaCkgD-hw2uSKN37Xfk5Mq3LmEW193c25pUDrb4JRw-Ih4Q7rxB4sMSc4eSpvDuhg"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    STOCKS_URL = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{WB_WAREHOUSE_ID}"
    SUPPLIER_URL="https://shop.firma-gamma.ru/api/v1.0/stock.php?type=csv"
    response = requests.get(
            SUPPLIER_URL,
            auth=requests.auth.HTTPBasicAuth("natalia-b2005", "Holst110878"),
        )
    response.raise_for_status()
    content = response.content.decode("windows-1251", errors='replace')
    column_names = [
        'id',
        'conversion_factor',
        'wholesale_price_retail_pack',
        'min_recommended_retail_price',
        'name',
        'availability_status',
        'wholesale_price_wholesale_pack'
    ]
    df = pd.read_csv(
        StringIO(content),  
        sep='\t',            
        header=None,                
        names=column_names,           
        quoting=csv.QUOTE_MINIMAL,     
        doublequote=True,              
        escapechar=None,               
    )
    def get_all_cards(api_key):
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }
        
        all_cards = []
        limit = 100 
        cursor = None  
        
        while True:
            sleep(5)
            
            payload = {
                "settings": {
                    "cursor": {
                        "limit": limit
                    },
                    "filter": {
                        "withPhoto": -1 
                    }
                }
            }
            
            if cursor:
                payload["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
                payload["settings"]["cursor"]["nmID"] = cursor["nmID"]
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code != 200:
                print(f"Ошибка {response.status_code}: {response.text}")
                break
            
            data = response.json()
            cards = data.get("cards", [])
            total = data.get("total", 0)  
            
            if not cards:
                break  
            
            all_cards.extend(cards)
            print(f"Загружено: {len(all_cards)} / {total}")
            
          
            if len(cards) < limit:
                break
            
   
            last_card = cards[-1]
            cursor = {
                "updatedAt": last_card["updatedAt"],
                "nmID": last_card["nmID"]
            }
        
        return all_cards
    
    all_cards = get_all_cards(WB_API_KEY)
    products = df.groupby('id').agg({
        'name': 'first',
        'wholesale_price_wholesale_pack': 'sum',
        'conversion_factor': 'sum'
        }).reset_index()
    
    stocks_data = []
    barcodes = []
    titles = []
    volumes = []
    wb_arts = []
    for i in products["id"]:
        chrt, title, volume, wb_art = get_chrt_ids_by_imt_id(WB_API_KEY, i, all_cards)
        if chrt:
            barcodes.append(chrt)
            titles.append(title)
            volumes.append(volume)
            wb_arts.append(str(wb_art))
        else:
            barcodes.append(np.nan)
            titles.append(np.nan)
            volumes.append(np.nan)
            wb_arts.append(np.nan)
    products = products.assign(barcode=barcodes, title=titles, volume=volumes, wb_articles=wb_arts)
    products = products.dropna()
    products = products.reset_index()
    products = products.drop("index", axis=1)
    url = "https://docs.google.com/spreadsheets/d/1dnnhSbIb7kSMu4jDKsdqwsOpIEWVqFK8x1omO6OJhOk/export?format=csv"
    df2 = pd.read_csv(url)
    mrrc_foreign = []
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df2["Внутренний id"]):    
            if n == i:
                mrrc_foreign.append(df2["РРЦ, руб"][j])
                flag = 1
        if flag == 0:
            mrrc_foreign.append(0)
    mrrc = []
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df["id"]):
            if mrrc_foreign[k] == 0:
                if i == n:
                    mrrc.append(df["min_recommended_retail_price"][j]*df["conversion_factor"][j])
            else:
                break
        if mrrc_foreign[k] != 0:
            mrrc.append(0)
    mrrc_discount_foreign_brands = []
    url = "https://docs.google.com/spreadsheets/d/1aTu5tToBy6MYct6oFXr3u3CGIA8NGJFlJHcD5TshXxE/export?format=csv"
    df3 = pd.read_csv(url)
    for k, n in enumerate(products["id"]):
        flag = 0
        for j, i in enumerate(df3["Артикул продавца"]):
            if i == n and type(df3["спец скидка"][j]) == str:
                char = 1 + (float(df3["спец скидка"][j].strip('%')) / 100)
                mrrc_discount_foreign_brands.append(char)
                flag = 1
        if mrrc_foreign[k] and flag == 0:
            mrrc_discount_foreign_brands.append(1.2)
        elif mrrc_foreign[k] == 0 and flag == 0:
            mrrc_discount_foreign_brands.append(0)
    mrrc_final = []
    for i in range(len(products["id"])):
        if mrrc[i]:
            mrrc_final.append(mrrc[i])
        elif mrrc_foreign[i]:
            mrrc_final.append(mrrc_foreign[i]*mrrc_discount_foreign_brands[i])
    products = products.assign(mrrc=mrrc, mrrc_foreign=mrrc_foreign, mrrc_discount_foreign_brands=mrrc_discount_foreign_brands, mrrc_final=mrrc_final)
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json",
                "locale": "ru"
            }
    response = requests.get(url=url, headers=headers)
    data = response.json()
    commisions = []
    for i in products["title"]:
        for j in data["report"]:
            if i == j["subjectName"]:
                commisions.append(j['kgvpMarketplace'])
    products = products.assign(commision=commisions)
    margin = [round(x/10, 2) if x>500 else 50 for x in products["wholesale_price_wholesale_pack"]]
    url = 'https://common-api.wildberries.ru/api/v1/tariffs/box'
    url2 = 'https://common-api.wildberries.ru/api/v1/tariffs/pallet'
    res = str(datetime.date.today())
    headers = {
                "Authorization": WB_API_KEY,
                "Content-Type": "application/json"
            }
    params = {"date":res}
    response1 = requests.get(url=url, headers=headers, params=params)
    data = response1.json()
    for i in data['response']['data']['warehouseList']:
        if i['warehouseName'] == "Маркетплейс":
            all_logistic_costs = [float(i['boxDeliveryBase'].replace(",", '.')) + float(i['boxDeliveryLiter'].replace(",", '.'))*(j-1) if j>1 else float(i['boxDeliveryBase'].replace(",", '.')) for j in products["volume"]]
    products = products.assign(margin=margin, full_logistic_price=all_logistic_costs)
    return products

def updating_data(products, taxe, acquiring, expense, our_exp):
    taxes = [taxe for x in range(len(products["id"]))]
    acq = [acquiring for x in range(len(products["id"]))]
    expenses = [expense for x in range(len(products["id"]))]
    full_taxes = [taxes[i]+acq[i]+expenses[i] + j for i, j in enumerate(products["commision"])]
    our_expenses = [our_exp for x in range(len(products["id"]))]
    products = products.assign(taxe=taxes, acquiring=acq, expense=expenses, full_taxe=full_taxes, our_expense=our_expenses)
    try:
        full_prices = [round((products["wholesale_price_wholesale_pack"][i]+products["our_expense"][i]+products["margin"][i]+products["full_logistic_price"][i])/(1-(j/100)), 2) for i, j in enumerate(products["full_taxe"])]
    except:
        full_prices = [round((products["price"][i]+products["our_expense"][i]+products["margin"][i]+products["full_logistic_price"][i])/(1-(j/100)), 2) for i, j in enumerate(products["full_taxe"])]
    products = products.assign(full_price=full_prices)
    final_price = [round(max(i, products["mrrc_final"][j]), 2) for j, i in enumerate(products["full_price"])]
    final_price_without_discount = [round(i*1.3, 2) for i in final_price]
    wb_discount = [round(((final_price_without_discount[i] - final_price[i])/final_price_without_discount[i])*100, 2) for i in range(len(final_price_without_discount))]
    products = products.assign(final_price=final_price, final_price_without_discount=final_price_without_discount, wb_discount=wb_discount)
    return products


class GoogleSheetsManager:
    """Управление Google Sheets с сохранением пользовательских изменений"""
    
    def __init__(self, creds_path='credentials.json'):
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, self.scope)
        self.client = gspread.authorize(self.creds)
    
    def load_user_changes(self, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
        """Загружает пользовательские изменения из листа"""
        try:
            sheet = self.client.open_by_key(spreadsheet_id).worksheet(sheet_name)
            return pd.DataFrame(sheet.get_all_records())
        except gspread.WorksheetNotFound:
            return pd.DataFrame()

    def load_base_values(self, spreadsheet_id: str) -> dict:
        """Загружает базовые значения из первого ряда листов Gamma, Gela или GAMMA_ART_ALLIANCE"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            base_values = {
                'taxe': 8,
                'acq': 2,
                'exp': 3.55,
                'our_exp': 70
            }
            
            # Проверяем листы на наличие данных
            for sheet_name in ['Gamma', 'Gela', 'GAMMA_ART_ALLIANCE']:
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    records = worksheet.get_all_records()
                    if records:
                        first_row = records[0]
                        if 'taxe' in first_row:
                            base_values['taxe'] = float(first_row['taxe'])
                        if 'acquiring' in first_row:
                            base_values['acq'] = float(first_row['acquiring'])
                        if 'expense' in first_row:
                            base_values['exp'] = float(first_row['expense'])
                        if 'our_expense' in first_row:
                            base_values['our_exp'] = float(first_row['our_expense'])
                        break  # берем значения из первого непустого листа
                except gspread.WorksheetNotFound:
                    continue
            
            return base_values
        except Exception as e:
            print(f"Ошибка загрузки базовых значений: {e}")
            return {
                'taxe': 8,
                'acq': 2,
                'exp': 3.55,
                'our_exp': 70
            }

    def save_data(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
        """Обновляет данные в листе"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=max(df.shape[0] + 1, 100), 
                    cols=max(df.shape[1], 10)
                )
            
            # Сохраняем текущие значения изменяемых столбцов
            if sheet_name in ['Gamma', 'Gela', 'GAMMA_ART_ALLIANCE']:
                try:
                    existing_data = worksheet.get_all_records()
                    if existing_data:
                        existing_df = pd.DataFrame(existing_data)
                        for col in ['taxe', 'acquiring', 'expense', 'our_expense']:
                            if col in existing_df.columns and col in df.columns:
                                df[col] = existing_df[col]
                except Exception as e:
                    print(f"Ошибка при сохранении существующих значений: {e}")
            
            # Обновление данных
            data = [df.columns.values.tolist()] + df.fillna('').astype(str).values.tolist()
            worksheet.update('A1', data)
            return True
        except Exception as e:
            print(f"Ошибка сохранения: {e}")
            return False
    def get_products_data(self, spreadsheet_id: str) -> tuple:
        """
        Получает данные из листов Gamma и Gela и возвращает два списка словарей
        с нужными полями для каждого листа
        
        Возвращает: (gamma_data, gela_data)
        """
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Получаем данные для Gamma
            gamma_sheet = spreadsheet.worksheet("Gamma")
            gamma_records = gamma_sheet.get_all_records()
            gamma_data = [
                {
                    'wb_articles': item.get('wb_articles', ''),
                    'barcode': item.get('barcode', ''),
                    'final_price': item.get('final_price', 0),
                    'final_price_without_discount': item.get('final_price_without_discount', 0),
                    'wb_discount': item.get('wb_discount', 0)
                }
                for item in gamma_records
            ]
            
            # Получаем данные для Gela
            gela_sheet = spreadsheet.worksheet("Gela")
            gela_records = gela_sheet.get_all_records()
            gela_data = [
                {
                    'wb_articles': item.get('wb_articles', ''),
                    'barcode': item.get('barcode', ''),
                    'final_price': item.get('final_price', 0),
                    'final_price_without_discount': item.get('final_price_without_discount', 0),
                    'wb_discount': item.get('wb_discount', 0)
                }
                for item in gela_records
            ]
            gamma2_sheet = spreadsheet.worksheet("GAMMA_ART_ALLIANCE")
            gamma2_records = gamma2_sheet.get_all_records()
            gamma2_data = [
                {
                    'wb_articles': item.get('wb_articles', ''),
                    'barcode': item.get('barcode', ''),
                    'final_price': item.get('final_price', 0),
                    'final_price_without_discount': item.get('final_price_without_discount', 0),
                    'wb_discount': item.get('wb_discount', 0)
                }
                for item in gamma2_records
            ]
            
            return gamma_data, gela_data, gamma2_data
            
        except Exception as e:
            print(f"Ошибка при получении данных из Google Sheets: {e}")
            return [], []


class DataProcessor:
    """Обработка данных с сохранением пользовательских изменений"""
    
    CHANGEABLE_COLS = ['taxe', 'acquiring', 'expense', 'our_expense']
    
    def __init__(self, gela_data: pd.DataFrame, gamma_data: pd.DataFrame, gamma_art_alliance_data: pd.DataFrame):
        self.original_gela = gela_data
        self.original_gamma = gamma_data
        self.original_gamma_art_alliance = gamma_art_alliance_data
        self.gela = gela_data.copy()
        self.gamma = gamma_data.copy()
        self.gamma_art_alliance = gamma_art_alliance_data.copy()
        self.gela['source'] = 'Gela'
        self.gamma['source'] = 'Gamma'
        self.gamma_art_alliance['source'] = 'GAMMA_ART_ALLIANCE'
        
    def apply_user_changes(self, changes_df: pd.DataFrame):
        """Применяет пользовательские изменения к данным"""
        if changes_df.empty:
            return
            
        for _, row in changes_df.iterrows():
            source = row['source']
            item_id = row['id']
            col_name = row['column']
            new_value = row['value']
            
            if col_name not in self.CHANGEABLE_COLS:
                continue
                
            if source == 'Gela':
                df = self.gela
            elif source == 'Gamma':
                df = self.gamma
            else:
                df = self.gamma_art_alliance
                
            mask = df['id'] == item_id
            
            if not mask.any():
                print(f"Товар {item_id} не найден в {source}")
                continue
                
            idx = df.index[mask].tolist()[0]
            df.at[idx, col_name] = new_value
            self.recalculate_item(df, idx)
    
    def recalculate_item(self, df: pd.DataFrame, idx: int):
        """Пересчитывает значения для конкретного товара"""
        row = df.loc[idx]
        
        # 1. Пересчет налоговой нагрузки
        full_tax = (
            row['taxe'] + 
            row['acquiring'] + 
            row['expense'] + 
            row['commision']
        )
        df.at[idx, 'full_taxe'] = full_tax
        
        # 2. Пересчет полной цены
        base_price = row['price'] if 'price' in row else row['wholesale_price_wholesale_pack']
        full_price = (
            (base_price + 
             row['our_expense'] + 
             row['margin'] + 
             row['full_logistic_price']) / 
            (1 - full_tax / 100)
        )
        df.at[idx, 'full_price'] = round(full_price, 2)
        
        # 3. Пересчет финальной цены
        mrrc_col = 'mrrc_final' if 'mrrc_final' in df.columns else 'mrrc'
        final_price = max(
            df.at[idx, 'full_price'], 
            row[mrrc_col] if not pd.isna(row[mrrc_col]) else 0
        )
        df.at[idx, 'final_price'] = round(final_price, 2)
        
        # 4. Пересчет цены без скидки
        df.at[idx, 'final_price_without_discount'] = round(final_price * 1.3, 2)
        
        # 5. Пересчет скидки WB
        price_without_disc = df.at[idx, 'final_price_without_discount']
        discount = ((price_without_disc - final_price) / price_without_disc) * 100
        df.at[idx, 'wb_discount'] = round(discount, 2)

    def get_data(self, source: str) -> pd.DataFrame:
        """Возвращает данные для указанного источника"""
        if source == 'Gela':
            return self.gela
        elif source == 'Gamma':
            return self.gamma
        else:
            return self.gamma_art_alliance

def main():
    SPREADSHEET_ID = "1aHPOu8E33QYpNwWb1NKy8gnk0_dNbf-l_ubEyp5V1G4"
    
    gs_manager = GoogleSheetsManager()
    
    base_values = gs_manager.load_base_values(SPREADSHEET_ID)
    taxe = base_values['taxe']
    acq = base_values['acq']
    exp = base_values['exp']
    our_exp = base_values['our_exp']
    
    products = get_data_gamma()
    gamma_products = updating_data(products, taxe, acq, exp, our_exp)
    
    products_gela = get_data_gela()
    gela_products = updating_data(products_gela, taxe, acq, exp, our_exp)
    
    products_gamma_art_alliance = get_data_gamma2() 
    gamma_art_alliance_products = updating_data(products_gamma_art_alliance, taxe, acq, exp, our_exp)
    
    processor = DataProcessor(gela_products, gamma_products, gamma_art_alliance_products)
    
    changes_df = gs_manager.load_user_changes(SPREADSHEET_ID, "User_Changes")
    

    processor.apply_user_changes(changes_df)
    

    gs_manager.save_data(SPREADSHEET_ID, "Gamma", processor.get_data('Gamma'))
    gs_manager.save_data(SPREADSHEET_ID, "Gela", processor.get_data('Gela'))
    gs_manager.save_data(SPREADSHEET_ID, "GAMMA_ART_ALLIANCE", processor.get_data('GAMMA_ART_ALLIANCE'))
    
    print(f"Данные успешно обновлены: {datetime.datetime.now(pytz.utc)}")
    gamma_products2, gela_products2, g_a_a = gs_manager.get_products_data(SPREADSHEET_ID)
    update_wb_prices("eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODAxNjk0MCwiaWQiOiIwMTk3ZmEyYy1mOGEzLTdiNzgtYTJmOS1mNjM1ZTE5ZjRkYmMiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5NzQ3MTQsInMiOjE2MTI2LCJzaWQiOiIyMDBjMWJmMC0xM2UxLTQ4MmEtYTM1MS01NjlhNzgxN2NiMmQiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.FUZNpgyRFT7HTar-l860MUJGr2WBVRvcoVmU9n72-t22NmSeeKqeRa5Mmn7I7zB0WU-P-IWpDnDjpp0mNpo3yQ", gamma_products2)
    update_wb_prices("eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODc4MDQzNywiaWQiOiIwMTk4MjdhZi0wMDk4LTc4NDAtYTRkOS0zMDk5ODQzNmQxYmUiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5MjQxMTIsInMiOjE2MTI2LCJzaWQiOiI1N2MyZGM2Zi0xMzFlLTRiNWQtOWQ2Ni0xYzBjYjkzYjk3ZmIiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.omhKkKF-ytYwoejdr1xxwYaCkgD-hw2uSKN37Xfk5Mq3LmEW193c25pUDrb4JRw-Ih4Q7rxB4sMSc4eSpvDuhg", gela_products2)
    update_wb_prices("eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODc4MDQzNywiaWQiOiIwMTk4MjdhZi0wMDk4LTc4NDAtYTRkOS0zMDk5ODQzNmQxYmUiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5MjQxMTIsInMiOjE2MTI2LCJzaWQiOiI1N2MyZGM2Zi0xMzFlLTRiNWQtOWQ2Ni0xYzBjYjkzYjk3ZmIiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.omhKkKF-ytYwoejdr1xxwYaCkgD-hw2uSKN37Xfk5Mq3LmEW193c25pUDrb4JRw-Ih4Q7rxB4sMSc4eSpvDuhg", g_a_a)

if __name__ == "__main__":
    main()
