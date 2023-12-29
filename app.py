import json
from math import sqrt
import time
from flask import (Flask, redirect, render_template, request, jsonify,
                   send_from_directory, url_for)
from azure.cosmos import CosmosClient
import redis

# password is the "Primary" copied in "Access keys"
redis_passwd = "Bc1iRDDtiHjPDzGijIuzJ3Aidg2is3zHJAzCaM8NWPo="
# "Host name" in properties
redis_host = "sygyq.redis.cache.windows.net"
# SSL Port
redis_port = 6380

cache = redis.StrictRedis(
            host=redis_host, port=redis_port,
            db=0, password=redis_passwd,
            ssl=True,
        )

if cache.ping():
    print("pong")


# to delete all data in the cache
def purge_cache():
    for key in cache.keys():
        cache.delete(key.decode())


app = Flask(__name__)
# 定义连接字符串和数据库信息
ENDPOINT = 'https://tutorial-uta-cse6332.documents.azure.com:443/'
KEY = 'fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw=='
client = CosmosClient(url=ENDPOINT, credential=KEY)
database = client.get_database_client('tutorial')
reviews = database.get_container_client('reviews')
cities = database.get_container_client('us_cities')


@app.route('/')
def index():
    return render_template('chart.html')


@app.route('/index1')
def index1():
    return render_template('index1.html')


@app.route('/query', methods=['POST'])
def query_distances():
    start_time = time.time()  # 开始计时
    data = request.json
    print(data)
    city = data['city']
    state = data['state']
    page = int(data['page'])
    cache_key = f"closest_cities:{city}:{state}"

    # Try to get cached data
    cached_data = cache.get(cache_key)
    if cached_data:
        sorted_result = json.loads(cached_data)
        distances = get_sorted_distances10(sorted_result, page)
        IsRedis = True
    else:
        query = "SELECT c.lat, c.lng FROM cities c WHERE c.city = @city and c.state = @state"
        params = [dict(name="@city", value=city), dict(name="@state", value=state)]
        result1 = list(cities.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True
        ))
        tmp = result1[0]
        lat = float(tmp['lat'])
        lng = float(tmp['lng'])
        result = []
        query = "SELECT * FROM cities"
        items = cities.query_items(query, enable_cross_partition_query=True)
        for item in items:
            citys = {}
            citys['city'] = item['city']
            citys['state'] = item['state']
            citys['lat'] = item['lat']
            citys['lng'] = item['lng']
            citys['Eular distance'] = sqrt((float(item['lat']) - lat) ** 2 + (float(item['lng']) - lng) ** 2)
            result.append(citys)
        sorted_result = sorted(result, key=lambda x: x['Eular distance'])
        distances = get_sorted_distances10(sorted_result, page)
        # Cache the data
        cache.setex(cache_key, 86400*5, json.dumps(sorted_result))  # 3600 seconds = 1 hour
        IsRedis = False
    response_time = (time.time() - start_time) * 1000  # 计算响应时间

    return jsonify({
        'distances': distances,
        'response_time': response_time,
        'IsRedis': IsRedis,
    })


def get_sorted_distances10(result, page):
    per_page = 50
    start_index = (page - 1) * per_page
    end_index = start_index + per_page

    return result[start_index:end_index]


@app.route('/line_score', methods=['POST'])
def line_score():
    start_time = time.time()  # 开始计时
    data = request.json
    # print(data)
    city = data['city']
    state = data['state']
    page = int(data['page'])
    cache_key = f"closest_cities:{city}:{state}:score"

    # Try to get cached data
    cached_data = cache.get(cache_key)
    if cached_data:
        sorted_result = json.loads(cached_data)
        distances = get_sorted_distances11(sorted_result, page)
        IsRedis = True

    else:
        query = "SELECT c.lat, c.lng FROM cities c WHERE c.city = @city and c.state = @state"
        params = [dict(name="@city", value=city), dict(name="@state", value=state)]
        result1 = list(cities.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True
        ))
        tmp = result1[0]
        lat = float(tmp['lat'])
        lng = float(tmp['lng'])
        result = []
        query = "SELECT * FROM cities"
        items = cities.query_items(query, enable_cross_partition_query=True)
        for item in items:
            citys = {}
            citys['city'] = item['city']
            citys['state'] = item['state']
            citys['lat'] = item['lat']
            citys['lng'] = item['lng']
            citys['Eular distance'] = sqrt((float(item['lat']) - lat) ** 2 + (float(item['lng']) - lng) ** 2)
            result.append(citys)
        sorted_result = sorted(result, key=lambda x: x['Eular distance'])
        for item in sorted_result:
            cit = item['city']
            # print(cit)
            query = "SELECT * FROM reviews c WHERE c.city = @cit "
            params = [dict(name="@cit", value=cit)]
            itemss = list(reviews.query_items(
                query=query,
                parameters=params,
                enable_cross_partition_query=True
            ))
            score_sum = sum(float(item1['score']) for item1 in itemss)
            print(score_sum)
            print(len(itemss))
            if len(itemss) == 0:
                item['score_average'] = 0
            else:
                score_average = score_sum / len(itemss)
                item['score_average'] = score_average
                print(score_average)
        distances = get_sorted_distances11(sorted_result, page)
        # Cache the data
        cache.setex(cache_key, 3600*24*5, json.dumps(sorted_result))  # 3600 seconds = 1 hour
        IsRedis = False
    response_time = (time.time() - start_time) * 1000  # 计算响应时间
    print(distances)
    return jsonify({
        'distances': distances,
        'response_time': response_time,
        'IsRedis': IsRedis,
    })


def get_sorted_distances11(result, page):
    per_page = 10
    start_index = (page - 1) * per_page
    end_index = start_index + per_page

    return result[start_index:end_index]


@app.route('/purge_cache', methods=['POST'])
def handle_purge_cache():
    purge_cache()
    return render_template('index.html', message='Cache purged successfully!')


if __name__ == '__main__':
   app.run(debug=True)
