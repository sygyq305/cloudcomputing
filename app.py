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
connection_string = "AccountEndpoint=https://tutorial-uta-cse6332.documents.azure.com:443/;AccountKey=fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw==;"
database_name = "tutorial"
container_name_cities = "us_cities"
container_name_reviews = "reviews"

# 初始化Cosmos DB客户端
client = CosmosClient.from_connection_string(connection_string)

# 获取对数据库的引用
database = client.get_database_client(database_name)

# 获取对容器（表）的引用
container_cities = database.get_container_client(container_name_cities)
# container_reviews = database.get_container_client(container_name_reviews)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data/closest_cities', methods=['GET'])
def closest_cities():
    start_time = time.time()
    city = request.args.get('city', '')
    page = int(request.args.get('page', ''))
    page_size = int(request.args.get('page_size', ''))

    # Generate a unique key for caching
    cache_key = f"closest_cities:{city}:{page}:{page_size}"

    # Try to get cached data
    cached_data = cache.get(cache_key)
    if cached_data:
        # Data is in cache
        current_page_records = json.loads(cached_data)
        from_cache = True
    else:
        query = "SELECT c.lat, c.lng FROM c WHERE c.city = @city"
        params = [dict(name="@city", value=city)]
        result1 = list(container_cities.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True
        ))
        tmp = result1[0]
        lat = float(tmp['lat'])
        lng = float(tmp['lng'])
        result = []
        query = "SELECT * FROM c"
        items = container_cities.query_items(query, enable_cross_partition_query=True)
        for item in items:
            citys = {}
            citys['city'] = item['city']
            citys['lat'] = item['lat']
            citys['lng'] = item['lng']
            citys['Eular distance'] = sqrt((float(item['lat']) - lat) ** 2 + (float(item['lng']) - lng) ** 2)
            result.append(citys)

        sorted_result = sorted(result, key=lambda x: x['Eular distance'])
        # 计算当前页的起始和结束索引
        start_index = (page - 1) * page_size
        end_index = start_index + page_size

        # 提取当前页的记录
        current_page_records = sorted_result[start_index:end_index]
        # Cache the data
        cache.setex(cache_key, 3600, json.dumps(current_page_records))  # 3600 seconds = 1 hour
        from_cache = False
    end_time = time.time()  # 记录程序结束运行的时间
    elapsed_time = (end_time - start_time) * 1000  # 计算运行时间并转换为毫秒
    current_page_records.append({'the time of computing the response': f'{elapsed_time:.3f}'+'ms'})
    current_page_records.append({'the time of computing the response': f'{from_cache}'})
    print(current_page_records)
    return jsonify(current_page_records)


@app.route('/purge_cache', methods=['POST'])
def handle_purge_cache():
    purge_cache()
    return render_template('index.html', message='Cache purged successfully!')


if __name__ == '__main__':
   app.run(debug=True, host="127.0.0.1", port=8080)
