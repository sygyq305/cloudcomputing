import json
from math import sqrt
import time
from flask import (Flask, redirect, render_template, request, jsonify,
                   send_from_directory, url_for)
from azure.cosmos import CosmosClient

import redis
from flask import request, jsonify
from flask import Flask, render_template
from popular_words import popular
from categor_city import classify_cities
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


@app.route('/ass4')
def index2():
    return render_template('ass4.html')

@app.route('/cluster', methods=['GET'])
def knn_reviews():
    classes = int(request.args.get('classes'))
    k = int(request.args.get('k'))
    words = int(request.args.get('words'))
    cache_key = f"class_k_words:{classes}:{k}:{words}"
    cached_data = cache.get(cache_key)
    start_time = time.time()
    if cached_data:
        result = json.loads(cached_data)
        IsRedis = True
    else:
        total = 5393
        classify_cities_dict, central_city_dict = classify_cities(total_cities=total, classes=classes, k=k,
                                                                  words_number=words)
        category_words_counts, category_scores = popular(words, classify_cities_dict)
        # clusters_dict = {key: len(value) for key, value in classify_cities_dict.items()}
        # 正确提取每个类别的第一个键
        # first_keys = [list(cls.keys())[0] for cls in category_words_counts.values()]
        category_keywords = {}
        used_keywords = set()

        for category, words_counts in category_words_counts.items():
            sorted_words = sorted(words_counts, key=words_counts.get, reverse=True)
            for word in sorted_words:
                if word not in used_keywords:
                    category_keywords[category] = word
                    used_keywords.add(word)
                    break
        category_keywords_freq = {word: category_words_counts[category][word]
                                  for category, word in category_keywords.items()}

        total_counts_for_keywords = {keyword: 0 for keyword in category_keywords.values()}

        for category in category_words_counts.values():
            for keyword, count in category.items():
                if keyword in total_counts_for_keywords:
                    total_counts_for_keywords[keyword] += count

        print(category_keywords_freq)
        result = {
            'clusters': classify_cities_dict,
            'scores': category_scores,
            'centers': central_city_dict,
            'words': category_keywords_freq,
            'avg_words': total_counts_for_keywords
        }
        cache.setex(cache_key, 86400 * 5, json.dumps(result))  # 3600 seconds = 1 hour
        IsRedis = False
    result['IsRedis'] = IsRedis
    end = time.time()
    response_time = (end - start_time)*1000
    result['response_time'] = response_time
    # return jsonify(class_info=classify_cities_dict, center_city=central_city_dict, word_data=category_words_counts,
    #                score = category_scores)
    return jsonify(result)

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
