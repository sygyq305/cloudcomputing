from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from azure.cosmos import CosmosClient
import numpy as np
import json
np.random.seed(0)
ENDPOINT = 'https://tutorial-uta-cse6332.documents.azure.com:443/'
KEY = 'fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw=='
client = CosmosClient(url=ENDPOINT, credential=KEY)

database = client.get_database_client('tutorial')
reviews = database.get_container_client('reviews')
cities = database.get_container_client('us_cities')


def classify_cities(total_cities, classes, k, words_number):
    sql_query = f"SELECT * FROM cities"
    items = cities.query_items(query=sql_query, enable_cross_partition_query=True)
    # with open("process/citys_list.txt", "r") as file:
    #     data_string = file.read()
    # citys_list = json.loads(data_string)
    cities_coordinates = []

    for item in items:
        item['lat'], item['lng'] = float(item['lat']), float(item['lng'])
        cities_coordinates.append([item['lat'], item['lng']])
    cities_coordinates = np.array(cities_coordinates)
    # 找到聚类中心
    kmeans = KMeans(n_clusters=classes, random_state=0).fit(cities_coordinates)
    cluster_centers = kmeans.cluster_centers_
    # 找到每个聚类中心最近的城市索引
    from scipy.spatial import distance

    center_city_indices = []
    for center in cluster_centers:
        # 计算每个城市与聚类中心的距离
        distances = [distance.euclidean(center, city) for city in cities_coordinates]
        # 找到最近城市的索引
        nearest_city_index = np.argmin(distances)
        center_city_indices.append(nearest_city_index)

    central_citys_dict = {}
    # 为每个城市分配一个初始类别（这里简化为使用种子城市的索引）
    city_categories = np.full(total_cities, -1)  # 所有城市初始类别设置为 -1
    for cluster_index, city_index in enumerate(center_city_indices):
        city_categories[city_index] = cluster_index  # 将中心城市的类别设置为聚类中心的索引
        central_citys_dict['item-' + str(city_index)] = cluster_index#中心城市字典

    # 使用KNN为每个城市分配类别
    nbrs = NearestNeighbors(n_neighbors=k, algorithm='auto').fit(cities_coordinates)
    distances, indices = nbrs.kneighbors(cities_coordinates)
    for i in range(total_cities):
        if city_categories[i] == -1:  # 仅为未分类的城市分配类别
            # 获取最近邻居的类别并排除未分类的邻居
            neighbors_categories = city_categories[indices[i]]
            valid_categories = neighbors_categories[neighbors_categories != -1]

            # 如果有已分类的邻居
            if len(valid_categories) > 0:
                city_categories[i] = np.bincount(valid_categories).argmax()
            else:
                # 没有已分类的邻居，使用最近的聚类中心
                city_coords = cities_coordinates[i]
                distances_to_centers = [distance.euclidean(city_coords, center) for center in cluster_centers]
                nearest_center_index = np.argmin(distances_to_centers)
                city_categories[i] = nearest_center_index

    city_categories_dict = {f"item-{i + 1}": category for i, category in enumerate(city_categories)}
    items = cities.query_items(query=sql_query, enable_cross_partition_query=True)
    for item in items:
        item_id = item['id']
        city_state_key = f"{item['city']}_{item['state']}"
        if item_id in city_categories_dict:
            city_categories_dict[city_state_key] = city_categories_dict.pop(item_id)
        if item_id in central_citys_dict:
            central_citys_dict[city_state_key] = central_citys_dict.pop(item_id)
            # {city:1,city:2...}

    # {1:city,2:city...}
    classify_cities_dict = {}
    for city, category in city_categories_dict.items():
        classify_cities_dict.setdefault(category, []).append(city)
    central_city_dict = {}
    for key, value in central_citys_dict.items():
        if value not in central_city_dict:
            central_city_dict[value] = key

    classify_cities_dict = {int(key): value for key, value in classify_cities_dict.items()}
    central_city_dict = {int(key): value for key, value in central_city_dict.items()}
    print(classify_cities_dict)
    print(central_city_dict)
    return classify_cities_dict, central_city_dict
