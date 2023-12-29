from collections import Counter
from azure.cosmos import CosmosClient
from categor_city import classify_cities
import json
ENDPOINT = 'https://tutorial-uta-cse6332.documents.azure.com:443/'
KEY = 'fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw=='
client = CosmosClient(url=ENDPOINT, credential=KEY)

database = client.get_database_client('tutorial')
reviews = database.get_container_client('reviews')
cities = database.get_container_client('us_cities')


def popular(word_number, cities_dict):
    sql_query = f"SELECT reviews.city,reviews.score,reviews.review,reviews.id FROM reviews "#OFFSET 0 LIMIT 1000"
    items = reviews.query_items(query=sql_query, enable_cross_partition_query=True)
    reviews_list = []
    citys_list = []
    for item in items:
        # print(item)
        reviews_list.append(item)
        # break
    sql_query = f"SELECT cities.id,cities.population,cities.city FROM cities"
    items_citys = cities.query_items(query=sql_query, enable_cross_partition_query=True)
    for item_city in items_citys:
        citys_list.append(item_city)
        # break

    # with open("process/reviews_list.txt", "r") as file:
    #     data_string = file.read()
    # reviews_list = json.loads(data_string)
    # with open("process/citys_list.txt", "r") as file:
    #     data_string = file.read()
    # citys_list = json.loads(data_string)

    stopwords = []
    with open('data/stopwords.txt', mode='r', encoding='utf-8') as f:
        for line in f:
            stopwords.append(line.strip())
    # print(stopwords)

    # 首先，创建一个将城市和州的组合映射到相关评论的字典
    city_reviews = {}
    for item in reviews_list:
        city = item['city']
        city_reviews.setdefault(city, []).append(item['review'])

    # 然后，遍历 cities_dict 中的每个类别和城市列表
    category_words_counts = {}
    category_scores = {}
    for category, city_states in cities_dict.items():
        words_counts = Counter()
        for city_state in city_states:
            # 对于每个城市，累加所有评论中的词频
            city, state = city_state.split('_')
            for review in city_reviews.get(city, []):
                words = review.lower().split()
                words_counts.update(word for word in words if word not in stopwords)
        # 获取最常见的词和它们的计数
        sorted_dict = dict(words_counts.most_common(word_number))
        category_words_counts[category] = sorted_dict
        # print(category_words_counts)

        # avg
        sum_score = 0
        sum_pop = 1
        # 每一类的城市名
        city_names_list = [item.split('_')[0] for item in city_states]
        for item_city in citys_list:
            if item_city['city'] in city_names_list:
                for item_review in reviews_list:
                    if item_city.get('id') == item_review.get('id'):
                        sum_score += int(item_city['population']) * int(item_review['score'])
                sum_pop += int(item_city['population'])
        avg_score = float(sum_score / sum_pop)
        category_scores[category] = avg_score

    category_scores = {int(key): value for key, value in category_scores.items()}
    category_words_counts = {int(key): value for key, value in category_words_counts.items()}
    print(f'category_words_counts:{category_words_counts}')
    # print(type(category_words_counts[0]))
    # print(type(category_scores[1]))
    print(category_scores)
    return category_words_counts, category_scores


if __name__ == '__main__':
    # classify_cities_dict = {2: ['Troy_New York', 'Los Angeles_California'],
    #                         3: ['New York_New York', 'Los Angeles_California']}
    # central_city_dict = {}
    classify_cities_dict, central_city_dict = classify_cities(total_cities=5393, classes=6, k=3, words_number=6)
    print(type(classify_cities_dict[0]))
    print(type(central_city_dict[0]))
    popular(3, classify_cities_dict)
