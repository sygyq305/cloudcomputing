import csv
import pandas as pd
from flask import (Flask, redirect, render_template, request, jsonify, Response,
                   send_from_directory, url_for)

app = Flask(__name__)
# 从 CSV 文件加载评论
reviews_data = pd.read_csv('amazon-reviews.csv')
cities_data = pd.read_csv('us-cities.csv')

@app.route('/')
# def index():
#    print('Request for index page received')
#    return render_template('index.html')
def show_reviews():
    score_path = {1: 'score-1', 2: 'score-2', 3: 'score-3', 4: 'score-4', 5: 'score-5'}

    return render_template('index.html', reviews=reviews_data.to_dict('records'), score_path=score_path)


@app.route('/city_details/<city>')
def city_details(city):
    # 获取城市详情数据
    city_details = cities_data[cities_data['city'] == city].to_dict(orient='records')
    if not city_details:
        return 'City details not found'

    return render_template('city_details.html', city_details=city_details[0])


def fetch_data(city_name = None, include_header = False, exact_match = False):
    with open("'amazon-reviews.csv'") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        row_id = -1
        wanted_data = []
        for row in csvreader:
            row_id += 1
            if row_id == 0 and not include_header:
                continue
            line = []
            col_id = -1
            is_wanted_row = False
            if city_name is None:
                is_wanted_row = True
            for raw_col in row:
                col_id += 1
                col = raw_col.replace('"', '')
                line.append( col )
                if col_id == 0 and city_name is not None:
                    if not exact_match and city_name.lower() in col.lower():
                        is_wanted_row = True
                    elif exact_match and city_name.lower() == col.lower():
                        is_wanted_row = True
            if is_wanted_row:
                if row_id > 0:
                    line.insert(0, "{}".format(row_id))
                else:
                    line.insert(0, "")
                wanted_data.append(line)
    return wanted_data
@app.route('/popular_words10', methods=['GET'])
def popular_words():
    # Get query parameters
    city_name = request.args.get('city', None)
    limit = int(request.args.get('limit', 10))

    # Filter data based on city_name if provided
    if city_name:
        filtered_data = reviews_data[reviews_data['city'] == city_name]
    else:
        filtered_data = reviews_data

    # Count word occurrences in reviews
    word_count = {}
    for review in filtered_data['review']:
        words = review.lower().split()
        unique_words = set(words)  # Ensure each word is counted only once per review
        for word in unique_words:
            if word.isalpha():  # Exclude non-alphabetic characters
                word_count[word] = word_count.get(word, 0) + 1

    # Sort words by popularity in descending order
    sorted_words = sorted(word_count.items(), key=lambda x: x[1], reverse=True)

    # Create response JSON
    response_data = [{'term': term, 'popularity_Q10': popularity} for term, popularity in sorted_words[:limit]]

    return jsonify(response_data)


@app.route('/popular_words11', methods=['GET'])
def popular_words11():
    # Get query parameters
    city_name = request.args.get('city', None)
    limit = int(request.args.get('limit', 10))

    # Filter data based on city_name if provided
    if city_name:
        filtered_data = reviews_data[reviews_data['city'] == city_name]
    else:
        filtered_data = reviews_data

    # Count word occurrences and track the city populations
    word_count = {}
    city_populations = {}
    for _, row in filtered_data.iterrows():
        words = row['review'].lower().split()
        unique_words = set(words)  # Ensure each word is counted only once per review
        for word in unique_words:
            if word.isalpha():  # Exclude non-alphabetic characters
                word_count[word] = word_count.get(word, 0) + 1
                city_populations[word] = city_populations.get(word, {})
                city_populations[word][row['city']] = cities_data.loc[cities_data['city'] == row['city'], 'population'].values[0]

    # Calculate word popularity based on the sum of city populations
    popularity_data = {}
    for word, populations in city_populations.items():
        population_sum = sum(populations.values())
        popularity_data[word] = population_sum

    # Sort words by popularity in descending order
    sorted_words = sorted(popularity_data.items(), key=lambda x: x[1], reverse=True)

    # Create response JSON
    response_data = [{'term': term, 'popularity': int(popularity)} for term, popularity in sorted_words[:limit]]
    print("---------------------------------")
    print(response_data)
    print("---------------------------------")
    return jsonify(response_data)


@app.route('/substitute_words', methods=['POST'])
def substitute_words():
    data = request.get_json()
    print("---------------------------------")
    print(request.content_type)
    print("---------------------------------")
    data = request.get_json()
    original_word = data['word']
    substitute_word = data['substitute']

    # 计算受影响的评论数量并替换单词
    affected_reviews = 0
    for i, review in reviews_data['review'].items():
        if original_word in review:
            new_review = review.replace(original_word, substitute_word)
            reviews_data.at[i, 'review'] = new_review
            affected_reviews += 1
    response_data = {"affected_reviews": affected_reviews,
        "message": "Word substitution successful."}
    # # 返回 JSON 响应
    # return jsonify(response_data)
    print("---------------------------------")
    print(response_data)
    print("---------------------------------")
    response = Response(response=jsonify(response_data).data, status=200, content_type='application/json')
    print(response.content_type)
    return jsonify(response_data)


if __name__ == '__main__':
   app.run(debug=True)
