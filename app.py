import os
import pandas as pd
from flask import (Flask, redirect, render_template, request,
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


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()
