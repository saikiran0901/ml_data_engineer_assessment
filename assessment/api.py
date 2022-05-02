from flask import Flask

app = Flask(__name__)

from run import get_max_min_avg_age,get_city_with_most_people,get_top_5_common_interest
results = {}

@app.route('/get_answers', methods=['POST'])
def get_answers_from_database():
    try:
        results['min_max_avg_age']: get_max_min_avg_age()   
        results['city_with_most_people']: get_city_with_most_people()
        results['top_5_common_interests']: get_top_5_common_interest()
        return results,"Ok"
    except Exception as e:
        return f"{e}"    


if __name__ == "__main__":
    app.run(host='127.0.0.1',port=8080,debug=True)  