from flask import Flask, jsonify, request
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

app = Flask(__name__)


def train_model():
    iris_df = datasets.load_iris()
    data = iris_df.data
    target = iris_df.target
    target_names = iris_df.target_names
    train_data, test_data, train_target, test_target = train_test_split(data, target, test_size=0.3)
    dt = DecisionTreeClassifier().fit(train_data, train_target)
    acc = accuracy_score(test_target, dt.predict(test_data))
    return dt, acc, target_names


model, accuracy, names = train_model()


@app.route('/predict', methods=['POST'])
def predict():
    posted_data = request.get_json()
    sepal_length = posted_data['sepal_length']
    sepal_width = posted_data['sepal_width']
    petal_length = posted_data['petal_length']
    petal_width = posted_data['petal_width']
    prediction = model.predict([[sepal_length, sepal_width, petal_length, petal_width]])[0]
    return jsonify({'class': names[prediction]})


@app.route('/model')
def get_model():
    return jsonify({'name': 'Decision Tree Classifier',
                    'accuracy': accuracy})


if __name__ == '__main__':
    app.run(host='0.0.0.0')
