from flask import Flask
app = Flask(__name__)


@app.route("/")
def hello():
    return "<H1>Hello, World!</H1>"


@app.route("/user/<name>")
def user(name):
    return "<H1>Hello, {}!".format(name)


@app.route("/user/")
def user_list():
    return "<H1>Hello, Users</H1>"


if __name__ == "__main__":
    app.run()
