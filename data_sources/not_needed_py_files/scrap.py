from flask import Flask

app = Flask(__name__)

# routes are end points like urls

# create the first route
@app.route("/")
def index():
    return "<h3> Hello world </h3>"

if __name__ == "__main__":
    app.run(debug = True)