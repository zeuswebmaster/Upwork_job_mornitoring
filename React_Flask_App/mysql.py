from flask import Flask, jsonify, request
from flask_mysqldb import MySQL
from flask_cors import CORS

app = Flask(__name__)

app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'upworkjob_db'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

mysql = MySQL(app)
CORS(app)

@app.route('/api/data', methods=['GET'])
def get_all_tasks():
    cur = mysql.connection.cursor()
    print(cur)
    cur.execute("SELECT * FROM upworkjob_db.upwork_job")
    rv = cur.fetchall()
    return jsonify(rv)


@app.route('/api/data/<id>', methods=['DELETE'])
def delete_data(id):
    cur = mysql.connection.cursor()
    print(cur)
    response = cur.execute("DELETE FROM upworkjob_db.upwork_job where id = " + id)
    mysql.connection.commit()

    if response > 0:
        result = {'message' : 'record deleted'}
    else:
        result = {'message' : 'no record found'}

    return jsonify({"result": result})
    

@app.route('/api/delete/all_items', methods=['DELETE'])
def delete_all_data():
    cur = mysql.connection.cursor()
    print(cur)
    response = cur.execute("TRUNCATE upworkjob_db.upwork_job")
    mysql.connection.commit()

    if response > 0:
        result = {'message' : 'all record deleted'}
    else:
        result = {'message' : 'there is no any record'}

    return jsonify({"result": result})


if __name__ == '__main__':
    app.run(debug=True)