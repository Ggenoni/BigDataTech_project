from flask import Flask, render_template, request
from kafka import KafkaProducer, KafkaConsumer
import json
import csv

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/')
def index():
    buttons = ""
    with open('/app/config/cities.csv') as csv_f:
        csv_r = csv.reader(csv_f, delimiter=',')
        for row in csv_r:
            if row[0] != 'localita':
                buttons += f'<input class="city-btn" type="submit" value="{row[0]}" formaction="/data/{row[2]}">'
    return render_template('index.html', buttons=buttons)

@app.route('/data/<int:id_location>', methods=['GET', 'POST'])
def data(id_location):
    if request.method == 'POST':
        producer.send('input-head', {"status": "START", "id_location": id_location})
        # Additional logic to handle data retrieval and display
    return render_template('data.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
