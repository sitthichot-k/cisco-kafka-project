import json
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=['localhost:7092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/send-command', methods=['POST'])
def send_command():
    data = request.json
    router_ip = data.get('router_ip')
    command = data.get('command', 'show ip interface brief')

    if not router_ip:
        return jsonify({'error': 'router_ip is required'}), 400

    payload = {
        'router_ip': router_ip,
        'command': command
    }

    producer.send('cisco-commands', payload)
    producer.flush()

    return jsonify({
        'status': 'queued',
        'message': f"Command '{command}' queued for router {router_ip}"
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000)
