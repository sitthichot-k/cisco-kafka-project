import json
import time
import paramiko
from kafka import KafkaConsumer

def execute_cisco_command(ip, command, username='admin', password='cisco'):
    print(f"[*] Connecting to router {ip}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=ip,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=10
        )
        
        shell = ssh.invoke_shell()
        shell.send(f"{command}\n")
        time.sleep(2)
        
        output = shell.recv(65535).decode('utf-8')
        ssh.close()
        return output
    except Exception as e:
        return f"[!] Connection error to {ip}: {str(e)}"

def run_worker():
    consumer = KafkaConsumer(
        'cisco-commands',
        bootstrap_servers=['localhost:7092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='cisco-worker-group'
    )

    print("[*] Worker started. Waiting for Cisco command events...")

    for message in consumer:
        event = message.value
        router_ip = event.get('router_ip')
        command = event.get('command')

        print(f"\n[+] Received Task -> Router: {router_ip} | Command: {command}")
        
        result = execute_cisco_command(ip=router_ip, command=command)
        
        print(f"--- Execution Output ({router_ip}) ---")
        print(result)
        print("--------------------------------------")

if __name__ == '__main__':
    run_worker()
