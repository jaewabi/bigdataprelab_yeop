import os
import json
import subprocess
import mysql.connector
from datetime import datetime
import time

db_config = {
    'host': 'm1',
    'user': 'root',
    'password': 'root',
    'database': 'test_db',
}

def hdfs_list_files(dir_path):
    cmd = ['hadoop', 'fs', '-ls', dir_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"hadoop fs -ls 실패: {result.stderr}")
    files = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 8:
            filepath = parts[-1]
            if filepath.endswith('.json'):
                files.append(filepath)
    return files

def hdfs_rename_done(hdfs_path):
    done_path = hdfs_path + '.done'
    cmd = ['hadoop', 'fs', '-mv', hdfs_path, done_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"hadoop fs -mv 실패: {result.stderr}")

def parse_datetime(dt_str):
    try:
        dt_fixed = dt_str[:19]
        return datetime.strptime(dt_fixed, '%Y-%m-%dT%H:%M:%S')
    except Exception:
        return None

def insert_json_to_mysql(hdfs_path):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    cmd = ['hadoop', 'fs', '-cat', hdfs_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"hadoop fs -cat 실패: {result.stderr}")

    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        data = json.loads(line)

        collected_at = parse_datetime(data.get('collected_at', ''))
        time_val = parse_datetime(data.get('time', ''))

        sql = """
        INSERT INTO device_data
        (device_id, collected_at, is_fail, motor1, motor2, motor3, sensor1, sensor2, sensor3, time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            data.get('DeviceId'),
            collected_at,
            1 if data.get('isFail') else 0,
            data.get('motor1'),
            data.get('motor2'),
            data.get('motor3'),
            data.get('sensor1'),
            data.get('sensor2'),
            data.get('sensor3'),
            time_val
        ))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    while True:
        now = datetime.now()
        yyyy = now.strftime('%Y')
        mm = now.strftime('%m')
        dd = now.strftime('%d')
        hh24 = now.strftime('%H')

        any_file_found = False

        for device_id in range(1, 6):
            dir_path = f'/fms/analytics/data/{yyyy}/{mm}/{dd}/{hh24}/*/part*.json'
            try:
                files = hdfs_list_files(dir_path)
                if not files:
                    print(f"DeviceID {device_id} 경로에 파일 없음: {dir_path}")
                    continue

                any_file_found = True

                for file_path in files:
                    try:
                        insert_json_to_mysql(file_path)
                        print(f"MySQL 삽입 완료: {file_path}")

                        hdfs_rename_done(file_path)
                        print(f"HDFS 파일명 변경 완료: {file_path} -> {file_path}.done")

                    except Exception as e_file:
                        print(f"DeviceID {device_id} 파일 {file_path} 처리 중 오류:", e_file)

            except Exception as e_dir:
                print(f"DeviceID {device_id} 경로 {dir_path} 처리 중 오류:", e_dir)

        if not any_file_found:
            time.sleep(10)

if __name__ == '__main__':
    main()

