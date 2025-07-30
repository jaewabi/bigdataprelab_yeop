#!/usr/bin/env python3
"""
FMS Consumer for Real Data Processing
Kafka로부터 FMS 센서 데이터를 수신하고, 초단위로 버퍼에 쌓인 데이터를 HDFS에 저장 및 전처리 실행
전처리는 기준치 벗어난 데이터는 에러 경로에 저장
"""

import json
import os
import tempfile
import subprocess
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Kafka 설정
BROKER = "s1:9092,s2:9092,s3:9092"
#TOPIC = "fms-sensor-data"
TOPIC = "topic9"
GROUP_ID = "fms-data-processor"

# HDFS 네임노드 주소
HDFS_NAMENODE = "s1:9000"

# HDFS 기본 디렉터리
HDFS_RAW_DIR = "/fms/raw-data"
HDFS_ANALYTICS_DIR = "/fms/analytics/data"
HDFS_ERROR_DIR = "/fms/analytics/error"

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FMSDataConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': BROKER,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest'
        })
        self.buffer = []
        self.last_flush_time = time.time()
        self.spark = SparkSession.builder.appName("FMSPreprocessing").getOrCreate()

    def preprocess(self, data_list, y, m, d, hh, mm, ss):
        try:
            df = self.spark.createDataFrame(data_list)

            filtered = df.filter(
                (col("sensor1") >= 0) & (col("sensor1") <= 100) &
                (col("sensor2") >= 0) & (col("sensor2") <= 100) &
                (col("sensor3") >= 0) & (col("sensor3") <= 150) &
                (col("motor1") >= 0) & (col("motor1") <= 2000) &
                (col("motor2") >= 0) & (col("motor2") <= 1500) &
                (col("motor3") >= 0) & (col("motor3") <= 1800) &
                (col("DeviceId") >= 1) & (col("DeviceId") <= 5) &
                (col("isFail").isin(True, False))
            )
            error_df = df.subtract(filtered)

            # 디렉터리 생성
            analytics_dir = f"{HDFS_ANALYTICS_DIR}/{y}/{m}/{d}/{hh}"
            error_dir = f"{HDFS_ERROR_DIR}/{y}/{m}/{d}/{hh}"
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", analytics_dir], check=True)
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", error_dir], check=True)

            processed_path = f"{analytics_dir}/processeddata_{hh}{mm}{ss}.json"
            error_path = f"{error_dir}/processeddata_{hh}{mm}{ss}.json"

            filtered.coalesce(1).write.mode("overwrite").json(f"hdfs://{HDFS_NAMENODE}{processed_path}")
            if error_df.count() > 0:
                error_df.coalesce(1).write.mode("overwrite").json(f"hdfs://{HDFS_NAMENODE}{error_path}")
                logger.info(f"⚠ 에러 데이터 저장: {error_path}")

            logger.info(f"✔ 전처리 정상 데이터 저장: {processed_path}")
        except Exception as e:
            logger.error(f"전처리 오류: {e}")

    def write_buffer_to_hdfs(self):
        if not self.buffer:
            logger.info("버퍼에 데이터 없어 저장 안함")
            return

        now = datetime.now()
        y, m, d, hh, mm, ss = now.strftime("%Y %m %d %H %M %S").split()

        raw_dir = f"{HDFS_RAW_DIR}/{y}/{m}/{d}/{hh}"
        filename = f"sensor-data_{now.strftime('%Y%m%d_%H%M%S')}.json"
        hdfs_path = f"{raw_dir}/{filename}"

        # 디렉터리 생성
        try:
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", raw_dir], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"HDFS 디렉터리 생성 실패: {e.stderr}")

        # 임시파일에 버퍼 저장 후 HDFS 업로드
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            for rec in self.buffer:
                tmp.write(json.dumps(rec) + "\n")
            tmp_path = tmp.name

        try:
            subprocess.run(["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_path], check=True)
            logger.info(f"📁 원본 저장 완료: {hdfs_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"HDFS 저장 실패: {e.stderr}")
        finally:
            os.remove(tmp_path)

        # 버퍼 데이터로 전처리 실행
        self.preprocess(self.buffer, y, m, d, hh, mm, ss)

        self.buffer.clear()
        self.last_flush_time = time.time()

    def process_message(self, msg):
        try:
            raw_value = msg.value().decode('utf-8')
            logger.info(f"[RAW INPUT] {raw_value}")
            data = json.loads(raw_value)

            # 숫자 필드 모두 float로 타입 통일
            for key in ['sensor1', 'sensor2', 'sensor3', 'motor1', 'motor2', 'motor3']:
                if key in data:
                    data[key] = float(data[key])

            # DeviceId는 int로 통일
            if 'DeviceId' in data:
                data['DeviceId'] = int(data['DeviceId'])

            # isFail은 bool 유지 (json.loads에서 자동 처리됨)

            self.buffer.append(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}")

    def run(self):
        logger.info("FMS Consumer 시작...")
        logger.info(f"구독 토픽: {TOPIC}")

        self.consumer.subscribe([TOPIC])

        try:
            while True:
                start_time = time.time()
                # 1초 동안 메시지 쭉 받기
                while time.time() - start_time < 1:
                    msg = self.consumer.poll(0.1)
                    if msg is None:
                        continue
                    elif msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                            continue
                    else:
                        self.process_message(msg)

                # 1초간 받은 메시지 모두 처리(저장+전처리)
                self.write_buffer_to_hdfs()

        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
        finally:
            self.write_buffer_to_hdfs()
            self.consumer.close()
            self.spark.stop()
            logger.info("Consumer 종료")

if __name__ == "__main__":
    consumer = FMSDataConsumer()
    consumer.run()

