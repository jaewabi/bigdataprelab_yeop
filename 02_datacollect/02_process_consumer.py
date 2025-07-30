#!/usr/bin/env python3
"""
FMS Consumer for Real Data Processing
Kafka로부터 FMS 센서 데이터를 수신하고, 수신 즉시 HDFS에 저장 (디렉토리는 yyyy/mm/dd/hh 기준)
"""
import json
import os
import tempfile
import subprocess
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import logging

# Kafka 설정
BROKER = "s1:9092,s2:9092,s3:9092"
TOPIC = "topic9"
GROUP_ID = "fms-data-processor"

# HDFS 루트 디렉터리
HDFS_ROOT = "/fms/raw-data"

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

    def write_buffer_to_hdfs(self):
        """버퍼 내용을 HDFS에 저장 (디렉토리 구조: yyyy/mm/dd/hh)"""
        if not self.buffer:
            logger.info("버퍼 비어있음, 저장 건너뜀")
            return

        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d/%H")
        filename_ts = now.strftime("%Y%m%d_%H%M%S")
        hdfs_dir = os.path.join(HDFS_ROOT, date_path)
        hdfs_path = os.path.join(hdfs_dir, f"sensor-data_{filename_ts}.json")

        try:
            # 디렉토리 생성
            subprocess.run(
                ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # 임시 파일 생성 및 기록
            with tempfile.NamedTemporaryFile('w', delete=False) as tmp:
                for record in self.buffer:
                    tmp.write(json.dumps(record) + '\n')
                tmp_path = tmp.name

            # HDFS 업로드
            subprocess.run(
                ["hdfs", "dfs", "-put", tmp_path, hdfs_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info(f"📁 HDFS 저장 완료: {hdfs_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"HDFS 업로드 실패: {e.stderr.strip()}")
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            self.buffer.clear()

    def process_message(self, msg):
        """메시지를 파싱하여 버퍼에 저장"""
        try:
            raw_value = msg.value().decode('utf-8')
            logger.info(f"[RAW INPUT] {raw_value}")

            data = json.loads(raw_value)
            self.buffer.append(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}")

    def run(self):
        """Consumer 실행"""
        logger.info("FMS Data Consumer 시작...")
        logger.info(f"구독 토픽: {TOPIC}")

        self.consumer.subscribe([TOPIC])

        try:
            while True:
                msg = self.consumer.poll(1.0)

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
                    self.write_buffer_to_hdfs()  # 수신 즉시 저장

        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
        finally:
            self.write_buffer_to_hdfs()
            self.consumer.close()
            logger.info("Consumer 종료")

if __name__ == "__main__":
    consumer = FMSDataConsumer()
    consumer.run()

