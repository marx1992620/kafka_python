#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import pymysql.cursors
from datetime import date, datetime, timedelta
import ast

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)


if __name__ == '__main__':
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': '10.120.26.15:9092',       # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': '11',                     # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'earliest',             # Offset從最前面開始 latest從最新的開始
        'enable.auto.commit': True,                 # 每次consume資料不再重抓
        'session.timeout.ms': 6000,                  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb                         # 設定接收error訊息的callback函數
    }

    # Connect to the database
    connection = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='zxc2520',
                                 db='kafka_consume',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    # Connect to the database 連接sql另一種寫法
    # config ={
    #     'host': '127.0.0.1',
    #     'port': 3306,
    #     'user': 'root',
    #     'password': 'zxc2520',
    #     'db': 'kafka_consume',
    #     'charset': 'utf8',
    #     'cursorclass': pymysql.cursors.DictCursor,
    # }
    # connection = pymysql.connect(config)

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = "par12345"

    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName]) # on_assign=my_assign為從offset=0開始抓
    # 步驟5. 持續的拉取Kafka有進來的訊息
    count = 0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # 這裡進行商業邏輯與訊息處理
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 獲取時間
                    time = str(datetime.now())[:10].replace("-","")
                    # 取出msgKey與msgValue並轉utf8
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    # msg_dict = ast.literal_eval(msgValue) 可將資料轉字典型態
                    # 秀出metadata與msgKey & msgValue訊息
                    count += 1
                    print('{}-{}-{} : ({} , {})'.format(topic, partition, offset, msgKey, msgValue))
                # 執行sql語句
                    try:
                        with connection.cursor() as cursor:
                            # 執行sql語句，插入資料
                            print(msgKey, msgValue, time)

                            cursor.execute("""INSERT INTO test1 VALUE ('{}','{}','{}')""".format(msgKey, msgValue, time))

                            # sql = """INSERT INTO test1 (key, value, time) VALUES (%s, %s, %s)"""
                            # cursor.execute(sql, ( msgKey, msgValue, time))
                        # 沒有設置自動默認提交，需主動提交，以保存所執行的語句
                        connection.commit()
                    except Exception as e:
                        print(e)

    except Exception as e:
        sys.stderr.write(e)
        print(e)
    finally:
        # 步驟6.關掉Consumer實例的連線
        connection.close()
        consumer.close()
