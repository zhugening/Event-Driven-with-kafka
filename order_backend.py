from ensurepip import bootstrap
import json
import time

# !package --> kafka-python
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i ,
        "user_id": f"tom_{i}",
        "total_cost": i *2 ,
        "items" : "burger, sanwish"
    }
producer.send(
    ORDER_KAFKA_TOPIC,
    json.dumps(data).encode("utf-8")
)


print(f"Done sending......{i}")
time.sleep(10)