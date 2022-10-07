from email.headerregistry import MessageIDHeader
from ensurepip import bootstrap
import json
from kafka import KafkaConsumer
from kafka import kafakaProducer
from order_backend import ORDER_KAFKA_TOPIC

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = kafakaProducer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

producer = kafakaProducer(
    bootstrap_servers = "localhost:29092"
)

print("Gonna start listening.....")
while True:
    for message in consumer:
        print("Onging transaction....")
        consumed_massage = json.loads(message.value.decode())
        print(consumed_massage)

        user_id = consumed_massage["user_id"]
        total_cost = consumed_message["total_cost"]

        data = {
            "customer_id" : user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }

        print("Suc")
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )