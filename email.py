import json
from kafka import KafkaConsumer
from transaction import ORDER_CONFIRMED_KAFKA_TOPIC

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    boostrap_servers = "localhost:29092"
)


emails_sent_so_far = set()
print("Gonna start listening")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        print(f"Sending email to {customer_email}")
        emails_sent_so_far.add(customer_email)
        print(f"So lfar emails sent to {len(emails_sent_so_far)} unique emails")