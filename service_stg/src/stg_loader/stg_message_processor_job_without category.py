import time
import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int = 100,
                 logger: Logger = None
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("No more messages to process.")
                break
            try:
                self._stg_repository.order_events_insert(
                    object_id=msg.get('object_id'),
                    object_type=msg.get('object_type'),
                    sent_dttm=msg.get('sent_dttm'),
                    payload=json.dumps(msg)
                )
                self._logger.info(f"Saved message to DB: {msg}")

            except Exception as e:
                self._logger.error(f"Failed to save message: {msg}, error: {e}")
            try:
                payload = msg.get('payload', {})
                user_id = payload.get('user', {}).get('id')
                user_info = None
                if user_id is not None:
                    user_info = self._redis.get_user_info(user_id)
                    self._logger.info(f"Fetched user info from Redis: {user_info}")
                else:
                    self._logger.warning("user_id not found in message payload.")

                restaurant_id = payload.get('restaurant', {}).get('id')
                restaurant_info = None
                if restaurant_id is not None:
                    restaurant_info = self._redis.get_restaurant_info(restaurant_id)
                    self._logger.info(f"Fetched restaurant info from Redis: {restaurant_info}")
                else:
                    self._logger.warning("restaurant_id not found in message payload.")

                # Формируем выходное сообщение в нужном формате
                # Извлекаем данные из payload исходного сообщения
                payload = msg.get('payload', {})
                # Получаем имя пользователя и ресторана из Redis
                user_name = user_info.get('name') if user_info else None
                restaurant_name = restaurant_info.get('name') if restaurant_info else None

                # Формируем список продуктов
                products = []
                for item in payload.get('order_items', []):
                    product = {
                        'id': item.get('id'),
                        'price': item.get('price'),
                        'quantity': item.get('quantity'),
                        'name': item.get('name'),
                        'category': item.get('category') if 'category' in item else None
                    }
                    products.append(product)

                # Определяем финальный статус заказа
                status = payload.get('final_status') or None

                output_msg = {
                    'object_id': msg.get('object_id'),
                    'object_type': msg.get('object_type'),
                    'payload': {
                        'id': msg.get('object_id'),
                        'date': payload.get('date'),
                        'cost': payload.get('cost'),
                        'payment': payload.get('payment'),
                        'status': status,
                        'restaurant': {
                            'id': payload.get('restaurant', {}).get('id'),
                            'name': restaurant_name
                        },
                        'user': {
                            'id': payload.get('user', {}).get('id'),
                            'name': user_name
                        },
                        'products': products
                    }
                }
                # Отправляем выходное сообщение в producer
                self._producer.produce(output_msg)
                self._logger.info(f"Sent output message to producer: {output_msg}")
            except Exception as e:
                self._logger.error(f"Failed to save message: {msg}, error: {e}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
        print('all done')
