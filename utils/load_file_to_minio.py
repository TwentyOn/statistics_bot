from datetime import timedelta

from settings import BUCKET_NAME, OUTER_ENDPOINT_URL, MINIO_SECURE

from datetime import timedelta
from io import BytesIO

from settings import (
    ACCESS_KEY,
    BUCKET_NAME,
    ENDPOINT_URL,
    MINIO_SECURE,
    OUTER_ENDPOINT_URL,
    SECRET_KEY,
)
from minio import Minio


class MyStorage:
    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            bucket_name,
            secure: bool = False,
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,  # отключение подключения по HTTPS
        )
        print("Подключение к хранилищу успешно")

    def upload_file(
            self, file_name: str, file_path: str, bucket_name: str = BUCKET_NAME
    ):
        """
        Загрузка файла в S3-хранилище
        :param bucket_name:
        :param file_name:
        :param file_path:
        :return: None
        """
        self.client.fput_object(bucket_name, file_name, file_path)

    def upload_memory_file(
            self, file_name: str, data: BytesIO, length: int, bucket_name: str = BUCKET_NAME
    ):
        self.client.put_object(bucket_name, file_name, data, length)

    def share_file_from_bucket(
            self, file_name, expire=timedelta(seconds=60), bucket_name=BUCKET_NAME
    ):
        """
        Генерирует ссылку на скачивание файла
        :param backet_name:
        :param file_name:
        :param expire:
        :return:
        """
        # return self.client.presigned_get_object(bucket_name, file_name, expire)
        return f"http{'s' if MINIO_SECURE else ''}://{OUTER_ENDPOINT_URL}/minio/{bucket_name}/{file_name}"


storage = MyStorage(ENDPOINT_URL, ACCESS_KEY, SECRET_KEY, BUCKET_NAME)
