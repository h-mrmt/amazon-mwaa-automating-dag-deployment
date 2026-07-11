from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import boto3

S3_BUCKET = "crypto-vol-data"
S3_KEY_PREFIX = "deribit/instruments/btc"

def _get_current_utc_timestamp_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

class TimestampedHttpOperator(HttpOperator):
    def pre_execute(self, context):
        self.call_ts_ms = _get_current_utc_timestamp_ms()
        super().pre_execute(context)

    def post_execute(self, context, result=None):
        context["ti"].xcom_push(key="call_ts_ms", value=self.call_ts_ms)
        return super().post_execute(context, result)


def _save_to_s3(ti, **context):
    payload = ti.xcom_pull(task_ids="get_btc_instruments")
    call_ts_ms = ti.xcom_pull(task_ids="get_btc_instruments", key="call_ts_ms")
    key = f"{S3_KEY_PREFIX}/{call_ts_ms}.json"
    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )


with DAG(
    "deribit_data_loader",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
):
    get_instruments = TimestampedHttpOperator(
        task_id="get_btc_instruments",
        http_conn_id="deribit_api",
        endpoint="/api/v2/public/get_instruments",
        method="GET",
        data={"currency": "BTC"},
        #response_filter=lambda r: r.text,
    )

    save_to_s3 = PythonOperator(
        task_id="save_to_s3",
        python_callable=_save_to_s3,
    )

    get_instruments >> save_to_s3
