"""
範例 Airflow DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


# 預設參數
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
# 使用 with DAG 語法
with DAG(
    dag_id='example_first_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='0 * * * *',  # 每小時執行
    start_date=datetime(2024, 1, 1),  # 從2024年1月1日開始生效
    catchup=False,  # 不執行歷史任務
    tags=['example'],
) as dag:

    def hello_world():
        """簡單的 Python function"""
        print("Hello from Airflow!")

    # 起始任務
    start_task = PythonOperator(
        task_id='start',
        python_callable=hello_world,
    )

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable='echo "Hello 1 from Airflow! Success"',
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable='echo "Hello 2 from Airflow! Success"',
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable='echo "Hello 3 from Airflow! Success"',
    )

    # 結束任務
    end_task = BashOperator(
        task_id='end',
        bash_command='echo "Hello from Airflow! Success"',
    )

    # 設定依賴關係：start -> end
    start_task >>  [task_1,task_2,task_3]  >> end_task
