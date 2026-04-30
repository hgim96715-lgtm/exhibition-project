import logging
import subprocess
import sys
import pendulum
from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag, task

CRAWL_PATH="/opt/airflow/crawl"
LOAD_PATH="/opt/airflow/load"
DBT_PATH="/opt/airflow/dbt/dbt_exhibition"


sys.path.insert(0,CRAWL_PATH)
sys.path.insert(0,LOAD_PATH)

log=logging.getLogger(__name__)

default_args={
    "owner":"exhibition",
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "email_on_failure":False,
}

@dag(
    dag_id="exhibition_pipeline",
    description="인터파크 전시 API → DB 적재 → dbt 변환",
    default_args=default_args,
    start_date=pendulum.datetime(2026,4,30,tz="Asia/Seoul"),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["exhibition","interpark"],
)
def exhibition_pipeline():
    
    @task
    def crawl():
        from crawler_api import InterparkCrawler
        log.info("크롤링 시작")
        crawler=InterparkCrawler(delay=0.4)
        exhibitions,price_rows,stats=crawler.crawl_all(max_pages=10)
        log.info( 
            f"""크롤링 완료 | 전시 : {len(exhibitions)}건 | 
            가격 : {len(price_rows)}건 | 통계 : {len(stats)}건"""
        )
        
        if not exhibitions:
            raise ValueError("수집된 전시 데이터 없습니다. 크롤링 실패.")
        
        return {
            "exhibitions":[ex.to_dict() for ex in exhibitions],
            "price_rows":price_rows,
            "stats":stats
        }
        
    @task
    def load(crawl_result:dict):
        from load_to_postgres import PostgresLoader
        
        if not crawl_result:
            raise ValueError("크롤링 결과를 가져오지 못했습니다.")
        
        exhibitions=crawl_result["exhibitions"]
        price_rows=crawl_result["price_rows"]
        stats=crawl_result["stats"]
        
        loader=PostgresLoader()
        
        # main.py의 run_pipeline과 동일한 로직을 여기에 구현
        if not loader.test_connection():
            raise ConnectionError("DB 연결 실패")
        
        n_ex=loader.upsert_exhibitions(exhibitions)
        n_price=loader.upsert_exhibition_prices(price_rows) if price_rows else 0
        n_stats=loader.upsert_stats(stats) if stats else 0
        n_hist=loader.insert_history(exhibitions)
        
        active_ids=[ex["exhibition_id"] for ex in exhibitions ]
        n_inactive=loader.mark_inactive(active_ids)
        
        log.info(
            f"""
            DB 적재 완료 | 전시 : {n_ex}건 | 가격 : {n_price}건 | 통계 : {n_stats}건 | 히스토리 : {n_hist}건 | 비활성 처리 : {n_inactive}건
            """
        )
        return {
            "exhibitions":n_ex,
            "prices":n_price,
            "stats":n_stats,
            "history":n_hist,
            "inactive":n_inactive
        }
        
    @task
    def dbt_run():
        dbt_project_dir=Path(DBT_PATH)
        
        if not dbt_project_dir.exists():
            raise FileNotFoundError(f"DBT 프로젝트 디렉토리 없음:{dbt_project_dir}")
        
        cmd=[
            "dbt",
            "run",
            "--project-dir",str(dbt_project_dir),
            "--profiles-dir",str(dbt_project_dir),
            "--target","dev"
        ]
        
        log.info(f"dbt run 시작 :{' '.join(cmd)}")
        
        result= subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
        )
        
        if result.stdout:
            log.info(f"dbt run 출력 : {result.stdout}")
        
        if result.stderr:
            log.warning(f"dbt run 에러 : {result.stderr}")
        
        if result.returncode != 0:
            raise RuntimeError(f"dbt run 실패 : {result.stderr}")
        
        log.info("dbt run 완료")
        
    @task
    def summary(load_result:dict):
        log.info(f"""
                 파이프라인 완료! |
                 전시 : {load_result.get('exhibitions',0)}건 |
                 가격 : {load_result.get('prices',0)}건 |
                 통계 : {load_result.get('stats',0)}건 |
                 히스토리 : {load_result.get('history',0)}건 |
                 비활성 처리 : {load_result.get('inactive',0)}건
                 """)
    
    crawl_result=crawl()
    load_result=load(crawl_result)
    dbt_task=dbt_run()
    summary_task=summary(load_result)
    
    load_result>>dbt_task>>summary_task
    
exhibition_pipeline()
        

