import json
import os
from datetime import datetime
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json,execute_values

load_dotenv()

class PostgresLoader:
    def __init__(self):
        self.db_config={
            "host":os.getenv("POSTGRES_HOST"),
            "port": int(os.getenv("POSTGRES_PORT")),
            "dbname":os.getenv("POSTGRES_DB"),
            "user":os.getenv("POSTGRES_USER"),
            "password":os.getenv("POSTGRES_PASSWORD")
        }
        
    def get_connection(self):
        return psycopg2.connect(**self.db_config)
    
    @staticmethod
    def _to_jsonb(value: str|dict|list|None)->Json|None:
        if value is None:
            return None
        if isinstance(value,str):
            return Json(json.loads(value))
        return Json(value)
    
    # postgres db connection Test
    
    def test_connection(self)->bool:
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version=cur.fetchone()[0]
            print(f"PostgreSQL 연결 성공 : 버전 {version}")
            return True
        except Exception as e:
            print(f"PostgreSQL 연결 실패: {e}")
            return False
        finally:
            if conn:
                conn.close()
                
    # 최신값으로 갱신 - upsert
    def upsert_exhibitions(self,exhibitions:list[dict[str,Any]])->int:
        if not exhibitions:
            print("업데이트할 데이터가 없습니다.")  
            return 0
        sql="""
        INSERT INTO raw_exhibitions(
            exhibition_id,title,
            venue,location,address,
            longitude,latitude,
            start_date,end_date,hours,
            prices_raw,age_limit,
            category,genre,
            day_rank,week_rank,month_rank,rank,
            image_url,detail_url,
            notice,
            is_active,crawled_at,updated_at
        ) VALUES %s
        ON CONFLICT (exhibition_id) DO UPDATE SET
            title=EXCLUDED.title,
            venue=EXCLUDED.venue,
            location=EXCLUDED.location,
            address=EXCLUDED.address,
            start_date=EXCLUDED.start_date,
            end_date=EXCLUDED.end_date,
            hours=EXCLUDED.hours,
            prices_raw=EXCLUDED.prices_raw,
            age_limit=EXCLUDED.age_limit,
            category=EXCLUDED.category,
            genre=EXCLUDED.genre,
            day_rank=EXCLUDED.day_rank,
            week_rank=EXCLUDED.week_rank,
            month_rank=EXCLUDED.month_rank,
            rank=EXCLUDED.rank,
            image_url=EXCLUDED.image_url,
            detail_url=EXCLUDED.detail_url,
            notice=EXCLUDED.notice,
            is_active=EXCLUDED.is_active,
            crawled_at=EXCLUDED.crawled_at,
            updated_at=EXCLUDED.updated_at
        """
        
        now= datetime.now().isoformat()
        
        values=[
            (
                ex["exhibition_id"],
                ex.get("title"),
                ex.get("venue"),
                ex.get("location"),
                ex.get("address"),
                ex.get("latitude"),
                ex.get("longitude"),
                ex.get("start_date"),
                ex.get("end_date"),
                ex.get("hours"),
                self._to_jsonb(ex.get("prices_raw")),
                ex.get("age_limit"),
                ex.get("category"),
                ex.get("genre"),
                ex.get("day_rank"),
                ex.get("week_rank"),
                ex.get("month_rank"),
                ex.get("rank"),
                ex.get("image_url"),
                ex.get("detail_url"),
                ex.get("notice"),
                ex.get("is_active",True),
                ex.get("crawled_at",now),
                now,
            )
            for ex in exhibitions
        ]
        
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                execute_values(cur,sql,values)
            conn.commit()
            print(f"raw_exhibitions 테이블에 {len(exhibitions)}건의 데이터 적재 /업데이트 완료")
            return len(exhibitions)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"데이터 적재 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
                
    # raw_exhibition_prices upsert
    
    def upsert_exhibition_prices(self,prices:list[dict[str,Any]])->int:
        if not prices:
            print("업데이트할 가격 데이터가 없습니다.")
            return 0
        
        sql="""
        INSERT INTO raw_exhibition_prices(
            exhibition_id,
            seat_grade,seat_grade_name,
            price_grade,price_grade_name,
            price_type_code,price_type_name,
            sales_price,origin_price,discount_rate,
            price_start_at,price_end_at,crawled_at
        ) VALUES %s
        ON CONFLICT (exhibition_id,seat_grade,price_grade,price_type_code) DO UPDATE SET
            seat_grade_name=EXCLUDED.seat_grade_name,
            price_grade_name=EXCLUDED.price_grade_name,
            price_type_name=EXCLUDED.price_type_name,
            sales_price=EXCLUDED.sales_price,
            origin_price=EXCLUDED.origin_price,
            discount_rate=EXCLUDED.discount_rate,
            price_start_at=EXCLUDED.price_start_at,
            price_end_at=EXCLUDED.price_end_at,
            crawled_at=EXCLUDED.crawled_at
        """
        now=datetime.now().isoformat()
        
        values=[
            (
                p["exhibition_id"],
                p.get("seat_grade"),
                p.get("seat_grade_name"),
                p.get("price_grade"),
                p.get("price_grade_name"),
                p.get("price_type_code"),
                p.get("price_type_name"),
                p.get("sales_price"),
                p.get("origin_price"),
                p.get("discount_rate"),
                p.get("price_start_at"),
                p.get("price_end_at"),
                now,
            )
            for p in prices
        ]
        
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                execute_values(cur,sql,values)
            conn.commit()
            print(f"raw_exhibition_prices 테이블에 {len(prices)}건의 가격 데이터 적재/업데이트 완료")
            return len(prices)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"가격 데이터 적재 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
            
        
    def upsert_stats(self,stats_list:list[dict[str,Any]])->int:
        if not stats_list:
            print("업데이트 할 통계 데이터가 없습니다.")
            return 0
        
        sql="""
        INSERT INTO raw_exhibition_stats(
            exhibition_id,
            age10_rate,age20_rate,age30_rate,age40_rate,age50_rate,
            male_rate,female_rate,
            stats_raw,
            snapshot_date,crawled_at
        )VALUES %s
        ON CONFLICT (exhibition_id,snapshot_date) DO UPDATE SET
        age10_rate=EXCLUDED.age10_rate,
        age20_rate=EXCLUDED.age20_rate,
        age30_rate=EXCLUDED.age30_rate,
        age40_rate=EXCLUDED.age40_rate,
        age50_rate=EXCLUDED.age50_rate,
        male_rate=EXCLUDED.male_rate,
        female_rate=EXCLUDED.female_rate,
        stats_raw=EXCLUDED.stats_raw,
        crawled_at=EXCLUDED.crawled_at
        """
        
        now=datetime.now().isoformat()
        today=datetime.now().date().isoformat()
        
        values=[
            (
                s["exhibition_id"],
                s.get("age10_rate"),
                s.get("age20_rate"),
                s.get("age30_rate"),
                s.get("age40_rate"),
                s.get("age50_rate"),
                s.get("male_rate"),
                s.get("female_rate"),
                s.get("stats_raw"),
                today,
                now
            )
            for s in stats_list
        ]
        
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                execute_values(cur,sql,values)
            conn.commit()
            print(f"raw_exhibition_stats 테이블에 {len(stats_list)}건의 통계 데이터 적재/업데이트 완료")
            return len(stats_list)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"통계 데이터 적재 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
    def insert_history(self,exhibitions:list[dict[str,Any]])->int:
        if not exhibitions:
            print("적재할 히스토리 snapshot 데이터가 없습니다.")
            return 0
        
        sql="""
        INSERT INTO raw_exhibition_history(
            exhibition_id,
            day_rank,week_rank,month_rank,
            prices_raw,
            snapshot_date,crawled_at
        ) VALUES %s
        ON CONFLICT (exhibition_id,snapshot_date) DO UPDATE SET
            day_rank=EXCLUDED.day_rank,
            week_rank=EXCLUDED.week_rank,
            month_rank=EXCLUDED.month_rank,
            prices_raw=EXCLUDED.prices_raw,
            crawled_at=EXCLUDED.crawled_at
        """
        
        now=datetime.now().isoformat()
        today=datetime.now().date().isoformat()
        
        values=[
            (
                ex["exhibition_id"],
                ex.get("day_rank"),
                ex.get("week_rank"),
                ex.get("month_rank"),
                self._to_jsonb(ex.get("prices_raw")),
                today,
                now
            )
            for ex in exhibitions
        ]
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                execute_values(cur,sql,values)
            conn.commit()
            print(f"raw_exhibition_history 테이블에 {len(exhibitions)}건의 히스토리 snapshot 데이터 적재 완료")
            return len(exhibitions)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"히스토리 snapshot 데이터 적재 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
                
    # 비활성화 처리 is_active=False
    def mark_inactive(self,active_ids:list[str])->int:
        if not active_ids:
            print("비활성화 처리할 exhibition_id가 없습니다.")
            return 0
        sql="""
        UPDATE raw_exhibitions
        SET is_active=FALSE,updated_at=NOW()
        WHERE exhibition_id != ALL(%s)
        AND is_active=TRUE
        """
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                cur.execute(sql,(active_ids,))
            conn.commit()
            print(f"비활성화 처리 완료: {len(active_ids)}건")
            return len(active_ids)
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"비활성화 처리 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
    # 통계 조회
    
    def get_stats(self)->dict[str,Any]:
        stats:dict[str,Any]={}
        conn=None
        try:
            conn=self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_exhibitions")
                stats["total"]=cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM raw_exhibitions WHERE is_active=TRUE")
                stats["active"]=cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_exhibition_history")
                stats["history"]=cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_exhibition_prices")
                stats["prices"]=cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw_exhibition_stats")
                stats["stats_records"]=cur.fetchone()[0]
                
                cur.execute("""
                    SELECT COALESCE(location,'기타') AS location, COUNT(*) AS cnt
                    FROM raw_exhibitions
                    WHERE is_active=TRUE
                    GROUP BY location
                    ORDER BY cnt DESC
                    LIMIT 5
                """)
                
                stats["by_location"]=cur.fetchall()
                
                cur.execute("""
                    SELECT exhibition_id,title,week_rank
                    FROM raw_exhibitions
                    WHERE is_active=TRUE AND week_rank IS NOT NULL
                    ORDER BY week_rank ASC
                    LIMIT 5
                """)
                
                stats["top_weekly"]=cur.fetchall()
                
                cur.execute("""
                    SELECT COUNT(*) FROM raw_exhibitions
                    WHERE is_active=TRUE AND prices_raw IS NOT NULL
                """)
                
                stats["price_collected"]=cur.fetchone()[0]
                
            return stats
        except Exception as e:
            print(f"통계 조회 실패: {e}")
            return {}
        finally:
            if conn:
                conn.close()
                

def load_exhibitions(exhibitions:list[dict[str,Any]])->None:
    print(f"총 {len(exhibitions)}건의 전시 데이터 적재 시작")
    print("\n"+"="*50+"\n")
    
    loader=PostgresLoader()
    
    if not loader.test_connection():
        print("PostgreSQL 연결 실패. 데이터 적재를 중단합니다.")
        return 
    
    loader.upsert_exhibitions(exhibitions)
    
    loader.insert_history(exhibitions)
    
    active_ids=[ex["exhibition_id"] for ex in exhibitions if ex.get("exhibition_id")]
    loader.mark_inactive(active_ids)
    
    stats=loader.get_stats()
    if not stats:
        print("통계 조회 실패")
        return
    
    print("\n"+"="*50+"\n")
    print("DB 적재 완료. 현재 통계: ")
    
    print(f"총 전시 수: {stats.get('total',0)}")
    print(f"활성 전시 수: {stats.get('active',0)}")
    print(f"히스토리 스냅샷 수: {stats.get('history',0)}")
    print(f"가격 데이터 수: {stats.get('prices',0)}")
    print(f"통계 레코드 수: {stats.get('stats_records',0)}")
    
    by_loc=stats.get("by_location",[])
    print("지역별 전시 수:")
    for loc, cnt in by_loc:
        print(f"  {loc}: {cnt}")
        
    top_weekly=stats.get("top_weekly",[])
    print("주간 랭킹 상위 5개 전시:")
    for ex_id,title,rank in top_weekly:
        print(f"  {ex_id} - {title} (주간 랭킹: {rank})")   
    
    print("\n"+"="*50+" 적재 완료 "+"\n")
    
if __name__=="__main__":
    loader=PostgresLoader()
    loader.test_connection()