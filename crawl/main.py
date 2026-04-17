import argparse

from crawler_api import Exhibition,InterparkCrawler
from load.load_to_postgres import PostgresLoader,load_exhibitions

def run_pipeline(pages:int=10)->None:
    print("\n"+"="*50)
    print("인터파크 전시 파이프라인 시작")
    print("="*50)
    
    crawler=InterparkCrawler()
    loader=PostgresLoader()
    
    exhibitions,all_price_rows=crawler.crawl_all(max_pages=pages)
    
    if not exhibitions:
        print("수집된 전시 없음 -파이프라인 중단")
        return
    
    print("DB 적재")
    if not loader.test_connection():
        print("DB 연결 실패 ")
        return
    
    ex_dicts=[ex.to_dict() for ex in exhibitions]
    
    loader.upsert_exhibitions(ex_dicts)
    
    if all_price_rows:
        loader.upsert_exhibition_prices(all_price_rows)
    
    loader.insert_history(ex_dicts)
    
    active_ids=[
        ex["exhibition_id"] for ex in ex_dicts if ex.get("exhibition_id")
    ]
    loader.mark_inactive(active_ids)
    
    stats=loader.get_stats()
    if not stats:
        print("통계 데이터 없음")
        return
    
    print("\n"+"="*50)
    print("통계 데이터")
    print("="*50)
    
    print(f"총 전시 수: {stats.get('total_exhibitions', '0')}")
    print(f"총 가격 데이터 수: {stats.get('total_prices', '0')}")
    print(f"히스토리:{stats.get('history',0)}")
    
    by_loc=stats.get("by_location",[])
    if by_loc:
        print(f"{by_loc}")
        print(f"\n 지역별 분포 (상위 5위)")
        for loc, cnt in by_loc:
            print(f"{loc}:{cnt}개")
    
    top_weekly=stats.get("top_weekly",[])
    if top_weekly:
        print(f"\n 주간 랭킹 상위 5위")
        for ex_id, title,rank in top_weekly:
            print(f" - {title} (ID: {ex_id}, 주간 랭킹: {rank})")
    
    print("\n 파이프라인 완료!\n")


def run_test() -> None:
    print("\n 테스트모드 ")
    
    loader=PostgresLoader()
    if not loader.test_connection():
        print("DB 연결 실패")
        return
    
    crawler=InterparkCrawler()
    links= crawler.get_exhibition_list(max_pages=1)
    print(f"수집된 전시 링크: {len(links)}개")
    
    if not links:
        print("수집된 링크 없음")
        return
    
    print("\n 3개 링크 상세 크롤링")
    for link in links[:3]:
        print(
            f"{link['title'][:30]}|"
            f"{link['start_date']}~{link['end_date']}|"
            f"주간{link.get('week_rank')}위"
        )
    print("\n 더 상세한 크롤링 1건만\n")
    ex,price_rows=crawler.get_exhibition_detail(links[0])
    if ex:
        for label,val in [
            ("제목",ex.title),
            ("장소",ex.venue),
            ("위치",ex.location),
            ("시작시간",ex.start_date),
            ("종료 시간",ex.end_date),
            ("주간 랭킹",ex.week_rank),
            ("일간순위",ex.day_rank),
            ("월간순위",ex.month_rank),
            ("가격 데이터 수",len(price_rows)),
            ("연령",ex.age_limit),
            ("관람시간",ex.hours),
            ("공지", (ex.notice or "")[:30]),
            ("카테고리",ex.category),
            ("장르",ex.genre),
        ]:
            print(f"{label}: {val}")
    

# 가격 테스트 추가 
def run_price_test(place_code:str)->None:
    print(f"\n 가격 테스트 -> {place_code}")
    
    crawler=InterparkCrawler()
    prices_row_str,price_rows=crawler.get_price(place_code)
    print(f"{prices_row_str}")
    
    print(f"prices_row 원문:{prices_row_str}")
    print(f"파싱된 가격 데이터:{price_rows}")
    print("`가격 테스트 완료!`\n")
    
# summary 테스트 추가

def run_summary_test(goods_code:str)->None:
    print(f"\n Summary test -> {goods_code}")
    
    crawler=InterparkCrawler()
    result=crawler.get_exhibition_summary(goods_code)

    if result:
        for k, v in result.items():
            print(f"{k}: {v}")
    else:
        print("Summary 데이터 없음")
        
# place 테스트 추가

def run_place_test(place_code:str)->None:
    print(f"\n 장소테스트 ->{place_code}")
    crawler=InterparkCrawler()
    result=crawler.get_place(place_code)
    if result:
        for k, v in result.items():
            print(f"{k}:{v}")
    else:
        print("장소 데이터 없음")



# main

def main():
    parser= argparse.ArgumentParser(description="인터파크 전시 크롤링 및 DB 적재")
    parser.add_argument("--mode",choices=["test","crawl","price-test","summary-test","place-test"],default="test", help="실행 모드")
    parser.add_argument("--pages",type=int,default=10,help="크롤링 페이지 수 ")
    parser.add_argument("--goods-code",type=str,default="26002980",help="가격/summary 테스트용 상품 코드")
    parser.add_argument("--place-code",type=str,default="19001136",help="장소 위도 경도 ")
    args=parser.parse_args()
    print(f"실행 모드: {args}")
    if args.mode=="crawl":
        run_pipeline(pages=args.pages)
        
    elif args.mode=="test":
        run_test()
    elif args.mode=="price-test":
        run_price_test(args.goods_code)
    elif args.mode=="summary-test":
        run_summary_test(args.goods_code)
    elif args.mode=="place-test":
        run_place_test(args.place_code)

if __name__=="__main__":
    main()