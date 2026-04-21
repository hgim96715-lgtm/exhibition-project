import json
import re
import time
from dataclasses import asdict,dataclass,field
from datetime import datetime
import requests

@dataclass
class Exhibition:
    exhibition_id:str
    title:str
    venue:str=None
    location:str=None
    address:str=None
    latitude:float=None
    longitude:float=None
    start_date:str=None
    end_date:str=None
    hours:str=None
    prices_raw:str=None
    age_limit:str=None
    category:str=None
    genre:str=None
    day_rank:int=None
    week_rank:int=None
    month_rank:int=None
    rank:str=None
    image_url:str=None
    detail_url:str=None
    notice:str=None
    is_active:bool=True
    crawled_at:str=None
    
    def __post_init__(self):
        if self.crawled_at is None:
            self.crawled_at=datetime.now().isoformat()
            
    def to_dict(self)->dict:
        return asdict(self)
    
class InterparkCrawler:
    
    BASE_URL        = "https://tickets.interpark.com"
    LIST_API_URL    = "https://tickets.interpark.com/contents/api/goods/genre"
    SUMMARY_API_URL = "https://api-ticketfront.interpark.com/v1/goods/{goods_code}/summary"
    PRICE_API_URL   = "https://api-ticketfront.interpark.com/v1/goods/{goods_code}/prices/group"
    # BEST_API_URL 에  "originPrice", "discountRate" 필드를 구할 수 있다.
    BEST_API_URL    = "https://api-ticketfront.interpark.com/v1/goods/{goods_code}/bestprices/group"
    PLACE_API_URL ="https://api-ticketfront.interpark.com/v1/Place/{place_code}"
    STATS_API_URL   = "https://api-ticketfront.interpark.com/v1/statistics/booking"
    
       
    LOCATION_MAP = {
        "서울": ["서울", "종로", "중구", "용산", "강남", "서초", "송파", "마포", "영등포", "이태원"],
        "경기": ["경기", "수원", "성남", "용인", "고양", "부천", "안양", "화성", "일산"],
        "인천": ["인천"],
        "부산": ["부산"],
        "대구": ["대구"],
        "대전": ["대전"],
        "광주": ["광주"],
        "울산": ["울산"],
        "세종": ["세종"],
        "강원": ["강원", "춘천", "강릉"],
        "충북": ["충북", "청주"],
        "충남": ["충남", "천안","태안"],
        "전북": ["전북", "전주", "남원"],
        "전남": ["전남", "여수", "순천"],
        "경북": ["경북", "포항", "경주"],
        "경남": ["경남", "창원", "김해"],
        "제주": ["제주"],
    }
    
    def __init__(self,delay:float=0.3):
        self.delay=delay
        self.session=requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/26.5 Safari/605.1.15"
            ),
            "Accept":  "application/json, text/plain, */*",
            "Referer": "https://tickets.interpark.com/",
            "Origin":  "https://tickets.interpark.com",
        })

    def _format_date(self,raw:str)->str|None:
        if raw and len(raw)==8 and raw.isdigit():
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        return None
    def _format_datetime(self,raw:str)->str|None:
        if raw and len(raw)==12 and raw.isdigit():
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]} {raw[8:10]}:{raw[10:]}"
        return None
    
    # def _normalize_region(self,text:str)->str|None:
    #     if not text:
    #         return None
    #     normalized=re.sub(r"(시|도|광역시)$", "", text.strip())
    #     return normalized
    
    
    def _extract_location(self,text:str)->str|None:
        if not text:
            return None
        for loc,keywords in self.LOCATION_MAP.items():
            if any(kw in text for kw in keywords):
                return loc
        return None
    
    def _safe_int(self,value)->int|None:
        try:
            return int(value) if value else None
        except (ValueError, TypeError):
            return None
        
    def _safe_float(self,value)->float|None:
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None
        
    # displayTemplate 
    
    def _parse_display_template(self,template:str)->dict:
        if not template:
            return {}
        
        clean=re.sub(r"<br\s*/?>", "\n", template)
        clean=re.sub(r"<[^>]+>", "", clean)
        lines=[l.strip() for l in clean.splitlines() if l.strip()]
        
        address=None
        hours=None
        notice_lines=[]
        skip_headers={"[전시개요]","[티켓사용 안내]"}
        
        for line in lines:
            if line in skip_headers:
                continue
            
            # m = re.match(r"전시장소\s*[:：]\s*(.+)", line)
            # if m:
            #     address=m.group(1).strip()
            #     continue
            
            # start_date, end_date가 있으니 굳이 기간에서 뽑을 필요는 없을 것 같아 주석처리
            # m = re.match(r"전시기간\s*[:：]\s*(.+)", line)
            # if m:
            #     period=m.group(1).strip()
            #     continue
            
            m = re.match(r"관람시간\s*[:：]?\s*(.+)", line)
            if m:
                hours=m.group(1).strip()
                continue
            
            notice_lines.append(line)
        
        return {
            "hours": hours,
            "notice": "\n".join(notice_lines) if notice_lines else None
        }
        
        # 목록 API
        
    def get_exhibition_list(self,max_pages:int=10)->list[dict]:
        print("목록 API 호출 .. 수집 시작")
        exhibitions=[]
        page=1
        
        while page<=max_pages:
            print(f"페이지 {page}/{max_pages} 수집중...")
            params={
                "genre":"EXHIBIT",
                "page":page,
                "pageSize":50,
                "sort":"WEEKLY_RANKING"
            }
            try:
                resp=self.session.get(self.LIST_API_URL,params=params,timeout=10)
                resp.raise_for_status()
                data=resp.json()
                
                if isinstance(data,list):
                    items=data
                elif isinstance(data,dict):
                    items=data.get("list") or data.get("data",{}).get("list",[])
                else:
                    items=[]
                    
                if not items:
                    print("더 이상 데이터가 없습니다. 수집 종료.")
                    break
                
                print("목록 API 응답 수:", len(items))
                
                for item in items:
                    goods_code=item.get("goodsCode")
                    if not goods_code:
                        continue
                    
                    image_url=item.get("imageUrl","") or item.get("posterImageUrl")
                    if image_url.startswith("//"):
                        image_url="https:"+image_url
                    
                    week_rank=self._safe_int(item.get("weekRank"))
                    # location=self._normalize_region(region_raw) if region_raw else None
                    # region_raw=item.get("regionName","")
                    exhibitions.append({
                        "exhibition_id": str(goods_code),
                        "title":item.get("goodsName",""),
                        "venue":item.get("venueName",""),
                        "location": item.get("regionName") or None, # 이건 place_code로 장소 API에서 다시 뽑아야할듯
                        "image_url": image_url,
                        "detail_url": f"{self.BASE_URL}/contents/{goods_code}",
                        "start_date":self._format_date(item.get("startDate","")),
                        "end_date":self._format_date(item.get("endDate","")),
                        "rank": f"주간 {week_rank}위" if week_rank else None,
                        "age_limit": item.get("ageLimit",""),
                        "category":item.get("subCategoryName",""),
                        
                    })
                page+=1
                time.sleep(self.delay)
            except requests.RequestException as e:
                print(f"API 요청 실패: {e} - page: {page}")
                break
            except Exception as e:
                print(f"데이터 처리 중 오류: {e} - page: {page}")
                continue
        
         # 중복 제거        
        seen,unique=set(),[]
        for ex in exhibitions:
            if ex["exhibition_id"] not in seen:
                seen.add(ex["exhibition_id"])
                unique.append(ex)
        print(f"총 {len(unique)}개의 고유한 전시회 데이터 수집 완료.")
        return unique

    # summary API
    
    def get_exhibition_summary(self,goods_code:str)->dict |None:
        url=self.SUMMARY_API_URL.format(goods_code=goods_code)
        params={
            "goodsCode":goods_code,
            "passCode":"",
            "priceGrade":"",
            "seatGrade":"",
            "ts":int(time.time()*1000),
        }
        
        try:
            resp=self.session.get(url,params=params,timeout=10)
            resp.raise_for_status()
            raw=resp.json()
            
            if raw.get("common",{}).get("message")!="success":
                return None
            
            d=raw.get("data",{})
            if not d:
                return None
            
            result:dict={
                "title": d.get("goodsName",""),
                "venue":d.get("placeName",""),
                "category":d.get("genreSubName",""),
                "genre":d.get("genreName",""),
                "place_code":d.get("placeCode",""),
                "age_limit":d.get("viewRateName",""),
                "hours":d.get("playTime",""),
            }
            
            result["start_date"]=self._format_date(d.get("playStartDate",""))
            result["end_date"]=self._format_date(d.get("playEndDate",""))
            
            img = d.get("goodsLargeImageUrl", "") or d.get("goodsSmallImageUrl")
            result["image_url"]=("https:"+img) if img.startswith("//") else img
            
            result["day_rank"]=self._safe_int(d.get("dayRank"))
            result["week_rank"]=self._safe_int(d.get("weekRank"))
            result["month_rank"]=self._safe_int(d.get("monthRank"))
            
            rank_parts=[]
            for val,label in [(result["day_rank"], "일간"), (result["week_rank"], "주간"), (result["month_rank"], "월간")]:
                if val is not None:
                    rank_parts.append(f"{label} {val}위")
            result["rank"]=" / ".join(rank_parts) if rank_parts else None
            
            parsed=self._parse_display_template(d.get("displayTemplate",""))
            result["address"]=parsed.get("address")
            result["notice"]=parsed.get("notice")
            if not result["hours"] and parsed.get("hours"):
                result["hours"]=parsed.get("hours")
            
            result["location"]=self._extract_location(result.get("address",""))
            return result
        
        except requests.RequestException as e:
            print(f"Summary API 요청 실패: {e} - goods_code: {goods_code}")
            return None
        except Exception as e:
            print(f"Summary API 데이터 처리 중 오류: {e} - goods_code: {goods_code}")
            return None
        
    # 주소 
    def get_place(self,place_code:str)->dict|None:
        if not place_code:
            return None
        
        url=self.PLACE_API_URL.format(place_code=place_code)
        print(f"장소 API 호출: {url}")
        print(f"디버그 -> place_code: {place_code}")
        try:
            resp=self.session.get(url,timeout=10)
            resp.raise_for_status()
            raw=resp.json()
            if raw.get("common",{}).get("message")!= "success":
                return None
            d=raw.get("data",{})
            if not d:
                return None
            return {
                "address":d.get("placeAddress",""),
                "latitude":self._safe_float(d.get("latitude")),
                "longitude":self._safe_float(d.get("longitude")),
            }
        except requests.RequestException as e:
            print(f"장소 API 요청 실패: {e} - place_code: {place_code}")
            return None
        except Exception as e:
            print(f"장소 API 데이터 처리 중 오류: {e} - place_code: {place_code}")
            return None
        
    # 가격 API
    
    def _get_best_price_map(self,goods_code:str)->dict:
        url=self.BEST_API_URL.format(goods_code=goods_code)
        try:
            resp=self.session.get(url,timeout=10)
            resp.raise_for_status()
            raw=resp.json()
            
            if raw.get("common",{}).get("message")!="success":
                return {}
            
            items=raw.get("data",[])
            if not isinstance(items,list):
                return {}
            
            return {
                item.get("priceGrade",""):{
                    "origin_price":item.get("originPrice",0),
                    "discount_rate":item.get("discountRate",0),
                }
                for item in items
                if item.get("priceGrade")
            }
        except requests.RequestException as e:
            print(f"Best 가격 API 요청 실패 {e}- goods_code:{goods_code}")
            return {}
        except Exception as e:
            print(f"Best 가격 API 파싱 오류 {e}- goods_code:{goods_code}")
            return {}
    
    def get_price(self,goods_code:str)->tuple[str|None,list[dict]]:
        url=self.PRICE_API_URL.format(goods_code=goods_code)
        try:
            resp=self.session.get(url,timeout=10)
            resp.raise_for_status()
            raw=resp.json()
            
            
            data=raw if isinstance(raw,dict) else {}
            
            if "common" in data:
                if data.get("common",{}).get("message")!="success":
                    return None,[]
                data=data.get("data",{})
            
            if not data:
                return None,[]
            
            best_map=self._get_best_price_map(goods_code)
            fallback_discount=next(
                (v["discount_rate"] for v in best_map.values() if v["discount_rate"]>0),0
            )
            
            prices_raw_list=[]
            price_rows=[]
            
            for seat_grade_name,type_dict in data.items():
                if not isinstance(type_dict,dict):
                    continue
                for price_type_name, items in type_dict.items():
                    if not isinstance(items,list):
                        continue
                    for item in items:
                        if item.get("salesPrice") is None:
                            continue
                        
                        price_grade=item.get("priceGrade","")
                        sales_price=item.get("salesPrice",0)
                        best=best_map.get(price_grade,{})
                        
                        origin_price=best.get("origin_price") or item.get("originPrice",0)
                        discount_rate=best.get("discount_rate") or item.get("discountRate",0)
                        
                        if origin_price == 0 and discount_rate >0 and sales_price >0:
                            origin_price=round(sales_price / (1 - discount_rate/100))
                        elif origin_price ==0 and fallback_discount >0 and sales_price>0:
                            origin_price=round(sales_price / (1 - fallback_discount/100))
                            discount_rate=fallback_discount
                            
                        
                        prices_raw_list.append({
                            "name":item.get("priceGradeName"),
                            "price":item.get("salesPrice"),
                            "type":price_type_name,
                            "seat_grade":seat_grade_name,
                            "origin_price":origin_price,
                            "discount_rate":discount_rate,
                        })
                        
                        price_rows.append({
                            "exhibition_id":goods_code,
                            "seat_grade":item.get("seatGrade",""),
                            "seat_grade_name":seat_grade_name,
                            "price_grade":item.get("priceGrade",""),
                            "price_grade_name":item.get("priceGradeName",""),
                            "price_type_code":item.get("priceTypeCode",""),
                            "price_type_name":price_type_name,
                            "sales_price":item.get("salesPrice"),
                            "origin_price":origin_price,
                            "discount_rate":discount_rate,
                        })
                        
            prices_raw_str=(
                    json.dumps(prices_raw_list,ensure_ascii=False) if prices_raw_list else None
            )
            return prices_raw_str, price_rows
            
            
        except requests.RequestException as e:
            print(f"가격 API 요청 실패 {e}- goods_code:{goods_code}")
            return None,[]
        except Exception as e:
            print(f"가격 API 파싱 오류 {e}- goods_code:{goods_code}")
            return None,[]
        
    def get_stats(self,goods_code:str,place_code:str="")->dict|None:
        params={
            "goodsCode":goods_code,
            "placeCode":place_code,
            "types":"ALL",
        }
        try:
            resp=self.session.get(self.STATS_API_URL,params=params,timeout=10)
            resp.raise_for_status()
            raw=resp.json()
            
            if raw.get("common",{}).get("message")!="success":
                return None
            
            d=raw.get("data",{})
            if not d:
                return None
            
            if isinstance(d,str):
                d=json.loads(d)
            
            age_gender=d.get("ageGender",{})
            
            def get_rate(key:str)->float|None:
                val=age_gender.get(key)
                return self._safe_float(val)
            
            return {
                "exhibition_id": goods_code,
                "age10_rate":  self._safe_float(age_gender.get("age10Rate")),
                "age20_rate": self._safe_float(age_gender.get("age20Rate")),
                "age30_rate": self._safe_float(age_gender.get("age30Rate")),
                "age40_rate": self._safe_float(age_gender.get("age40Rate")),
                "age50_rate": self._safe_float(age_gender.get("age50Rate")),
                "male_rate":self._safe_float(age_gender.get("maleRate")),
                "female_rate":self._safe_float(age_gender.get("femaleRate")),
                "stats_raw":json.dumps(d,ensure_ascii=False),
            }
        except requests.RequestException as e:
            print(f"통계 API 요청 실패 {e}- goods_code:{goods_code}")
            return None
        except Exception as e:
            print(f"통계 API 파싱 오류 {e}- goods_code:{goods_code}")
            return None
    
    def get_exhibition_detail(self,info:dict)->tuple["Exhibition|None",list[dict],dict|None]:
        goods_code=info.get("exhibition_id","")
        if not goods_code:
            return None,[],None
        
        ex =Exhibition(
            exhibition_id=goods_code,
            title=info.get("title",""),
            venue=info.get("venue",""),
            location=info.get("location"),
            image_url=info.get("image_url"),
            detail_url=info.get("detail_url"),
            start_date=info.get("start_date"),
            end_date=info.get("end_date"),
            rank=info.get("rank"),
            age_limit=info.get("age_limit"),
            category=info.get("category")
        )
        
        summary=self.get_exhibition_summary(goods_code)
        if summary:
            ex.title=summary.get("title") or ex.title
            ex.venue=summary.get("venue") or ex.venue
            ex.category=summary.get("category") or ex.category
            ex.genre=summary.get("genre")
            ex.age_limit=summary.get("age_limit") or ex.age_limit
            ex.hours=summary.get("hours")
            ex.day_rank   = summary.get("day_rank") 
            ex.week_rank  = summary.get("week_rank")  
            ex.month_rank = summary.get("month_rank")
            ex.rank       = summary.get("rank") or ex.rank 
            ex.notice=summary.get("notice")
            ex.address=summary.get("address")
            ex.start_date=summary.get("start_date") or ex.start_date
            ex.end_date=summary.get("end_date") or ex.end_date
            
            if summary.get("image_url"):
                ex.image_url=summary["image_url"] 
                
        place_code=summary.get("place_code") if summary else None
        if place_code:
            place=self.get_place(place_code)
            if place:
                ex.address=place.get("address")
                ex.latitude=place.get("latitude")
                ex.longitude=place.get("longitude")
                if ex.address:
                    ex.location=self._extract_location(ex.address) # 주소에서 location 다시 뽑는게 더 좋다.!!! 그럼 '시'까지 빠지고 깔끔해졌다!!
                    
                    
        if not ex.location and ex.venue:
            ex.location=self._extract_location(ex.venue)
            
        prices_raw_str,price_rows=self.get_price(goods_code)
        ex.prices_raw=prices_raw_str
        
        stats = self.get_stats(goods_code, place_code=place_code or "")
        
        print(f"price_rows: {len(price_rows)}개 - {price_rows[:2]} ...")  
        
        price_preview=(
            (prices_raw_str[:50]+"...") if prices_raw_str and len(prices_raw_str)>50
            else prices_raw_str
        )
        print(
            f"{ex.location }|"
            f"순위: 주간{ex.week_rank}위/일간{ex.day_rank}위/월간{ex.month_rank}위|"
            f"가격 미리보기: {price_preview}|"
            f"{ex.start_date}~{ex.end_date}|"
        )
        return ex, price_rows,stats
    
    def crawl_all(self,max_pages:int=10)->tuple[list[Exhibition],list[dict],list[dict]]:
        print("전체 크롤링 시작...")
        try:
            links=self.get_exhibition_list(max_pages=max_pages)
            if not links:
                print("수집된 전시회가 없습니다.")
                return [],[],[]
            
            print(f"\n 상세 크롤링 시작... 총 {len(links)}개")
            exhibitions=[]
            all_price_rows=[]
            all_stats=[]
            
            for i, link in enumerate(links,1):
                print(f"\n[{i}/{len(links)}] 크롤링 중: {link['title'][:30]}")
                ex,price_rows,stats=self.get_exhibition_detail(link)
                print(f" detail=>{ex}, price_rows=>{price_rows[:2]} ...")
                if ex:
                    exhibitions.append(ex)
                    all_price_rows.extend(price_rows)
                    if stats:
                        all_stats.append(stats)
                    time.sleep(self.delay)
            print(f"\n크롤링 완료: {len(exhibitions)}개 전시회, {len(all_price_rows)}개 가격 항목 수집")
            
            locs:dict[str,int]={}
            for ex in exhibitions:
                loc=ex.location or "기타"
                locs[loc]=locs.get(loc,0)+1
            print(f"\n 지역별:")
            for loc, cnt in sorted(locs.items(),key=lambda x:-x[1]):
                print(f"  {loc}: {cnt}개")
            return exhibitions, all_price_rows, all_stats
        
        except Exception as e:
            print(f"전체 크롤링 중 오류: {e}")
            return [],[],[]
                    