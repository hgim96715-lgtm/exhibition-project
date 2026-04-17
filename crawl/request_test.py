import requests, json

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.5 Safari/605.1.15",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://tickets.interpark.com/contents/genre/exhibition"
})

params = {
    "genre": "EXHIBIT",
    "page": 2,
    "pageSize": 50,
    "sort": "WEEKLY_RANKING",
}

response = session.get(
    "https://tickets.interpark.com/contents/api/goods/genre",
    params=params,
    timeout=10
)
data = response.json()
print(data)
