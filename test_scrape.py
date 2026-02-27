import asyncio
import aiohttp
from bs4 import BeautifulSoup

async def fetch():
    url = "https://finance.yahoo.com/news/joby-aviation-incurs-loss-q4-182100342.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Accept": "text/html",
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "lxml")
            paragraphs = soup.find("body").find_all("p") if soup.find("body") else []
            text = " ".join(p.get_text(strip=True) for p in paragraphs)
            print(text[:1000])

asyncio.run(fetch())
