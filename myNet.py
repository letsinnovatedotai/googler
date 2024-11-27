import aiohttp
import asyncio
import mimetypes
import asyncio
import aiohttp
import mimetypes
from yarl import URL
try:
    import magic
    HAVE_MAGIC = True
except ImportError:
    HAVE_MAGIC = False

class ContentTypeInferer:
    def __init__(self, timeout=10, max_concurrent_per_host=10):
        self.timeout = timeout
        self.max_concurrent_per_host = max_concurrent_per_host

    def is_valid_url(self, url):
        try:
            parsed = URL(url)
            return parsed.scheme in ('http', 'https')
        except Exception:
            return False


    async def fetch_content_type(self, session, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            async with session.get(url, headers=headers, timeout=self.timeout) as response:
                response.raise_for_status()  # Raise exception for HTTP errors
                content_type = response.headers.get('Content-Type', '').split(';')[0]
                if not content_type or content_type == 'application/octet-stream':
                    # Read a small portion of the content to infer type
                    data = await response.content.read(2048)  # Read first 2KB
                    if HAVE_MAGIC:
                        content_type = magic.from_buffer(data, mime=True)
                    else:
                        # Fallback to mimetypes based on URL extension
                        content_type, _ = mimetypes.guess_type(url)
                        if not content_type:
                            content_type = 'application/octet-stream'
                return url, content_type
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            
            msg = f"error fetching {url}: {e}"
            return url, msg
        except Exception as e:
            msg = f"error fetching {url}: {e}"
            return url, msg


    async def infer_content_type_async(self, urls):
        valid_urls = [url for url in urls if self.is_valid_url(url)]
        invalid_urls = set(urls) - set(valid_urls)
        if invalid_urls:
            print("Invalid URLs:")
            for url in invalid_urls:
                print(f" - {url}")
        
        if not valid_urls:
            print("No valid URLs provided.")
            return {}

        results = {}
        connector = aiohttp.TCPConnector(limit_per_host=self.max_concurrent_per_host)  # Limit concurrent connections per host
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.fetch_content_type(session, url) for url in valid_urls]
            for future in asyncio.as_completed(tasks):
                url, content_type = await future
                results[url] = content_type

        return results

    def infer_content_type(self, urls):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.infer_content_type_async(urls))