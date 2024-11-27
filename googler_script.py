import mimetypes
from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup
import requests
import requests
import myNet
import importlib
importlib.reload(myNet)
cti_obj = myNet.ContentTypeInferer()
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup
import requests
import pandas as pd  # Assuming you need pandas for DataFrame  
import aiohttp
import asyncio
import nest_asyncio
from urllib.parse import urlparse, parse_qs
import re
nest_asyncio.apply()
print("new googler")
class googler:


    

    def generate_google_news_url(self,queries, time_range=None, sort_by=None, region=None, language=None, page=1, num_results=10):
        print("inside gnews")
        """
        Generates a Google News search URL with advanced filters.

        Parameters:
            query (str): Search query.
            time_range (str, optional): Time filter (e.g., "d" for day, "w" for week, "m" for month, "y" for year).
                                    Use formats like "d1" (past 1 day), "w2" (past 2 weeks).
            sort_by (str, optional): Sorting method. Options:
                "relevance" (default): Sort by relevance.
                "date": Sort by newest first.
            region (str, optional): Country code for region (e.g., "US", "IN").
            language (str, optional): Language code (e.g., "en" for English, "fr" for French).
            page (int, optional): Page number (default is 1). Each page skips 10 results.
            num_results (int, optional): Number of results per page (default is 10, max is 100).

        Returns:
            str: URL for the Google News search with applied filters.
        """
        base_url = "https://www.google.com/search?"
        cmb_data = []

        for query in queries:
            search_params = {
                "q": query,
                "tbm": "nws",  # Google News search
                "start": (page - 1) * num_results,  # Pagination: start at 0, 10, 20...
                "num": num_results  # Number of results per page (up to 100)
            }
        
                

            # Add optional filters
            if time_range:
                search_params["tbs"] = f"qdr:{time_range}"
            if sort_by:
                search_params["sort"] = sort_by
            if region:
                search_params["cr"] = f"country{region.upper()}"
            if language:
                search_params["lr"] = f"lang_{language.lower()}"
            
            full_url = base_url + urlencode(search_params)
            print("full url is ",full_url)
            cmb_data.append([query,full_url])
        return cmb_data







    def generate_google_search_url(
        self,
        queries ,
        page: int = 1,
        language: str = 'en',
        country: str = 'US',
        time_range: str = '',  # Examples: 'd' for past 24 hours, 'w' for past week, 'm' for past month, 'y' for past year
        sort_by_date: bool = False,
        safe_search: bool = True,
        file_type: str = '',  # Example: 'pdf', 'doc', 'ppt'
        site: str = '',  # Restrict search to a specific site, e.g., 'example.com'
    ):
        """
        Generates a Google search URL with optional filters.

        :param query: Search query string.
        :param page: Page number of the results (1-indexed).
        :param language: Language code (e.g., 'en' for English).
        :param country: Country code (e.g., 'US' for the United States).
        :param time_range: Time range filter ('d', 'w', 'm', 'y').
        :param sort_by_date: Whether to sort results by date.
        :param safe_search: Enable SafeSearch.
        :param file_type: Restrict results to a specific file type (e.g., 'pdf').
        :param site: Restrict results to a specific site.
        :return: URL string for Google search with the specified filters.
        """
        base_url = 'https://www.google.com/search'
        cmb_data = []
        for query in queries:
            # Construct search parameters
            params = {
                'q': query,
                'hl': language,
                'gl': country,
                'start': (page - 1) * 10,  # Calculate the starting result index for pagination (10 results per page)
                'safe': 'active' if safe_search else 'off',
            }

            # Time range filter
            if time_range:
                params['tbs'] = f'qdr:{time_range}'

            # Sort by date
            if sort_by_date:
                params['tbs'] = params.get('tbs', '') + ',sbd:1'

            # File type filter
            if file_type:
                params['as_filetype'] = file_type

            # Site-specific search
            if site:
                params['q'] = f'site:{site} {params["q"]}'

            # Encode parameters
            encoded_params = urlencode(params, quote_via=quote_plus)
            full_url = f"{base_url}?{encoded_params}"
            cmb_data.append([query,full_url])
        return cmb_data





    def download_html(self,url):

        # Set up headers to mimic a browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"
        }

        # Make a GET request to fetch the raw HTML content
        response = requests.get(url, headers=headers)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Print the prettified HTML of the page
        # print(soup.prettify())
            return soup

            # Save the HTML content to a file
            with open("google_search_results.html", "w", encoding='utf-8') as file:
                file.write(soup.prettify())
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)
            return "No Content found"


    def remove_elements(self,html_string, tags_to_remove=None, classes_to_remove=None):
        """
        Removes specific tags and classes from the HTML string and returns the cleaned HTML.
        
        Parameters:
        html_string (str): The input HTML string.
        tags_to_remove (list): A list of tag names to remove.
        classes_to_remove (list): A list of class names to remove.
        
        Returns:
        str: The cleaned HTML string.
        """
    # print(html_string[0:500])
        # Parse HTML string with BeautifulSoup
        soup = BeautifulSoup(html_string, 'html.parser')
        
        # Remove specific tags
        if tags_to_remove:
            for tag in tags_to_remove:
                for element in soup.find_all(tag):
                    element.decompose()  # Completely remove the tag from the tree
        
        # Remove specific classes
        if classes_to_remove:
            for class_name in classes_to_remove:
                for element in soup.find_all(class_=class_name):
                    element.decompose()  # Completely remove the element with the class from the tree
        
        # Return cleaned HTML as string
        return str(soup)


    async def fetch_html(self, session, url, chunk_size=1024, download_full_content=True):
        """
        Fetch content from a URL and identify its type by analyzing a small fraction of data or entire content.

        Args:
        - session: The aiohttp session.
        - url (str): The URL to fetch.
        - chunk_size (int): Number of bytes to read for content type identification.
        - download_full_content (bool): Flag to control if we fetch entire content or only a small chunk.

        Returns:
        - Tuple containing the content (or error message) and the content type.
        """
        print(f"Fetching URL: {url}")
        try:
            async with session.get(url) as response:
                # Check for successful response
                if response.status == 200:
                    # Get the Content-Type from headers
                    content_type = response.headers.get('Content-Type', '').lower()

                    # If it's a known text-based type, return the entire text content if download_full_content is True
                    if 'text/html' in content_type or 'application/json' in content_type:
                        if download_full_content:
                            return (await response.text(), content_type)
                        else:
                            first_chunk = await response.content.read(chunk_size)
                            return (first_chunk.decode('utf-8'), content_type)

                    # Otherwise, either download the full content or just a chunk
                    if download_full_content:
                        full_content = await response.read()
                        return (full_content, content_type)

                    # Read only a small chunk to inspect the data if download_full_content is False
                    first_chunk = await response.content.read(chunk_size)

                    # Try to guess the MIME type based on the chunk content or URL extension
                    mime_type, _ = mimetypes.guess_type(url)

                    if first_chunk.strip().startswith(b'{') or first_chunk.strip().startswith(b'['):
                        return (first_chunk.decode('utf-8'), "application/json")
                    elif first_chunk.strip().startswith(b'<'):
                        return (first_chunk.decode('utf-8'), "text/html")
                    elif first_chunk[:4] == b'\x89PNG':
                        return (first_chunk, "image/png")
                    elif first_chunk[:2] == b'\xFF\xD8':
                        return (first_chunk, "image/jpeg")
                    elif first_chunk[:4] == b'%PDF':
                        return (first_chunk, "application/pdf")
                    elif first_chunk[:4] == b'PK\x03\x04':
                        return (first_chunk, "application/zip")

                    # If no known type, fallback to guessed MIME type
                    if mime_type:
                        return (first_chunk, "unknown : "+str(mime_type))
                    else:
                        return (first_chunk, "unknown")

                else:
                    # Return error message if the status code is not 200 OK
                    return (f"error : received status code {response.status}", f"error: received status code {response.status}")
        except Exception as e:
            # Return error message if there's an exception during the request
            return (f"error : {str(e)}",f"error: {str(e)}")
    
    


    def parse_google_news_results(self,html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        news_items = []
        rank = 1

        # Iterate over all anchor tags
        for a_tag in soup.find_all('a'):
            # Check if the anchor tag contains an h3 tag (which holds the title)
            h3_tag = a_tag.find('h3')
            if h3_tag:
                # Extract the link and parse it to get the actual URL
                href = a_tag.get('href')
                parsed_href = urlparse(href)
                if parsed_href.path == '/url':
                    qs = parse_qs(parsed_href.query)
                    link = qs.get('q', [''])[0]
                else:
                    link = href

                # Extract the title
                title = h3_tag.get_text().strip()

                # Initialize other fields
                description = ''
                date = ''
                image_url = ''
                media_name = ''

                # Extract media name
                # Find the next sibling div after h3_tag's parent
                h3_parent = h3_tag.parent
                media_div = None

                # Iterate through siblings to find media name
                for sibling in h3_parent.find_next_siblings():
                    if sibling.name == 'div' and sibling.get_text(strip=True):
                        media_div = sibling
                        break

                if media_div:
                    media_name = media_div.get_text(separator=' ').strip()

                # Collect all text-containing divs within the anchor tag for description and date
                description_text = ''
                for div in a_tag.find_all('div'):
                    if div.get_text(strip=True) and div not in [h3_tag, h3_parent, media_div]:
                        text = div.get_text(separator=' ').strip()
                        # Choose the div with the longest text as the description
                        if len(text) > len(description_text):
                            description_text = text

                if description_text:
                    # Use regex to extract date in formats like '5 days ago'
                    date_match = re.search(r'(\d+\s+\w+\s+ago)', description_text)
                    if date_match:
                        date = date_match.group(1)
                        description_text = description_text.replace(date, '').strip()
                    description = description_text

                # Extract image URL if available
                img_tag = a_tag.find('img')
                if img_tag:
                    image_url = img_tag.get('src')

                # Append the news item to the list
                news_items.append({
                    'rank': rank,
                    'link': link,
                    'title': title,
                    'description': description,
                    'date': date,
                    'image_url': image_url,
                    'media_name': media_name
                })

                rank += 1

        # Create a DataFrame from the list of news items
        df = pd.DataFrame(news_items)
        return df



    def parse_google_search_results(self,html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        # This will hold the results
        results = {'rank':[],'title':[],'description':[],'link':[]}

        # Google search results often contain <a> tags where each result is a child
        # We look for <a> tags that contain <h3> tags for titles
        search_results = soup.find_all('a')

        rm = 1
        # Iterate over each <a> tag to filter relevant search results
        for rank, result in enumerate(search_results, start=1):
            # Find the title
            title_tag = result.find('h3')
            if title_tag:
                # Extract title
                title = title_tag.get_text(strip=True)

                # Extract link
                link = result['href']
                # Clean the link if it contains the '/url?q=' pattern
                if link.startswith('/url?q='):
                    link = link.split('/url?q=')[1].split('&')[0]
                
                # Find the description: it's typically the next <div> or <span> after the title <h3> tag in the HTML structure
                description_tag = result.find_next('div', class_=lambda x: x is None)
                if description_tag:
                    description = description_tag.get_text(strip=True)
                else:
                    description = "No description"

                # Append the data to the results
                results['rank'].append(rm)
                results['title'].append(title)
                results['description'].append(description)
                results['link'].append(link)
                rm = rm+1

        results_df = pd.DataFrame(results)
        return results_df

    



    async def fetch_all(self, urls, download_full_content=True):
        """
        Fetch content from multiple URLs concurrently.

        Args:
        - urls (list): List of URLs to fetch.
        - download_full_content (bool): Flag to download the entire content or just a chunk.
        
        Returns:
        - List of tuples containing the content and content type for each URL.
        """
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            tasks = [self.fetch_html(session, url, download_full_content=download_full_content) for url in urls]
            return await asyncio.gather(*tasks)
    


    def fetch_async_html_responses(self,cmb_data_or):
        cmb_data = cmb_data_or.copy()

        all_urls = []
        for cd in cmb_data:
            all_urls.append(cd[1])
        # Running the asyncio event loop
        all_urls_htmls = asyncio.run(self.fetch_all(all_urls))
        
        for cd,auh,au in zip(cmb_data,all_urls_htmls,all_urls):
            if cd[1]!=au:
                print("Some issue here")
            else:
                auh_content = auh[0]
             #   print(auh_content)
                auh_type = auh[1]
                cd.append(auh_content)
                
                if auh_content.startswith("Error"):  
                    cd.append('0')
                else:
                    cd.append('1')
                cd.append(auh_type)
        #query, url, html, 0/1,type
        return cmb_data

    def concater(self,dfs,axis=1):
        dfs_new = []
        for df in dfs:
            df.reset_index(inplace=True,drop=True)
            dfs_new.append(df)
        op = pd.concat(dfs_new,axis=axis)
        return op


  #  def fetch_results()
    def fetch_results(
        self,
            queries,
            page: int = 1,
            language: str = 'en',
            country: str = 'US',
            time_range: str = '',  # Examples: 'd' for past 24 hours, 'w' for past week, 'm' for past month, 'y' for past year
            sort_by_date: bool = False,
            safe_search: bool = True,
            file_type: str = '',  # Example: 'pdf', 'doc', 'ppt'
            site: str = '',
            version: str='gsearch',
            sort_by_gn="date",
            num_results= '20'
            
            ):
            # Example usage:


        if version=="gsearch":
            cmb_data = self.generate_google_search_url(
                queries,
                page,
                language,
                country,
                time_range,  # Results from the past week
                sort_by_date,
                safe_search,
                file_type,
                site)
        else:
            
            cmb_data = self.generate_google_news_url(queries,
                                         time_range=time_range, sort_by=sort_by_gn, 
                                        region=country, language=language, 
                                        page=page,
                                        num_results=num_results)


        cmd_data = self.fetch_async_html_responses(cmb_data)
  #      print
        urls_list_with_failed_fetch = []
        urls_list_with_success_fetch = []
        dfs = []
        for cd in cmd_data:
        #query, url, html, 0/1,type
            if cd[3]=="1":
                html_content_cleaned = self.remove_elements(cd[2], tags_to_remove=['script','style'])
               # print(html_content_cleaned)

                if version=="gsearch":
                     df = self.parse_google_search_results(html_content_cleaned)
                else:
                    df = self.parse_google_news_results(html_content_cleaned)
                #print(df)
                df['query'] =cd[0]
                df['query_url']=cd[1]
                df['success']=cd[3]
                df['type']=cd[4]
                dfs.append(df)
                urls_list_with_success_fetch.append(cd)
            else:
                urls_list_with_failed_fetch.append(cd)

        dfs = self.concater(dfs,axis=0)


        links = list(dfs['link'])

        print("start to detect the types")

        links_types = cti_obj.infer_content_type(links)

        print(links_types)
        print("end to detect the types")
        
        dfs['link_type'] = dfs['link'].map(links_types)
        return dfs,urls_list_with_failed_fetch,urls_list_with_success_fetch 




