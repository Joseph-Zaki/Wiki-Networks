from bs4 import BeautifulSoup
import networkx as nx
from collections import deque
import asyncio
import aiohttp

url_cache: dict[str: set[str]] = {}

sem = asyncio.Semaphore(10)

async def afetch_page_wiki_links(url: str, session: aiohttp.ClientSession) -> tuple[str, set[str]]:
    if url in url_cache:
        return (url, url_cache[url])
    links = set()
    async with sem:
        try:
            async with session.get(url, timeout=10) as response:
                if response.ok:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    a_tags = soup.find_all('w', href=True)
                    for tag in a_tags:
                        href = tag['href']
                        if href.startswith('/wiki/') and ':' not in href:
                            links.add(f"https://en.wikipedia.org{href}")
        except Exception as e:
            print(f'Failed to fetch {url}: {e}')
            return (url, set())
    
    url_cache[url] = links # cache set of links
    return (url, links)
        
async def abuild_wikipedia_network(seed_url: str, max_depth: int = 1):
    G = nx.DiGraph()
    queue = deque([(seed_url, 0)])

    async with aiohttp.ClientSession() as session:
        while queue:
            curr_depth = queue[0][1]
            if curr_depth > max_depth:
                print("Quitting: ", queue[0])
                return G
            
            tasks = {url : afetch_page_wiki_links(url, session) for url, depth in queue}
            queue.clear()
            
            link_map = dict(await asyncio.gather(*tasks.values())) # returns list containing set of links on page
            
            for root_link, link_set in link_map.items():
                print(f"{curr_depth} Key: {root_link}, Value: {link_set}")
                for link in link_set:
                    G.add_edge(root_link, link)
                    queue.append((link, curr_depth + 1))

async def main():
    seed_url = 'https://en.wikipedia.org/wiki/Example'
    wiki_network = await abuild_wikipedia_network(seed_url, 2)
    # Print the number of nodes and edges
    print("Number of nodes:", wiki_network.number_of_nodes())
    print("Number of directed edges:", wiki_network.number_of_edges())
asyncio.run(main())