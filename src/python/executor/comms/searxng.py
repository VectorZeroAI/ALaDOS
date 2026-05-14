#!/usr/bin/env python3


import random
import httpx
from bs4 import BeautifulSoup

class SearxngSearcher:
    """
    A searcher that randomly picks a working SearXNG instance from a 
    dynamically fetched list and parses its HTML results.
    """

    INSTANCES_URL = "https://searx.space/data/instances.json"

    def __init__(self, timeout: float = 10.0):
        """
        Initialize the searcher by fetching the list of public instances.
        
        Args:
            timeout: HTTP request timeout in seconds.
        """
        self.timeout = timeout
        self.instances = self._fetch_instances()
        if not self.instances:
            raise RuntimeError("Could not fetch any SearXNG instances.")

    def _fetch_instances(self) -> list[str]:
        """Download and filter instances from searx.space."""
        try:
            resp = httpx.get(self.INSTANCES_URL, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"SEARXNG SEARCH COMMUNICATION SYSTEM: Warning: could not fetch instance list ({e}). Using empty list.")
            return []

        valid = []
        for url, info in data.get("instances", {}).items():
            # Safely get the uptime dict; default to empty dict if None/missing
            uptime_data = info.get("uptime") or {}
            monthly_uptime = uptime_data.get("uptimeMonth", 0)

            # Ensure it's a number before comparing
            if not isinstance(monthly_uptime, (int, float)):
                monthly_uptime = 0

            if monthly_uptime > 95:
                valid.append(url.rstrip("/"))
        return valid

    def _remove_broken(self, instance: str) -> None:
        """Remove an instance that has failed."""
        if instance in self.instances:
            self.instances.remove(instance)

    def search(self, query: str) -> list[dict[str, str]]:
        """
        Perform a search using a random, healthy instance.
        
        Args:
            query: The search term.
            
        Returns:
            List of result dicts with keys 'title', 'url', 'snippet'.
            
        Raises:
            RuntimeError: If no working instances remain.
        """
        while self.instances:
            instance = random.choice(self.instances)
            search_url = f"{instance}/search"
            params = {"q": query}

            try:
                resp = httpx.get(search_url, params=params, timeout=self.timeout)
                resp.raise_for_status()
            except Exception as e:
                self._remove_broken(instance)
                continue  # try another one

            return self._parse_html(resp.text)

        raise RuntimeError("All instances exhausted. No valid SearXNG instance available.")

    def _parse_html(self, html: str) -> list[dict[str, str]]:
        """
        Extract search results from SearXNG HTML.
        
        According to SearXNG conventions each result is an <article class="result">
        containing an <h3><a href="...">title</a></h3> and a <p class="content">snippet</p>.
        """
        soup = BeautifulSoup(html, "html.parser")
        results = []

        for article in soup.find_all("article", class_="result"):
            # Title & URL
            title_tag = article.find("h3")
            a_tag = title_tag.find("a") if title_tag else None
            title = a_tag.get_text(strip=True) if a_tag else "N/A"
            url = a_tag.get("href") if a_tag else ""

            # Snippet
            snippet_tag = article.find("p", class_="content")
            snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""

            results.append({
                "title": title,
                "url": url,
                "snippet": snippet,
            })

        return results


# ----------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    searcher = SearxngSearcher()
    try:
        results = searcher.search("Python asyncio tutorial")
        for i, r in enumerate(results, 1):
            print(f"{i}. {r['title']}\n   {r['url']}\n   {r['snippet']}\n")
    except RuntimeError as e:
        print(f"Search failed: {e}")
