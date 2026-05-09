import re
from typing import TypeAlias

search_and_replace_block: TypeAlias = str

def sr_block_parser(sr_block: search_and_replace_block) -> tuple[str, str]:
    """
    retuns (search, replacement)
    search and replace blocks outputten by the model parser that retuns a list of strings and their replacements. 
    """
    match = re.search(
        r"<SEARCH>\s*(.*?)\s*</SEARCH>\s*\s*<REPLACE>\s*(.*?)\s*</REPLACE>",
        sr_block,
        re.DOTALL
    )
    if not match:
        raise ValueError("No matches found.")
    
    search = match.group(1).strip()
    replacement = match.group(2)
    return (search, replacement)
