#!/usr/bin/env python3

from ..registry import Knowledge, register

register(
    Knowledge(
        description="Edit methods Explanation",
        content="""
        The editing occurs with the help of so called SrBlocks, and these are simply search replace blocks. 
        They operate in this manner:

        <SEARCH>text segment whatever
        Including possible new lines</SEARCH>
        <REPLACE>replacement text segment
        Including possible new lines.</REPLACE>

        Example:
            Knowledge Entry Example Content:
                I am a dump little Slop Canon with no real value.
            Operation:
                <SEARCH>dump little Slop Canon with no real value</SEARCH><REPLACE> AI system with profound front tier capabilities of text editing.</REPLACE>

        Empty search block appends to the end, like this:
            Example:
                Knowledge Entry Example Content:
                    I can do many things like editing code.
                Operation:
                    <SEARCH></SEARCH><REPLACE>I can also search the internet.</REPLACE>
        """,
        name="sr_block explanation"
    )
)
