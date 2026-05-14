#!/usr/bin/env python3

from typing import Literal, TypedDict, Any

CronjobActions = Literal['do_this_later', 'notify_user']

class CronjobExpression(TypedDict):
    """ The cronjob expression DSL """
    action: CronjobActions
    params: dict[str, Any]
    cronjob_frequensy: Literal["loop","once"]
    time: int

def parse(input: CronjobExpression):
    match input["action"]:
        case 'do_this_later':
            pass








def do_this_later(ai_instruction: str):
    pass
