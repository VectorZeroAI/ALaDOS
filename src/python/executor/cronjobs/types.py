#!/usr/bin/env python3
from dataclasses import dataclass
from ...utils.conn_factory import Conn
from typing import Literal, Any

@dataclass(slots=True)
class SysState:
    conn: Conn 

CronjobActions = Literal['do_this_later', 'notify_user']

@dataclass(slots=True)
class Cronjob:
    """ The cronjob expression DSL """
    action: CronjobActions
    params: dict[str, Any]
    cronjob_type: Literal["loop","once"]
    time: int
