from ..utils.uqueue import Uqueue
from .types import instr_json

executor_interrupt_queue = Uqueue[str]()
executor_queue = Uqueue[instr_json]()
