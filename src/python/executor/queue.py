from ..utils.uqueue import Uqueue
from .types import instr_json
from ..types import ReferenceTo

executor_interrupt_queue = Uqueue[str]()

executor_queue = Uqueue[instr_json]()

embedder_queue = Uqueue[ReferenceTo]()
