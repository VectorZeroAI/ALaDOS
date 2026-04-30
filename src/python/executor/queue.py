from ..utils.uqueue import Uqueue
from ..types import ReferenceTo

executor_interrupt_queue = Uqueue[str]()

executor_queue = Uqueue[int]()

embedder_queue = Uqueue[ReferenceTo]()
