from python.rmt.dsl import serialise
from python.rmt.main import create_from_serial

addr = create_from_serial("START -> (instruction='lol1', scope='general') -> (instruction='lol2', scope='task') -> END")
print(serialise(addr))
