from python.rmt.dsl import serialise
from python.rmt.main import create_from_serial

DSL_STR = """
START -> (instruction='lol1', scope='general', id='1') -> (instruction='lol2', scope='task', id='last') -> END
(id='1') -> (instruction='lol3', scope='general') -> (instruction='lol4', scope='task', id='random') -> (id='last')
(id='1') -> (instruction='lol4') -> (id='random')
"""

addr = create_from_serial(DSL_STR)
print(DSL_STR)
print('\n')
print(serialise(addr))
