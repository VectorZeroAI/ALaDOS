from python.rmt.dsl import parse

result = parse("""
START -> (instruction='test1', id='lol1') -> (instruction='test2') -> (id='lol2', instruction='test3') -> END
(instruction='test_refs_1') -> (id='lol1')
(id='lol2') -> (id='lol1')
               """)
print(str(result))
