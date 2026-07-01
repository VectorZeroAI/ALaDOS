from python.rmt.main import create_from_serial, activate_as_master

DSL_STR = """
START -> (id='1', instruction='Add "CODE GREEN" to the master result')
"""

addr = create_from_serial(DSL_STR)
activate_as_master(addr)
