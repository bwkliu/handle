import ezibpy
import time

# initialize ezIBpy
ibConn = ezibpy.ezIBpy()
ibConn.connect(clientId=100, host="localhost", port=7497)

# create a contract
contract = ibConn.createFuturesContract("YM", exchange="ECBOT", expiry="201612")

# submit a bracket order (entry=0 = MKT order)
order = ibConn.createBracketOrder(contract, quantity=1, entry=0, target=19800., stop=19900.)

# let order fill
time.sleep(1)

# see the positions
print("Positions")
print(ibConn.positions)

# disconnect
ibConn.disconnect()