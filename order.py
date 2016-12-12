from ib.opt import Connection
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.ComboLeg import ComboLeg

from Util import Util

import ezibpy




if __name__ == '__main__':
    ezConn = ezibpy.ezIBpy()
    contract = ezConn.createFuturesContract("YM", exchange="GLOBEX", expiry="201612")
    order = ezConn.createBracketOrder(contract, quantity=1, entry=0, target=2200., stop=1900.)
    print order