# -*- coding: euc-kr -*-
from catch import *
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')

catch = Catch('source/plan.csv', 'source/tag.csv')
# catch.transmission_error()
# catch.wrong_tag()
# catch.overtime()
catch.holiday()