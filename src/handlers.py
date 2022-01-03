from protocol.aave_v2.lcr import lambda_handler as aave_lcr
from protocol.aave_v2.lcr_historical import lambda_handler as aave_lcr_historical
from protocol.aave_v2.var import lambda_handler as aave_var
from protocol.aave_v2.var_historical import lambda_handler as aave_var_historical
from protocol.compound_v2.lcr import lambda_handler as compound_lcr
from protocol.compound_v2.lcr_historical import lambda_handler as compound_lcr_historical
from protocol.compound_v2.var import lambda_handler as compound_var
from protocol.compound_v2.var_historical import lambda_handler as compound_var_historical


def aave_lcr_historical_handler(event, context):
    aave_lcr_historical(event, context)


def aave_var_historical_handler(event, context):
    aave_var_historical(event, context)


def aave_lcr_handler(event, context):
    aave_lcr(event, context)


def aave_var_handler(event, context):
    aave_var(event, context)


def compound_lcr_historical_handler(event, context):
    compound_lcr_historical(event, context)


def compound_var_historical_handler(event, context):
    compound_var_historical(event, context)


def compound_lcr_handler(event, context):
    compound_lcr(event, context)


def compound_var_handler(event, context):
    compound_var(event, context)
