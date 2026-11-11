"""Finite nonnegative Kubernetes scalar quantities for observed CPU/memory quotas."""
import re
from decimal import Decimal, DecimalException

def decimal_quantity(value):
    if isinstance(value,bool):
        raise ValueError('Boolean quantity')
    match=re.fullmatch(r'([+]?[0-9]+(?:\.[0-9]+)?)([numkMGTPE]|[KMGTPE]i|[eE][+-]?[0-9]+)?',str(value))
    if not match:
        raise ValueError('Unsupported quantity')
    suffix=match[2] or ''
    scales={'':Decimal(1),'n':Decimal('1e-9'),'u':Decimal('1e-6'),'m':Decimal('1e-3'),'k':Decimal(1000)}
    scales.update({s:Decimal(1000)**(i+2) for i,s in enumerate('MGTPE')})
    scales.update({s+'i':Decimal(1024)**(i+1) for i,s in enumerate('KMGTPE')})
    try:
        factor=Decimal(10)**int(suffix[1:]) if suffix.startswith(('e','E')) and len(suffix)>1 else scales[suffix]
        number=Decimal(match[1])*factor
        if not number.is_finite() or not 0<=number<=Decimal('1e18') or (number==0 and Decimal(match[1])!=0):
            raise ValueError('Out-of-range quantity')
        return number
    except (DecimalException,OverflowError,KeyError):
        raise ValueError('Invalid quantity') from None

def quantity_string(number):
    result=format(number,'f')
    return result.rstrip('0').rstrip('.') if '.' in result else result


def normalize_quota_hard(hard):
    """Canonical values only; preserve exact keys and reject malformed maps."""
    if not isinstance(hard, dict):
        raise ValueError('Quota hard must be an object')
    return {key: quantity_string(decimal_quantity(value)) for key, value in hard.items()}
