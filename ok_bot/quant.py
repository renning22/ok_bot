from decimal import Decimal


class Quant(Decimal):
    """Fixed-decimal with algebraic-closure

    Quant applies with any other operants int/float/str will still stay Quant.
    """
    def __new__(cls, value=0):
        if isinstance(value, float):
            return super().__new__(cls, str(value))
        else:
            return super().__new__(cls, value)

    def __repr__(self):
        return str(self)

    def to_json(self):
        """Protocol to json.dumps."""
        return str(self)


# https://docs.python.org/3/reference/datamodel.html?highlight=__int__#emulating-numeric-types
_ALL_ARITHMETIC_OPERATORS = [
    '__add__',
    '__sub__',
    '__mul__',
    '__matmul__',
    '__truediv__',
    '__floordiv__',
    '__mod__',
    '__divmod__',
    '__pow__',
    '__lshift__',
    '__rshift__',
    '__and__',
    '__xor__',
    '__or__',
    '__radd__',
    '__rsub__',
    '__rmul__',
    '__rmatmul__',
    '__rtruediv__',
    '__rfloordiv__',
    '__rmod__',
    '__rdivmod__',
    '__rpow__',
    '__rlshift__',
    '__rrshift__',
    '__rand__',
    '__rxor__',
    '__ror__',
    '__iadd__',
    '__isub__',
    '__imul__',
    '__imatmul__',
    '__itruediv__',
    '__ifloordiv__',
    '__imod__',
    '__ipow__',
    '__ilshift__',
    '__irshift__',
    '__iand__',
    '__ixor__',
    '__ior__',
]


def _patch_all_arithmetic_operators_type_closure():
    def _create_operator(operator):
        def _operator_wrapper(self, value):
            return Quant(getattr(Decimal, operator)(self, Quant(value)))
        return _operator_wrapper

    for operator in _ALL_ARITHMETIC_OPERATORS:
        if hasattr(Quant, operator):
            setattr(Quant, operator, _create_operator(operator))


_patch_all_arithmetic_operators_type_closure()


if __name__ == '__main__':
    a = Quant(1234.5678)
    b = a + 10.1
    c = 7.77 * b
    d = c ** 2
    e = d / 1000
    f = e // 9
    g = 0.00000000001 + f
    h = '1.2345' + g
    i = h * '678.910'

    assert (0.00000001 + g - 0.00000001) == g
    assert ((1234.5678 * g) / 1234.5678) == g

    print(a)
    print(b)
    print(c)
    print(d)
    print(e)
    print(f)
    print(g)
    print(h)
    print(i)
