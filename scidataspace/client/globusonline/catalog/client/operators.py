"""
Enum like objects describing the operators supported by tagfiler and the
dataset API.
"""
from globusonline.catalog.client.rest_client import urlquote


class DictObject(dict):
    """Mutable dict where keys can be accessed as instance variables."""
    def __getattr__(self, name):
        return self[name]


"""Enum of binary tagfiler operators."""
BinaryOp = DictObject(
    EQUAL="=",
    NOT_EQUAL="!=",
    GT=":gt:",
    GEQ=":geq:",
    LT=":lt:",
    LEQ=":leq:",
    FULLTEXT=":word:",
    NOT_FULLTEXT=":!word:",
    LIKE=":like:",
    SIMTO=":simto:",
    REGEXP=":regexp:",
    NOT_REGEXP=":!regexp:",
    REGEXP_CASE_INSENSITIVE=":ciregexp:",
    NOT_REGEXP_CASE_INSENSITIVE=":!ciregexp:",
)
BinaryOpSet = set(BinaryOp.values())

"""Enum of unary tagfiler operators."""
UnaryOp = DictObject(
    ABSENT=":absent:",
    TAGGED=""
)
UnaryOpSet = set(UnaryOp.values())

Op = DictObject()
Op.update(BinaryOp)
Op.update(UnaryOp)


def build_selector(selector_list):
    """Build a tagfiler selector query from a list of tuples."""
    sl = []
    for s in selector_list:
        if (not isinstance(s, (tuple, list))) or len(s) == 1:
            s = (s, UnaryOp.TAGGED)

        if len(s) == 2:
            tagname, op = s
            if op in BinaryOpSet:
                raise ValueError("Binary operator '%s' requires second "
                                 "argument" % op)
            elif op not in UnaryOpSet:
                raise ValueError("Unknown operator '%s'" % op)
            sl.append("%s%s" % (urlquote(tagname), op))
        elif len(s) == 3:
            tagname, op, value = s
            if op in UnaryOpSet:
                raise ValueError("Unary operator '%s' does not support a"
                                 "second argument" % op)
            elif op not in BinaryOpSet:
                raise ValueError("Unknown operator '%s'" % op)
            else:
                if not isinstance(value, (tuple, list)):
                    value = (value,)
                value = ",".join(urlquote(v) for v in value)
                sl.append("%s%s%s" % (urlquote(tagname), op, value))
        else:
            raise ValueError("Selector expression must contain one, two "
                            +"or three values")
    return ";".join(sl)


def build_projection(projection_list):
    """Build a tagfiler projection from a list of tuples."""
    if not projection_list:
        return ""
    pl = []
    for p in projection_list:
        if not isinstance(p, (tuple, list)):
            p = (p,)

        if len(p) == 1:
            pl.append(urlquote(p[0]))
        elif len(p) == 2:
            pl.append("%s=%s" % (urlquote(p[0]), urlquote(p[1])))
        else:
            raise ValueError("Projection expression must contain one or "
                             "more values")
    return ";".join(pl)
