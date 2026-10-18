"""A deliberately small interpreter for the subset of Kestra/Pebble expressions used by the flow's validate_genes condition.

IT IS NOT PEBBLE. It implements only constructs that Kestra's documentation describes (https://kestra.io/docs/expressions/syntax,
/filters/collections, /filters/strings): variables, string and integer literals, the filters join(sep), replace({map}, regexp=true),
length, and the operators ~ (string concatenation), != , == and `or`/`and`, with parentheses. Its purpose is to evaluate the LITERAL
condition text from the YAML (not a retyped copy), to reject constructs outside that subset, and to refuse any expression that
relies on operator precedence (a binary operator needs parentheses around its operands), so the result cannot depend on how Pebble
orders operators. Whether Kestra's real Pebble engine agrees with this reading remains to be shown by a runtime test.
"""
import re

TOKEN = re.compile(r"\s*(?:(?P<str>'[^']*')|(?P<num>\d+)|(?P<op>!=|==|[()|{},:~=])|(?P<name>[A-Za-z_][A-Za-z0-9_.]*))")
BINARY = ("or", "and", "!=", "==", "~")
FILTERS = ("join", "replace", "length")


class SubsetError(ValueError):
    pass


def tokenize(text):
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = TOKEN.match(text, pos)
        if not m or m.end() == pos:
            raise SubsetError("cannot tokenise at %r" % text[pos:pos + 20])
        pos = m.end()
        kind = m.lastgroup
        value = m.group(kind)
        out.append(("str", value[1:-1]) if kind == "str" else ("num", int(value)) if kind == "num" else (kind, value))
    return out


class Parser:
    def __init__(self, tokens, context):
        self.t, self.i, self.ctx = tokens, 0, context
        self.used_filters, self.used_ops = set(), set()

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def take(self, expect=None):
        tok = self.peek()
        if expect is not None and tok[1] != expect:
            raise SubsetError("expected %r, found %r" % (expect, tok[1]))
        self.i += 1
        return tok

    def expression(self):
        left = self.pipeline()
        kind, value = self.peek()
        if value in BINARY and kind in ("op", "name"):
            self.take()
            self.used_ops.add(value)
            right = self.pipeline()
            result = {"or": lambda a, b: bool(a) or bool(b), "and": lambda a, b: bool(a) and bool(b), "!=": lambda a, b: a != b,
                      "==": lambda a, b: a == b, "~": lambda a, b: str(a) + str(b)}[value](left, right)
            nxt = self.peek()[1]
            if nxt in BINARY:
                raise SubsetError("two binary operators without parentheses (%r then %r): precedence must not matter" % (value, nxt))
            return result
        return left

    def pipeline(self):
        value = self.primary()
        while self.peek() == ("op", "|"):
            self.take()
            name = self.take()[1]
            if name not in FILTERS:
                raise SubsetError("filter %r is outside the documented subset" % name)
            self.used_filters.add(name)
            args = self.arguments() if self.peek() == ("op", "(") else []
            value = self.apply(name, value, args)
        return value

    def primary(self):
        kind, value = self.take()
        if (kind, value) == ("op", "("):
            inner = self.expression()
            self.take(")")
            return inner
        if kind in ("str", "num"):
            return value
        if kind == "name":
            node = self.ctx
            for part in value.split("."):
                node = node[part]
            return node
        raise SubsetError("unexpected token %r" % (value,))

    def arguments(self):
        self.take("(")
        args = []
        while self.peek() != ("op", ")"):
            kind, value = self.peek()
            if (kind, value) == ("op", "{"):
                self.take()
                mapping = {}
                while self.peek() != ("op", "}"):
                    key = self.take()[1]
                    self.take(":")
                    mapping[key] = self.take()[1]
                    if self.peek() == ("op", ","):
                        self.take()
                self.take("}")
                args.append(mapping)
            elif kind == "name" and self.t[self.i + 1] == ("op", "="):
                name = self.take()[1]
                self.take("=")
                flag = self.take()[1]
                args.append((name, flag == "true"))
            else:
                args.append(self.take()[1])
            if self.peek() == ("op", ","):
                self.take()
        self.take(")")
        return args

    @staticmethod
    def apply(name, value, args):
        if name == "length":
            return len(value)
        if name == "join":
            return (args[0] if args else "").join(str(v) for v in value)
        if name == "replace":
            regexp = any(isinstance(a, tuple) and a == ("regexp", True) for a in args)
            for key, repl in args[0].items():
                value = re.sub(key, repl, value) if regexp else value.replace(key, repl)
            return value
        raise SubsetError(name)


def evaluate(condition, context):
    """Evaluate a '{{ ... }}' expression. Returns (value, used_filters, used_operators)."""
    body = condition.strip()
    if not (body.startswith("{{") and body.endswith("}}")):
        raise SubsetError("not a {{ }} expression")
    parser = Parser(tokenize(body[2:-2]), context)
    value = parser.expression()
    if parser.i != len(parser.t):
        raise SubsetError("trailing tokens: %r" % (parser.t[parser.i:],))
    return value, parser.used_filters, parser.used_ops
