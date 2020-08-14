import re
import chalicelib.parameters as params

# class to simplify working with complicated dynamodb update expressions
class DynamoUpdateExpressionHandler:
    _expression = None
    _tokens = None

    def __init__(self, expression=None):
        self._expression = expression
        self._tokens = {}

        if expression is not None:
            tokens = re.split('(%s|%s|%s)' % (params.SET, params.REMOVE, params.ADD), expression)

            next_token_name = None
            for t in tokens:
                if t.upper() == params.SET:
                    next_token_name = params.SET
                elif t.upper() == params.REMOVE:
                    next_token_name = params.REMOVE
                elif t.upper() == params.ADD:
                    next_token_name = params.ADD
                else:
                    if t != '':
                        self._tokens[next_token_name] = t.strip()

    # add an item to the specified type of expression
    def add_expression_item(self, type, expression):
        if type not in self._tokens:
            self._tokens[type] = expression
        else:
            self._tokens[type] = f"{self._tokens[type]}, {expression}"

    # method which reassembles a set of internal update expression tokens into a valid update expression
    def get_expression(self):
        expressions = []

        for x in [params.SET, params.REMOVE, params.ADD]:
            if x in self._tokens and self._tokens[x] is not None and self._tokens[x] != '':
                expressions.append(f"{x} {self._tokens[x]}")

        return ' '.join(expressions)
