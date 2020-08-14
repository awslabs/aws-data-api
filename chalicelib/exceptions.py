NO_GREMLIN = "Cannot perform Lineage search without Gremlin Endpoint"


class ConstraintViolationException(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class UnimplementedFeatureException(Exception):
    def __init__(self, message=None):
        super().__init__("Unimplemented Feature" if message is None else message)


class ResourceNotFoundException(Exception):
    def __init__(self, message=None):
        super().__init__("Resource Not Found" if message is None else message)


class InvalidArgumentsException(Exception):
    def __init__(self, message=None):
        super().__init__("Invalid Arguments" if message is None else message)


class DetailedException(Exception):
    message = None
    detail = None

    def __init__(self, message=None, detail=None, ):
        super().__init__(f"General Exception: {message}")
        self.message = message
        self.detail = detail
