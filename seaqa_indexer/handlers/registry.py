import logging

logger = logging.getLogger(__name__)


class ConnectionHandlerRegistry:
    """Registry for connection type handlers.
    
    This class maintains a mapping from connection_type strings to their
    corresponding handler instances.
    """
    
    _handlers = {}
    
    @classmethod
    def register(cls, handler_class):
        if handler_class.connection_type is None:
            raise ValueError(f"Handler {handler_class.__name__} has no connection_type defined")
        cls._handlers[handler_class.connection_type] = handler_class()
        logger.debug(f"Registered handler for {handler_class.connection_type}: {handler_class.__name__}")
    
    @classmethod
    def get(cls, connection_type: str):
        handler = cls._handlers.get(connection_type)
        if not handler:
            raise ValueError(f'Unsupported connection type: {connection_type}')
        return handler
    
    @classmethod
    def get_or_none(cls, connection_type: str):
        return cls._handlers.get(connection_type)
    
    @classmethod
    def get_table_name(cls, connection_type: str, connection_id) -> str:
        handler = cls.get_or_none(connection_type)
        if handler:
            return handler.gen_table_name(connection_id)
        return ''


def get_handler(connection_type: str):
    return ConnectionHandlerRegistry.get(connection_type)


def get_handler_or_none(connection_type: str):
    return ConnectionHandlerRegistry.get_or_none(connection_type)


def gen_table_name_by_connection_type(connection_type: str, connection_id) -> str:
    return ConnectionHandlerRegistry.get_table_name(connection_type, connection_id)
