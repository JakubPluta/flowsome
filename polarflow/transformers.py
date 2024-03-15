import enum


class RowBasedTransformation(str, enum.Enum):
    FILTER = 'FILTER'
    LIMIT = 'LIMIT'
    SELECT = 'SELECT'


# TODO: Posible to use lambda func, or some outof the box functions
class SchemaBasedTransformation(str, enum.Enum):
    RENAME = 'RENAME'
    CHANGE_TYPE = 'CHANGE_TYPE'
    
    
class ShapeBasedTransformation(str, enum.Enum):
    GROUP_BY = 'GROUP_BY'
    PIVOT = 'PIVOT'
    UNPIVOT = 'UNPIVOT'
    JOIN = 'JOIN'