"""This module contains transformation functions."""
from __future__ import annotations
import enum
from functools import reduce
import polars as pl


class Transformation(str, enum.Enum):
    """Transform methods that can be applied to a LazyFrame"""

    FILTER: str = "filter"
    JOIN: str = "join"
    SELECT: str = "select"
    SORT: str = "sort"
    LIMIT: str = "limit"


mapping = {
    "eq": "eq",
    "in": "is_in",
    "ne": "ne",
    "gt": "gt",
    "lt": "lt",
    "ge": "ge",
    "le": "le",
}


# TODO: check if it's not better to use pl.col(key).or_ or pl.col(key).and_ instead reduce
def build_expression(filter_dict: dict):
    if "AND" in filter_dict:
        and_conditions = [build_expression(cond) for cond in filter_dict["AND"]]
        return reduce(lambda a, b: a & b, and_conditions)
    if "OR" in filter_dict:
        or_conditions = [build_expression(cond) for cond in filter_dict["OR"]]
        or_conditions_reduced = reduce(lambda a, b: a | b, or_conditions)
        return or_conditions_reduced

    key, value = list(filter_dict.items())[0]
    operator, operand = list(value.items())[0]
    op = mapping[operator]
    return getattr(pl.col(key), op)(operand)


filter_condition = {
    "OR": [
        {
            "AND": [
                {"Country": {"IN": ["Chile", "Bulgaria", "Cyprus", "Canada"]}},
                {
                    "City": {
                        "IN": [
                            "East Leonard",
                            "Kimport",
                            "Robersonstad",
                            "New Alberttown",
                        ]
                    }
                },
            ]
        },
        {
            "AND": [
                {"Country": {"EQ": "United States of America"}},
                {
                    "OR": [
                        {"City": {"EQ": "New Kaitlyn"}},
                        {"City": {"EQ": "San Francisco"}},
                    ]
                },
            ]
        },
    ]
}
