"""Data dictionary shared models."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

@dataclass
class AttributeRule:
    """Rule for validating an attribute in a data dictionary."""
    name: str
    type: str
    tolerance: float | None = None
    ignore_case: bool = False
    trim: bool = False


@dataclass(frozen=True)
class AttributeRuleSet:
    rules: Mapping[str, AttributeRule]

    def get_rule(self, attribute_name: str) -> AttributeRule | None:
        return self.rules.get(attribute_name)
