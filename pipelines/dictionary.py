"""Data dictionary helpers."""
from dataclasses import dataclass
from typing import Any, Dict
import yaml
from pathlib import Path

@dataclass
class AttributeRule:
    """Rule for validating an attribute in a data dictionary."""
    name: str
    type: str
    tolerance: float | None = None
    ignore_case: bool = False
    trim: bool = False

class AttributeRuleSet:
    def __init__(self, rules: Dict[str, AttributeRule]) -> None:
        self.rules = rules
    
    @classmethod
    def from_yaml(cls, file_path: str | Path) -> "AttributeRuleSet":
        with open(file_path, "r") as file:
            cfg = yaml.safe_load(file)
        rules = {}
        for name, spec in cfg.get("attributes", {}).items():
            rules[name] = AttributeRule(
                name = name,
                type = spec["type"],                            ## required field
                tolerance = spec.get("tolerance"),              ## optional field  
                ignore_case = spec.get("ignore_case", False),   ## optional field with default
                trim = spec.get("trim", False),                 ## optional field with default
            )
        return cls(rules)
    
    def get_rule(self, attribute_name: str) -> AttributeRule | None:
        return self.rules.get(attribute_name)