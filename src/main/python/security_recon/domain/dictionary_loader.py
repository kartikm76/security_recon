"""Helpers for hydrating ``AttributeRuleSet`` definitions."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from security_recon.domain.dictionary import AttributeRule, AttributeRuleSet
from security_recon.support.paths import resource_path
import yaml

def _build_rule_set(config: Dict[str, Any]) -> AttributeRuleSet:
    attributes = config.get("attributes", {}) or {}
    if not isinstance(attributes, dict):
        attributes = {}
    rules: Dict[str, AttributeRule] = {}
    for name, spec in attributes.items():
        if not isinstance(spec, dict):
            continue
        rules[name] = AttributeRule(
            name=name,
            type=spec["type"],
            tolerance=spec.get("tolerance"),
            ignore_case=spec.get("ignore_case", False),
            trim=spec.get("trim", False),
        )
    return AttributeRuleSet(rules)


def load_rules_from_yaml(file_path: str | Path) -> AttributeRuleSet:
    path = Path(file_path)
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _build_rule_set(config)


def load_rules_from_resource(resource_name: str = "data_dictionary.yml") -> AttributeRuleSet:
    return load_rules_from_yaml(resource_path(resource_name))
