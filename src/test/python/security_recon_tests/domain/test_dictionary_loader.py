from __future__ import annotations

from pathlib import Path

from security_recon.domain.dictionary import AttributeRuleSet
from security_recon.domain.dictionary_loader import load_rules_from_resource, load_rules_from_yaml

def test_load_rules_from_resource() -> None:
    rule_set = load_rules_from_resource()
    assert isinstance(rule_set, AttributeRuleSet)
    coupon_rule = rule_set.get_rule("coupon")
    assert coupon_rule is not None
    assert coupon_rule.tolerance == 0.01
    assert coupon_rule.ignore_case is False


def test_load_rules_from_yaml_round_trip(tmp_path: Path) -> None:
    sample_yaml = """
                    attributes:
                      field_a:
                        type: string
                        ignore_case: true
                      field_b:
                        type: float
                        tolerance: 1.5
                    """
    yaml_path = tmp_path / "rules.yml"
    yaml_path.write_text(sample_yaml, encoding="utf-8")

    rule_set = load_rules_from_yaml(yaml_path)
    field_a_rule = rule_set.get_rule("field_a")
    field_b_rule = rule_set.get_rule("field_b")
    assert field_a_rule is not None
    assert field_a_rule.ignore_case is True
    assert field_b_rule is not None
    assert field_b_rule.tolerance == 1.5
