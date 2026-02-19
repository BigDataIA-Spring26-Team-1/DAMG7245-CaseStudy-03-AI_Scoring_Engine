from app.scoring_engine.synergy import SynergyRule, compute_synergy


def test_synergy_cap():
    scores = {
        "a": 100,
        "b": 100,
        "c": 100,
        "d": 100,
    }
    rules = [
        SynergyRule("a", "b", "positive", 60, 10),
        SynergyRule("c", "d", "positive", 60, 10),
    ]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == 15.0


def test_synergy_activation_positive():
    scores = {"x": 70, "y": 80}
    rules = [SynergyRule("x", "y", "positive", 60, 3)]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == 3.0
    assert any(h.activated for h in res.hits)
