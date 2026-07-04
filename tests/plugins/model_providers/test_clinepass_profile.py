"""Tests for the bundled ClinePass provider profile."""

from __future__ import annotations

import pytest


_OFFICIAL_CLINEPASS_MODELS = {
    "cline-pass/glm-5.2",
    "cline-pass/kimi-k2.7-code",
    "cline-pass/kimi-k2.6",
    "cline-pass/deepseek-v4-pro",
    "cline-pass/deepseek-v4-flash",
    "cline-pass/mimo-v2.5",
    "cline-pass/mimo-v2.5-pro",
    "cline-pass/minimax-m3",
    "cline-pass/qwen3.7-max",
    "cline-pass/qwen3.7-plus",
}


@pytest.fixture
def clinepass_profile():
    import model_tools  # noqa: F401
    import providers

    profile = providers.get_provider_profile("clinepass")
    assert profile is not None, "clinepass provider profile must be registered"
    return profile


def test_clinepass_profile_registration_and_alias(clinepass_profile):
    from hermes_cli.models import normalize_provider

    assert clinepass_profile.name == "clinepass"
    assert clinepass_profile.display_name == "ClinePass"
    assert clinepass_profile.base_url == "https://api.cline.bot/api/v1"
    assert normalize_provider("cline-pass") == "clinepass"
    assert normalize_provider("clinepass") == "clinepass"


def test_clinepass_profile_includes_official_models(clinepass_profile):
    assert _OFFICIAL_CLINEPASS_MODELS.issubset(set(clinepass_profile.fallback_models))
    assert clinepass_profile.default_aux_model == "cline-pass/deepseek-v4-flash"


@pytest.mark.usefixtures("clinepass_profile")
def test_clinepass_resolves_as_builtin_provider():
    from hermes_cli.providers import get_provider, resolve_provider_full

    pdef = get_provider("clinepass")
    assert pdef is not None
    assert pdef.id == "clinepass"
    assert pdef.base_url == "https://api.cline.bot/api/v1"

    full = resolve_provider_full("clinepass")
    assert full is not None
    assert full.id == "clinepass"
    assert full.base_url == "https://api.cline.bot/api/v1"


@pytest.mark.usefixtures("clinepass_profile")
def test_provider_model_ids_falls_back_to_official_models_without_key(monkeypatch):
    monkeypatch.delenv("CLINE_API_KEY", raising=False)
    monkeypatch.delenv("CLINE_BASE_URL", raising=False)

    from hermes_cli.models import provider_model_ids

    models = provider_model_ids("clinepass")
    assert _OFFICIAL_CLINEPASS_MODELS.issubset(set(models))
