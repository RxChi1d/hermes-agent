"""ClinePass provider profile.

ClinePass is an OpenAI-compatible chat-completions API served at
https://api.cline.bot/api/v1. The model IDs are namespaced as
cline-pass/<model-id>.
"""

from providers import register_provider
from providers.base import ProviderProfile

clinepass = ProviderProfile(
    name="clinepass",
    aliases=("cline-pass",),
    display_name="ClinePass",
    description="ClinePass — subscription access to selected open coding models",
    signup_url="https://app.cline.bot/dashboard/subscription?personal=true",
    env_vars=("CLINE_API_KEY", "CLINE_BASE_URL"),
    base_url="https://api.cline.bot/api/v1",
    auth_type="api_key",
    default_aux_model="cline-pass/deepseek-v4-flash",
    fallback_models=(
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
    ),
)

register_provider(clinepass)
