import pytest

@pytest.fixture(autouse=True)
def isolate_paid_model_credentials(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("DESK_LLM_PROVIDER", raising=False)
