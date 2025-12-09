import os
from typing import Optional
from openai import OpenAI

def get_openai_client(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    organization: Optional[str] = None,
    timeout: int = 60,
) -> OpenAI:
    """
    Create an OpenAI client only when called.
    No global clients, no import-time side effects.
    """
    key = api_key or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set. Pass api_key or export OPENAI_API_KEY.")
    return OpenAI(api_key=key, base_url=base_url, organization=organization, timeout=timeout)

client = get_openai_client(
    api_key="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"
)

completion = client.chat.completions.create(
  model="gpt-4o-mini",
  store=True,
  messages=[
    {"role": "user", "content": "write a haiku about ai"}
  ]
)

print(completion.choices[0].message);
