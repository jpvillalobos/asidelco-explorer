from openai import OpenAI

client = OpenAI(
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
