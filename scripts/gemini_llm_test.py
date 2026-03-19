#!/usr/bin/env python3
# To run this code you need to install the following dependencies:
# pip install google-genai

from __future__ import annotations

import argparse
import os

from google import genai  # type: ignore
from google.genai import types  # type: ignore


def generate(user_text: str, model: str = "gemini-3-flash-preview") -> None:
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=user_text),
            ],
        ),
    ]
    tools = [
        types.Tool(
            googleSearch=types.GoogleSearch(),
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_level="LOW",
        ),
        safety_settings=[
            types.SafetySetting(
                category="HARM_CATEGORY_HARASSMENT",
                threshold="BLOCK_NONE",  # Block none
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_HATE_SPEECH",
                threshold="BLOCK_NONE",  # Block none
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                threshold="BLOCK_NONE",  # Block none
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_DANGEROUS_CONTENT",
                threshold="BLOCK_NONE",  # Block none
            ),
        ],
        tools=tools,
    )

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        if chunk.text:
            print(chunk.text, end="")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt", nargs="?", default="INSERT_INPUT_HERE")
    ap.add_argument("--model", default="gemini-3-flash-preview")
    args = ap.parse_args()

    generate(args.prompt, model=args.model)
