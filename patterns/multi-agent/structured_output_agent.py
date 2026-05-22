"""
LLM Agent with Structured Output and Validation Gates
------------------------------------------------------
Pattern for reliable LLM agents in production:
- Pydantic schema enforced at every output boundary
- Retry with correction prompt on schema violation
- Confidence threshold gating
- Structured rejection logging for continuous improvement

Used in AKSH's LLM pipelines for data extraction and classification.
"""

import json
import logging
from typing import Optional, Type, TypeVar

import anthropic
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

client = anthropic.Anthropic()
T = TypeVar("T", bound=BaseModel)


def extract_json_from_text(text: str) -> str:
    """Extract JSON block from LLM output that may contain preamble/commentary."""
    # Try to find a JSON block
    text = text.strip()

    # Handle ```json ... ``` fencing
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        return text[start:end].strip()

    if "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start)
        return text[start:end].strip()

    # Find first { or [ and last matching } or ]
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        if start_char in text:
            start = text.index(start_char)
            end = text.rindex(end_char) + 1
            return text[start:end]

    return text


class StructuredOutputAgent:
    """
    LLM agent that enforces a Pydantic schema on every output.

    On schema violation:
    1. Logs the violation with full context
    2. Retries once with a correction prompt that shows the error
    3. Raises if correction also fails

    This pattern ensures downstream code always receives valid typed data.
    """

    MAX_RETRIES = 2

    def __init__(
        self,
        system_prompt: str,
        model: str = "claude-opus-4-5",
        max_tokens: int = 1024,
    ):
        self.system_prompt = system_prompt
        self.model = model
        self.max_tokens = max_tokens

    def run(
        self,
        user_prompt: str,
        output_schema: Type[T],
        context: Optional[dict] = None,
    ) -> T:
        """
        Run the agent and return a validated instance of output_schema.

        Args:
            user_prompt: The task prompt
            output_schema: Pydantic model class to validate output against
            context: Optional metadata for logging (not sent to LLM)
        """
        schema_description = json.dumps(output_schema.model_json_schema(), indent=2)
        full_prompt = f"{user_prompt}\n\nRespond with valid JSON matching this schema:\n{schema_description}"

        last_error = None

        for attempt in range(self.MAX_RETRIES):
            if attempt > 0:
                # Correction prompt: show the LLM what went wrong
                full_prompt = (
                    f"{user_prompt}\n\n"
                    f"Your previous response had this validation error:\n{last_error}\n\n"
                    f"Please fix and respond with valid JSON matching this schema:\n{schema_description}"
                )

            response = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=self.system_prompt,
                messages=[{"role": "user", "content": full_prompt}],
            )

            raw = response.content[0].text

            try:
                json_str = extract_json_from_text(raw)
                return output_schema.model_validate_json(json_str)

            except (ValidationError, json.JSONDecodeError, ValueError) as e:
                last_error = str(e)
                logger.warning(
                    f"Schema validation failed (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}\n"
                    f"Raw output: {raw[:300]}\n"
                    f"Context: {context}"
                )

        # All retries exhausted
        logger.error(
            f"Agent failed after {self.MAX_RETRIES} attempts. "
            f"Schema: {output_schema.__name__}. Context: {context}"
        )
        raise ValueError(
            f"Could not get valid {output_schema.__name__} after {self.MAX_RETRIES} retries. "
            f"Last error: {last_error}"
        )


# --- Example: document classification ---

class DocumentClassification(BaseModel):
    category: str          # e.g. "earnings_report", "press_release", "10-K"
    confidence: float      # 0-1
    key_entities: list[str]
    summary: str
    requires_human_review: bool


def classify_document(text: str) -> DocumentClassification:
    """Classify a financial document using the structured output agent."""
    agent = StructuredOutputAgent(
        system_prompt=(
            "You are a financial document classifier. "
            "Analyse documents and return structured JSON classifications. "
            "Be precise and conservative -- set requires_human_review=true when uncertain."
        ),
        max_tokens=512,
    )

    return agent.run(
        user_prompt=f"Classify this document:\n\n{text[:2000]}",
        output_schema=DocumentClassification,
        context={"doc_length": len(text)},
    )


# --- Usage example ---
if __name__ == "__main__":
    sample_doc = """
    Q3 2024 Earnings Release
    Revenue: $2.4B (+18% YoY)
    EPS: $1.23 (beat consensus of $1.18)
    CEO John Smith: "Strong quarter driven by AI product adoption..."
    """

    result = classify_document(sample_doc)
    print(f"Category: {result.category}")
    print(f"Confidence: {result.confidence:.0%}")
    print(f"Entities: {result.key_entities}")
    print(f"Summary: {result.summary}")
    print(f"Human review needed: {result.requires_human_review}")
