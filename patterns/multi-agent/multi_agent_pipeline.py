"""
Multi-Agent Pipeline Pattern (Claude API)
------------------------------------------
Production-hardened multi-agent pipeline with:
- Typed schemas between agent stages (Pydantic)
- Validation gates: each stage must pass before the next runs
- Structured failure modes (don't silently swallow errors)
- Continuous quality monitoring hook

Used in AKSH's AI-powered lead research & outreach pipeline.
Reduced manual prospecting work by ~80%.

Pipeline: Research Agent -> Qualification Agent -> Outreach Agent
"""

import asyncio
import logging
from enum import Enum
from typing import Optional

import anthropic
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)

client = anthropic.Anthropic()
MODEL = "claude-opus-4-5"


# --- Schemas ---------------------------------------------------------------

class ResearchOutput(BaseModel):
    company: str
    industry: str
    headcount_estimate: int
    recent_news: list[str]
    tech_stack_signals: list[str]
    confidence: float  # 0-1

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v):
        if not 0 <= v <= 1:
            raise ValueError("confidence must be 0-1")
        return v


class QualificationDecision(Enum):
    QUALIFIED = "QUALIFIED"
    DISQUALIFIED = "DISQUALIFIED"
    NEEDS_MORE_INFO = "NEEDS_MORE_INFO"


class QualificationOutput(BaseModel):
    decision: QualificationDecision
    score: float       # 0-100
    reasons: list[str]
    blockers: list[str]


class OutreachOutput(BaseModel):
    subject: str
    body: str
    personalization_hooks: list[str]
    send_channel: str  # "email" | "linkedin" | "twitter"


class PipelineResult(BaseModel):
    company: str
    research: ResearchOutput
    qualification: QualificationOutput
    outreach: Optional[OutreachOutput]
    disqualified_at: Optional[str] = None


# --- Agent stages ----------------------------------------------------------

class ResearchAgent:
    SYSTEM = """You are a B2B research analyst. Given a company name and URL,
    extract key facts needed to assess fit for outreach. Be concise and factual.
    Output valid JSON matching the ResearchOutput schema."""

    async def run(self, company: str, url: str) -> ResearchOutput:
        prompt = f"""Research this company for outreach qualification:
Company: {company}
URL: {url}

Return JSON with fields: company, industry, headcount_estimate,
recent_news (list of 2-3 strings), tech_stack_signals (list), confidence (0-1)"""

        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=self.SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text
        # Validation gate: parse + validate schema
        try:
            return ResearchOutput.model_validate_json(raw)
        except Exception as e:
            logger.error(f"ResearchAgent schema validation failed: {e}\nRaw: {raw[:200]}")
            raise ValueError(f"Research output failed schema validation: {e}")


class QualificationAgent:
    SYSTEM = """You are a sales qualification specialist. Given research data about a company,
    decide if they are a qualified prospect. Be strict -- false positives waste time.
    Output valid JSON matching the QualificationOutput schema."""

    ICP_CRITERIA = """
    Ideal Customer Profile:
    - B2B SaaS or fintech company
    - 50-500 employees
    - Uses Python or TypeScript in their stack
    - Growing (recent funding or hiring signals)
    """

    async def run(self, research: ResearchOutput) -> QualificationOutput:
        prompt = f"""Qualify this prospect against our ICP:

{self.ICP_CRITERIA}

Research data:
{research.model_dump_json(indent=2)}

Return JSON with fields: decision (QUALIFIED/DISQUALIFIED/NEEDS_MORE_INFO),
score (0-100), reasons (list), blockers (list of disqualifying factors if any)"""

        response = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=self.SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text
        try:
            return QualificationOutput.model_validate_json(raw)
        except Exception as e:
            logger.error(f"QualificationAgent schema validation failed: {e}")
            raise ValueError(f"Qualification output failed schema validation: {e}")


class OutreachAgent:
    SYSTEM = """You are an expert B2B copywriter. Write hyper-personalized,
    concise outreach messages. No generic boilerplate. 1-3 sentences max for cold email.
    Output valid JSON matching the OutreachOutput schema."""

    async def run(self, research: ResearchOutput, qualification: QualificationOutput) -> OutreachOutput:
        prompt = f"""Write a personalized outreach message for this qualified prospect.

Research: {research.model_dump_json(indent=2)}
Why qualified: {qualification.reasons}

Return JSON with fields: subject, body, personalization_hooks (list), send_channel"""

        response = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=self.SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text
        try:
            return OutreachOutput.model_validate_json(raw)
        except Exception as e:
            logger.error(f"OutreachAgent schema validation failed: {e}")
            raise ValueError(f"Outreach output failed schema validation: {e}")


# --- Orchestrator ----------------------------------------------------------

class MultiAgentPipeline:
    """
    Orchestrates the Research -> Qualification -> Outreach pipeline.

    Key design decisions:
    1. Each stage validates its output schema before passing to the next
    2. Disqualification at any stage is a clean exit (not an error)
    3. Errors are explicit and propagate -- no silent None returns
    4. min_score gate prevents low-confidence outputs from proceeding
    """

    MIN_QUALIFICATION_SCORE = 65.0

    def __init__(self):
        self.research_agent = ResearchAgent()
        self.qualification_agent = QualificationAgent()
        self.outreach_agent = OutreachAgent()

    async def run(self, company: str, url: str) -> PipelineResult:
        logger.info(f"Pipeline start: {company}")

        # Stage 1: Research
        research = await self.research_agent.run(company, url)
        logger.info(f"Research complete: confidence={research.confidence:.2f}")

        # Validation gate: low confidence research -> don't qualify
        if research.confidence < 0.5:
            logger.info(f"Disqualified at research: confidence too low ({research.confidence:.2f})")
            return PipelineResult(
                company=company,
                research=research,
                qualification=QualificationOutput(
                    decision=QualificationDecision.DISQUALIFIED,
                    score=0,
                    reasons=["Research confidence too low"],
                    blockers=["Insufficient data to qualify"],
                ),
                outreach=None,
                disqualified_at="research",
            )

        # Stage 2: Qualification
        qualification = await self.qualification_agent.run(research)
        logger.info(f"Qualification: {qualification.decision.value} (score={qualification.score:.0f})")

        if qualification.decision != QualificationDecision.QUALIFIED:
            return PipelineResult(
                company=company,
                research=research,
                qualification=qualification,
                outreach=None,
                disqualified_at="qualification",
            )

        if qualification.score < self.MIN_QUALIFICATION_SCORE:
            logger.info(f"Score {qualification.score:.0f} below minimum {self.MIN_QUALIFICATION_SCORE}")
            return PipelineResult(
                company=company,
                research=research,
                qualification=qualification,
                outreach=None,
                disqualified_at="score_gate",
            )

        # Stage 3: Outreach (only for qualified prospects)
        outreach = await self.outreach_agent.run(research, qualification)
        logger.info(f"Outreach generated for {company} via {outreach.send_channel}")

        return PipelineResult(
            company=company,
            research=research,
            qualification=qualification,
            outreach=outreach,
        )


# --- Batch runner with quality monitoring hook ---------------------------

async def run_batch(prospects: list[dict], quality_monitor=None) -> list[PipelineResult]:
    """
    Run pipeline on a batch of prospects.
    quality_monitor(result) is called after each result for continuous monitoring.
    """
    pipeline = MultiAgentPipeline()
    results = []

    for prospect in prospects:
        try:
            result = await pipeline.run(prospect["company"], prospect["url"])
            results.append(result)

            if quality_monitor:
                quality_monitor(result)

        except Exception as e:
            logger.error(f"Pipeline failed for {prospect['company']}: {e}")
            # Continue batch -- one failure doesn't stop others

    qualified = [r for r in results if r.outreach is not None]
    logger.info(
        f"Batch complete: {len(results)} processed, "
        f"{len(qualified)} qualified, "
        f"{len(results) - len(qualified)} disqualified"
    )
    return results


# --- Usage example ---------------------------------------------------------
if __name__ == "__main__":
    prospects = [
        {"company": "Acme Corp", "url": "https://acmecorp.io"},
        {"company": "BuildFast", "url": "https://buildfast.dev"},
    ]

    def log_result(result: PipelineResult):
        status = "QUALIFIED" if result.outreach else f"SKIP ({result.disqualified_at})"
        print(f"  {result.company}: {status}")

    results = asyncio.run(run_batch(prospects, quality_monitor=log_result))

    for r in results:
        if r.outreach:
            print(f"\n--- {r.company} ---")
            print(f"Subject: {r.outreach.subject}")
            print(f"Body: {r.outreach.body}")
