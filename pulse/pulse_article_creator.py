from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
import pendulum
import json
import logging
import os
import re
import time
import uuid
import base64

import redis as redis_lib

from agent_dags.utils.think_logging import get_logger, set_request_id

logger = logging.getLogger(__name__)
lot = get_logger("pulse_article_creator")

default_args = {"owner": "pulse", "start_date": pendulum.datetime(2025, 1, 1), "retries": 1}

# ---------------------------------------------------------------------------
# Positioning pillars and voice rules (injected into LLM prompts)
# ---------------------------------------------------------------------------

POSITIONING_PILLARS = """
lowtouch.ai Positioning Pillars:
1. Private by Architecture - Runs inside customer infrastructure. No data leaves. Air-gapped support.
2. No-Code - Business and technical teams assemble agents without writing code.
3. Production in 4-6 Weeks - Pre-built agents, predictable timelines from idea to governed production.
4. Governed Autonomy - HITL controls, thought-logging, compliance readiness (ISO 27001, SOC 2, GDPR, RBI).
5. Enterprise Architecture - ReAct/CodeAct frameworks, Apache Airflow orchestration, multi-LLM support, full observability.
"""

VOICE_RULES = """
Content Voice Rules:
- Write as a builder/practitioner, not a marketer. Lead with business outcomes.
- No em dashes. Use commas, periods, semicolons, or parentheses instead.
- No exclamation marks.
- No hype words: "revolutionary," "game-changing," "cutting-edge," "unleash," "unlock."
- Never position lowtouch.ai as a chatbot, copilot, or consumer AI tool.
- Always refer to it as "private, no-code Agentic AI platform."
- Be opinionated. Take clear positions.
- Lead with the problem, then offer the insight.
- Skip jargon: no "synergy," "paradigm shift," "leverage AI."
"""

REDIS_ARTICLE_TTL = 86400 * 7  # 7 days

# Content type param -> (report section key, inner array key)
CONTENT_TYPE_MAP = {
    "blog_outline": ("blog_outlines", "outlines"),
    "linkedin_post": ("linkedin_posts", "posts"),
    "youtube_idea": ("youtube_ideas", "ideas"),
    "carousel": ("carousels", "carousels"),
    "reel": ("reels", "reels"),
    "founders_notebook": ("founders_notebook", None),
}

# ---------------------------------------------------------------------------
# Helper functions (duplicated from pulse_content_engine.py for standalone operation)
# ---------------------------------------------------------------------------


def _get_variable(key):
    """Get an Airflow Variable, trying PULSE_ prefix first then unprefixed."""
    prefixed = f"PULSE_{key}"
    try:
        return Variable.get(prefixed)
    except Exception:
        return Variable.get(key)


def _get_redis():
    """Get a Redis client using the same connection as think_logging."""
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379").split(":")[-1])
    db = os.getenv("REDIS_DB", "0")
    return redis_lib.from_url(f"redis://{host}:{port}/{db}", decode_responses=True)


def _cache_get(key):
    """Get cached data from Redis. Returns parsed dict/list or None."""
    try:
        r = _get_redis()
        data = r.get(key)
        if data:
            logger.info(f"Cache hit for {key}")
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Redis cache read failed: {e}")
    return None


def _cache_set(key, value, ttl=REDIS_ARTICLE_TTL):
    """Set data in Redis cache with TTL."""
    try:
        r = _get_redis()
        r.setex(key, ttl, json.dumps(value))
        logger.info(f"Cached {key} with {ttl}s TTL")
    except Exception as e:
        logger.warning(f"Redis cache write failed: {e}")


def _get_openai_client():
    """Create an OpenAI client using the Airflow Variable."""
    from openai import OpenAI

    api_key = _get_variable("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)


def gpt4o_json(system_message, user_message, max_retries=3):
    """Call OpenAI GPT-4o with JSON response format. Returns parsed dict."""
    client = _get_openai_client()
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message},
                ],
                response_format={"type": "json_object"},
                temperature=0.7,
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning(f"GPT-4o call failed (attempt {attempt + 1}): {e}, retrying in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"GPT-4o call failed after {max_retries} attempts: {e}")
                raise


def gpt4o_text(system_message, user_message, max_retries=3):
    """Call OpenAI GPT-4o returning plain text."""
    client = _get_openai_client()
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.7,
            )
            return response.choices[0].message.content
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning(f"GPT-4o call failed (attempt {attempt + 1}): {e}, retrying in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"GPT-4o call failed after {max_retries} attempts: {e}")
                raise


def _generate_image(prompt, size="1536x1024", max_retries=2):
    """Generate an image via OpenAI gpt-image-1, return base64 string."""
    import requests as req

    client = _get_openai_client()
    for attempt in range(max_retries):
        try:
            response = client.images.generate(
                model="gpt-image-1",
                prompt=prompt,
                size=size,
                n=1,
            )
            image_data = response.data[0]
            # gpt-image-1 may return b64_json directly or a URL
            if hasattr(image_data, "b64_json") and image_data.b64_json:
                return image_data.b64_json
            if hasattr(image_data, "url") and image_data.url:
                img_bytes = req.get(image_data.url, timeout=60).content
                return base64.b64encode(img_bytes).decode("utf-8")
            raise ValueError("No image data in response")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Image generation attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** (attempt + 1))
            else:
                raise


def _extract_title(item, content_type):
    """Extract a human-readable title from a content item based on its type."""
    if content_type == "blog_outline":
        return item.get("title", "Untitled")
    elif content_type == "linkedin_post":
        return (item.get("hook", "") or "Untitled post")[:80]
    elif content_type == "youtube_idea":
        return item.get("title", "Untitled")
    elif content_type == "carousel":
        return item.get("title_slide", "Untitled carousel")
    elif content_type == "reel":
        rtype = item.get("type", "Reel")
        point = item.get("teaching_point", "")
        return f"{rtype}: {point}" if point else rtype
    elif content_type == "founders_notebook":
        return item.get("trending_topic", "Founder's Notebook")
    return "Untitled"


def _classify_intent(user_prompt):
    """Use GPT-4o to classify the user's intent from their prompt.

    Returns dict with:
      - regenerate_graphic (bool): user wants a new header image
      - graphic_only (bool): user ONLY wants graphic work, no article text changes
      - edit_instructions (str|null): cleaned edit instructions for the article text
    """
    try:
        result = gpt4o_json(
            system_message=(
                "You classify user requests for an article editing system. "
                "Given the user's message, determine:\n"
                "1. regenerate_graphic: does the user want a new header image/graphic?\n"
                "2. graphic_only: is the request ONLY about the graphic with NO article text changes?\n"
                "3. edit_instructions: if there are article text edits requested, extract them as a clean instruction. "
                "If graphic_only is true, set this to null.\n\n"
                "Return JSON with keys: regenerate_graphic (bool), graphic_only (bool), edit_instructions (string or null).\n\n"
                "Examples:\n"
                '- "regenerate the graphics" → {"regenerate_graphic": true, "graphic_only": true, "edit_instructions": null}\n'
                '- "make the intro shorter" → {"regenerate_graphic": false, "graphic_only": false, "edit_instructions": "make the intro shorter"}\n'
                '- "regenerate the image and expand case studies" → {"regenerate_graphic": true, "graphic_only": false, "edit_instructions": "expand case studies"}\n'
                '- "create an article on blog outline 2" → {"regenerate_graphic": false, "graphic_only": false, "edit_instructions": "create an article on blog outline 2"}\n'
                '- "new header image please" → {"regenerate_graphic": true, "graphic_only": true, "edit_instructions": null}'
            ),
            user_message=user_prompt,
        )
        return {
            "regenerate_graphic": bool(result.get("regenerate_graphic", False)),
            "graphic_only": bool(result.get("graphic_only", False)),
            "edit_instructions": result.get("edit_instructions"),
        }
    except Exception as e:
        logger.warning(f"Intent classification failed: {e}, falling back to raw instructions")
        return {"regenerate_graphic": False, "graphic_only": False, "edit_instructions": user_prompt}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def load_context(**context):
    """Load report data from Redis and extract the target content item."""
    set_request_id(context)
    p = context["params"]
    ti = context["ti"]

    report_id = p["report_id"]
    content_type = p.get("content_type", "blog_outline")
    content_index = p.get("content_index", 0)
    article_id = p.get("article_id")
    instructions = p.get("instructions")
    regenerate_graphic = p.get("regenerate_graphic", False)

    # Prefer __user_query (injected by RunJobTool from the actual user prompt)
    # over instructions (which the agent may rephrase or pass incorrectly)
    user_query = context["dag_run"].conf.get("__user_query")
    if user_query:
        instructions = user_query
        lot.info(f"user prompt: {instructions}")
    else:
        lot.info(f"user request: {instructions or '(new article)'}")

    # Use LLM to classify user intent from the prompt
    graphic_only = False
    if instructions:
        intent = _classify_intent(instructions)
        lot.info(f"intent: {intent}")
        if intent.get("regenerate_graphic"):
            regenerate_graphic = True
        if intent.get("graphic_only"):
            graphic_only = True
            lot.info("graphic-only request: will regenerate graphic without rewriting article")
        # Use cleaned edit instructions if LLM extracted them
        edit_instructions = intent.get("edit_instructions")
        if edit_instructions and not graphic_only:
            instructions = edit_instructions

    # Auto-resolve article_id from index if not explicitly provided
    if not article_id:
        index_key = f"pulse:article_index:{report_id}:{content_type}:{content_index}"
        try:
            r = _get_redis()
            resolved_id = r.get(index_key)
            if resolved_id:
                article_id = resolved_id
                lot.info(f"found existing article: {article_id}")
        except Exception as e:
            logger.warning(f"Article index lookup failed: {e}")

    # Determine mode: edit (article exists) vs new
    is_edit = article_id is not None
    if is_edit:
        lot.info(f"edit mode: revising article {article_id}")
        if instructions:
            lot.info(f"instructions: {instructions}")
        if regenerate_graphic:
            lot.info("graphic will be regenerated (regenerate_graphic=true)")
        else:
            lot.info("reusing existing graphic (pass regenerate_graphic=true to regenerate)")
    else:
        lot.info(f"new article from {content_type} #{content_index + 1}")

    lot.info(f"loading report {report_id}...")

    # Pull report from Redis
    redis_key = f"pulse:report:{report_id}"
    report = _cache_get(redis_key)
    if not report:
        raise ValueError(f"Report not found in Redis: {redis_key}. It may have expired (7-day TTL).")

    # Extract the content item
    if content_type not in CONTENT_TYPE_MAP:
        raise ValueError(f"Unknown content_type: {content_type}. Valid: {list(CONTENT_TYPE_MAP.keys())}")

    section_key, inner_key = CONTENT_TYPE_MAP[content_type]
    content_section = report.get("content", {}).get(section_key, {})

    if inner_key:
        items = content_section.get(inner_key, [])
        if content_index >= len(items):
            raise ValueError(
                f"Content index {content_index} out of range for {content_type} "
                f"(only {len(items)} items available)"
            )
        content_item = items[content_index]
    else:
        # founders_notebook is a single object
        content_item = content_section

    title = _extract_title(content_item, content_type)
    lot.info(f"found: {title}")

    # Extract trends and top videos for research context
    trends = report.get("analysis", {}).get("trends", {})
    all_videos = report.get("source", {}).get("videos", [])
    top_videos = sorted(all_videos, key=lambda v: v.get("view_count", 0), reverse=True)[:15]

    # If editing, load existing article from Redis
    existing_article = None
    if article_id:
        lot.info(f"loading existing article {article_id} for editing...")
        article_data = _cache_get(f"pulse:article:{article_id}")
        if article_data:
            existing_article = article_data
            lot.info(f"loaded article v{article_data.get('version', 1)}: {article_data.get('title', 'unknown')}")
        else:
            logger.warning(f"Article {article_id} not found in Redis, will create new")
            lot.info("existing article not found, will create new")

    ti.xcom_push(key="content_item", value=content_item)
    ti.xcom_push(key="content_type", value=content_type)
    ti.xcom_push(key="content_title", value=title)
    ti.xcom_push(key="trends", value=trends)
    ti.xcom_push(key="top_videos", value=top_videos)
    ti.xcom_push(key="existing_article", value=existing_article)
    ti.xcom_push(key="instructions", value=instructions)
    ti.xcom_push(key="report_id", value=report_id)
    ti.xcom_push(key="regenerate_graphic", value=regenerate_graphic)
    ti.xcom_push(key="graphic_only", value=graphic_only)
    ti.xcom_push(key="resolved_article_id", value=article_id)  # may be auto-resolved from index

    lot.info("context loaded")
    return {"title": title, "content_type": content_type, "has_existing": existing_article is not None}


def deep_research(**context):
    """Feed the content item and market intelligence to GPT-4o for expanded research."""
    set_request_id(context)
    ti = context["ti"]

    content_item = ti.xcom_pull(task_ids="load_context", key="content_item") or {}
    content_type = ti.xcom_pull(task_ids="load_context", key="content_type") or "blog_outline"
    title = ti.xcom_pull(task_ids="load_context", key="content_title") or "Untitled"
    trends = ti.xcom_pull(task_ids="load_context", key="trends") or {}
    top_videos = ti.xcom_pull(task_ids="load_context", key="top_videos") or []
    existing_article = ti.xcom_pull(task_ids="load_context", key="existing_article")
    instructions = ti.xcom_pull(task_ids="load_context", key="instructions")

    # Skip deep research in edit mode (already have the article and research)
    if existing_article:
        existing_research = existing_article.get("research", {})
        arg_count = len(existing_research.get("key_arguments", []))
        lot.info(f"edit mode: reusing existing research ({arg_count} key arguments)")
        return existing_research

    lot.info(f"researching topic: {title}...")

    # Build video context for the prompt
    vid_lines = []
    for v in top_videos:
        tags = ", ".join(v.get("tags", [])[:5])
        vid_lines.append(
            f"- \"{v.get('title', '')}\" by {v.get('channel_title', '')} "
            f"({v.get('view_count', 0):,} views) [tags: {tags}]"
        )
    vid_text = "\n".join(vid_lines) if vid_lines else "No video data available."

    system_msg = f"""You are a research analyst for lowtouch.ai, a private, no-code Agentic AI platform.

{POSITIONING_PILLARS}

Given the content outline below and the market intelligence data, produce deep research to support
writing a comprehensive LinkedIn article. Structure your response as JSON with these keys:

1. "key_arguments" - array of 4-6 objects, each with "argument" (string), "evidence" (string),
   and "data_point" (string, a specific statistic or fact from the source data)
2. "counterarguments" - array of 2-3 objects, each with "objection" (string) and "rebuttal" (string)
3. "examples" - array of 3-4 real-world examples or use cases, each with "company_or_scenario" (string)
   and "description" (string)
4. "lowtouch_angles" - array of 2-3 strings showing how lowtouch.ai's capabilities uniquely address
   the topic (be specific to the platform's architecture, not generic)
5. "headline_options" - array of 3 headline options for the article, each clear and professional
6. "key_statistics" - array of 4-6 relevant statistics drawn from the source video data and trends

Return valid JSON only."""

    user_msg = f"""CONTENT OUTLINE:
Type: {content_type}
{json.dumps(content_item, indent=2)}

TOP PERFORMING VIDEOS THIS WEEK:
{vid_text}

TREND ANALYSIS:
{json.dumps(trends, indent=2)}"""

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Deep research failed: {e}")
        lot.info("research failed, proceeding with available context")
        result = {
            "key_arguments": [], "counterarguments": [], "examples": [],
            "lowtouch_angles": [], "headline_options": [], "key_statistics": [],
        }

    arg_count = len(result.get("key_arguments", []))
    example_count = len(result.get("examples", []))
    lot.info(f"research complete: {arg_count} key arguments, {example_count} examples")
    return result


def write_article(**context):
    """Write the full article (new) or revise an existing article with user instructions."""
    set_request_id(context)
    ti = context["ti"]

    content_item = ti.xcom_pull(task_ids="load_context", key="content_item") or {}
    content_type = ti.xcom_pull(task_ids="load_context", key="content_type") or "blog_outline"
    title = ti.xcom_pull(task_ids="load_context", key="content_title") or "Untitled"
    existing_article = ti.xcom_pull(task_ids="load_context", key="existing_article")
    instructions = ti.xcom_pull(task_ids="load_context", key="instructions")
    graphic_only = ti.xcom_pull(task_ids="load_context", key="graphic_only") or False
    research = ti.xcom_pull(task_ids="deep_research") or {}

    is_edit = existing_article is not None

    # Graphic-only request: skip article rewrite, pass through existing text
    if graphic_only and existing_article:
        existing_md = existing_article.get("article_md", "")
        # Strip base64 image and footer from stored markdown to get clean article text
        existing_md = re.sub(
            r"!\[Header\]\(data:image/[^;]+;base64,[A-Za-z0-9+/=\s]+\)\n*",
            "",
            existing_md,
        )
        # Strip old footer line
        existing_md = re.sub(r"\n---\n\*Article ID:.*\*$", "", existing_md, flags=re.DOTALL)
        word_count = len(existing_md.split())
        lot.info(f"graphic-only: keeping existing article as-is ({word_count} words)")
        return existing_md.strip()

    if is_edit:
        current_word_count = len(existing_article.get("article_md", "").split())
        lot.info(f"revising article (currently {current_word_count} words)")
        if instructions:
            lot.info(f"applying instructions: {instructions}")
        else:
            instructions = "improve the article while keeping the same topic and structure"
            lot.info(f"no specific instructions, applying general improvements")

        system_msg = f"""You are a senior ghostwriter for Rejith Krishnan, Founder/CEO of lowtouch.ai.
You are editing an existing LinkedIn article based on user feedback.

{POSITIONING_PILLARS}
{VOICE_RULES}

IMPORTANT RULES:
- Apply the user's specific instructions precisely
- Preserve the overall quality, structure, and voice of the article
- Keep the article between 1200-2000 words (sweet spot for LinkedIn dwell time)
- Maintain all positioning and voice rules
- Do not add generic filler to reach word count; every sentence must earn its place
- Return the complete revised article as markdown (not JSON)

LINKEDIN ARTICLE FORMAT:
- Headline (as # heading): under 60 characters, include 1-2 target keywords naturally
- Subtitle (as *italic* line): one sentence that captures the core thesis
- Body with ## section headers every 200-300 words
- Paragraphs: 1-3 sentences max (over 60% of LinkedIn readers are on mobile)
- Use **bold** for key phrases (not full sentences) to aid scanning
- Use bullet points and numbered lists for sequences, comparisons, and takeaways
- End with author bio line

HOOK (first 200 characters are the LinkedIn preview, this determines if anyone reads the article):
- Open with a surprising statistic, contrarian claim, or specific problem statement
- Never open with "In today's...", "As we...", "It's no secret that...", or any throat-clearing
- The first sentence must create urgency or curiosity

CTA (last paragraph):
- End with a specific question that invites comments (not "share if you agree")
- Ask about the reader's experience, a specific challenge, or whether they agree/disagree with a clear position
- Comments count 2x as much as likes in the LinkedIn algorithm"""

        current_md = existing_article.get("article_md", "")
        # Strip base64 image data to avoid blowing up GPT-4o's context limit
        # (stored article_md includes the full graphic blob from assemble_article)
        current_md = re.sub(
            r"!\[Header\]\(data:image/[^;]+;base64,[A-Za-z0-9+/=\s]+\)",
            "[Header graphic preserved]",
            current_md,
        )
        user_msg = f"""CURRENT ARTICLE:
{current_md}

USER'S EDIT INSTRUCTIONS:
{instructions}

Rewrite the article applying these changes. Return the complete revised article as markdown."""

    else:
        lot.info("writing article...")

        system_msg = f"""You are a senior ghostwriter for Rejith Krishnan, Founder/CEO of lowtouch.ai,
a private, no-code Agentic AI platform.

{POSITIONING_PILLARS}
{VOICE_RULES}

Write a 1200-2000 word LinkedIn article based on the research and outline provided.
Target the sweet spot of 1500-1800 words for optimal LinkedIn dwell time and completion rate.

ARTICLE STRUCTURE (7-part framework):
1. Headline (as # heading): under 60 characters for mobile visibility. Include 1-2 target keywords.
   Strong patterns: "Why [common belief] is wrong", "[Number] [things] that [outcome]",
   "We [did X]. Here is what [happened]." Avoid generic headlines like "AI in Enterprise."
2. Subtitle (as *italic* line): one sentence that captures the core thesis
3. Hook (first 2-3 sentences): THIS IS THE MOST IMPORTANT PART. The first 200 characters appear
   as the LinkedIn preview and determine whether anyone reads the article. Use one of:
   - A surprising statistic from the research
   - A contrarian claim that challenges conventional wisdom
   - A specific, relatable problem statement with stakes
   - A brief personal anecdote (1-2 sentences) that grounds the topic in real experience
   NEVER open with "In today's...", "As we...", "It's no secret...", or any throat-clearing.
4. Context (1-2 paragraphs): establish why this matters now, connect to a business outcome
5. Core insights (3-5 sections with ## headers): each section makes ONE clear point with evidence.
   Place a ## header every 200-300 words. Use **bold** on key phrases for scanning.
   Use bullet points or numbered lists for steps, comparisons, or takeaways.
   Include at least one data point or specific example per section.
6. Counterpoint (within a section): address at least one objection directly and rebut it.
   This builds credibility with skeptical enterprise readers.
7. Conclusion and CTA (final section): clear takeaway in 1-2 sentences, then a specific question
   that invites comments. Ask about the reader's experience or whether they agree/disagree
   with a clear position. Never use "share if you agree" or "let me know your thoughts."

WRITING GUIDELINES:
- Write in first person as Rejith, a builder who has shipped this in production
- Lead with business outcomes, not technology features
- Use specific examples and data points from the research
- lowtouch.ai references should feel natural (2-3 mentions max), not forced
- Every sentence must earn its place; no filler, no throat-clearing
- Paragraphs: 1-3 sentences max (over 60% of LinkedIn readers are on mobile)
- Sentences: under 20 words on average for readability
- Use active voice; minimize passive constructions
- Do not start any section with "In today's...", "As we navigate...", or similar AI-isms
- Write for a specific reader: a VP or CTO at a mid-to-large enterprise evaluating AI agents

FORMATTING FOR LINKEDIN:
- Short paragraphs with whitespace between them (critical for mobile readability)
- **Bold** key phrases (not full sentences) so scanners catch the main points
- Use bullet points and numbered lists liberally
- If comparing options or showing data, use a markdown table (40% more engagement than text)

Return the complete article as markdown (not JSON)."""

        user_msg = f"""CONTENT OUTLINE:
Type: {content_type}
Title: {title}
{json.dumps(content_item, indent=2)}

DEEP RESEARCH:
{json.dumps(research, indent=2)}

Write the full article as markdown."""

    try:
        article_md = gpt4o_text(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Article writing failed: {e}")
        raise

    draft_words = len(article_md.split())
    lot.info(f"draft complete: {draft_words} words")
    return article_md


def humanize_article(**context):
    """Second GPT-4o pass to remove AI-isms and ensure practitioner voice."""
    set_request_id(context)
    ti = context["ti"]

    article_md = ti.xcom_pull(task_ids="write_article") or ""
    if not article_md:
        raise ValueError("No article text received from write_article task")

    graphic_only = ti.xcom_pull(task_ids="load_context", key="graphic_only") or False
    if graphic_only:
        lot.info("graphic-only: skipping humanization")
        return article_md

    lot.info("humanizing article...")

    system_msg = f"""You are an editor specializing in removing AI-generated writing patterns
and optimizing LinkedIn articles for maximum engagement and readability.
Your job is to make this article sound like it was written by Rejith Krishnan, a founder
who has personally built and deployed an enterprise Agentic AI platform (lowtouch.ai).

{VOICE_RULES}

SPECIFIC EDITING INSTRUCTIONS:

1. REMOVE these AI-isms and replace with natural phrasing:
   - "In today's rapidly evolving landscape" -> cut entirely or replace with specific context
   - "It's worth noting" -> just state the point directly
   - "Let's dive in" -> cut
   - "In the ever-evolving" -> cut
   - "At the end of the day" -> cut or use "practically" / "in production"
   - "Moving forward" -> cut
   - "Holistic approach" -> be specific about what the approach actually includes
   - "Robust solution" -> describe what makes it reliable
   - "Seamlessly integrate" -> describe the actual integration
   - "It's important to note that" -> just state it
   - "As we navigate" -> cut
   - "The reality is" -> just state the reality
   - "Think of it this way" -> cut, just make the point
   - "Here's the thing" -> cut
   - "Needless to say" -> then don't say it, or just state it directly

2. VOICE CHECK:
   - Does this sound like a person who has built production AI systems, or like a marketing brief?
   - Are there specific details (architectures, failure modes, timelines) or just abstract claims?
   - Would a CTO reading this think "this person has done the work" or "this is content marketing"?
   - Is the tone confident and opinionated, or hedging and generic?

3. HOOK CHECK (first 200 characters):
   - The first sentence MUST create urgency, curiosity, or surprise. This is the LinkedIn preview.
   - If the opening is generic or throat-clearing, rewrite it with a surprising stat, contrarian
     claim, or specific problem statement.
   - The hook determines whether anyone reads the article. Treat it as the highest-priority edit.

4. CTA CHECK (last paragraph):
   - Must end with a specific, answerable question (not "share if you agree" or "let me know your thoughts")
   - The question should invite the reader to share their own experience or take a clear position
   - LinkedIn's algorithm weights comments 2x over likes; a strong CTA drives comments

5. FORMATTING FOR LINKEDIN:
   - No em dashes (replace with commas, periods, semicolons, or parentheses)
   - No exclamation marks
   - No hype words: revolutionary, game-changing, cutting-edge, unleash, unlock, supercharge
   - Paragraphs: 1-3 sentences max (mobile readability is critical, 60%+ of readers are on mobile)
   - Sentences: aim for under 20 words on average
   - Ensure **bold** is used on key phrases (not full sentences) to aid scanning
   - Ensure bullet points and numbered lists are used where content is sequential or comparative
   - Use whitespace generously between paragraphs
   - ## headers should appear every 200-300 words

6. KEEP:
   - The overall structure and section headers
   - The article length (1200-2000 words)
   - Any specific data points, examples, or technical details
   - The headline and subtitle (unless the headline exceeds 60 characters, in which case tighten it)

Return the complete polished article as markdown."""

    user_msg = f"""ARTICLE TO EDIT:

{article_md}

Edit this article following the instructions above. Return the complete polished article as markdown."""

    try:
        polished = gpt4o_text(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Article humanization failed: {e}")
        lot.info("humanization failed, using original draft")
        polished = article_md

    polished_words = len(polished.split())
    lot.info(f"article polished: {polished_words} words")
    return polished


def generate_graphics(**context):
    """Generate a header graphic or reuse existing one during edits."""
    set_request_id(context)
    ti = context["ti"]

    article_md = ti.xcom_pull(task_ids="humanize_article") or ""
    title = ti.xcom_pull(task_ids="load_context", key="content_title") or "Untitled"
    existing_article = ti.xcom_pull(task_ids="load_context", key="existing_article")
    regenerate_graphic = ti.xcom_pull(task_ids="load_context", key="regenerate_graphic") or False

    # In edit mode, reuse existing graphic unless regenerate_graphic is true
    if existing_article and not regenerate_graphic:
        existing_b64 = existing_article.get("graphic_base64")
        existing_prompt = existing_article.get("graphic_prompt")
        if existing_b64:
            lot.info("reusing existing header graphic (no regeneration requested)")
            # Store in Redis under a new key so assemble_article can pick it up
            graphic_key = f"pulse:graphic:{uuid.uuid4()}"
            _cache_set(graphic_key, {"base64": existing_b64}, ttl=REDIS_ARTICLE_TTL)
            return {"graphic_key": graphic_key, "prompt_used": existing_prompt, "reused": True}
        else:
            lot.info("no existing graphic found, generating new one")

    lot.info("generating article header graphic...")

    # Use GPT-4o to create an image generation prompt from the article
    prompt_system = """You are an art director creating image prompts for LinkedIn article headers.

Given an article title and opening text, create a descriptive prompt for an AI image generator.

STYLE GUIDELINES:
- Professional, clean, modern
- Abstract or conceptual, NOT literal (no people, no screenshots, no text)
- Enterprise-appropriate, sophisticated
- Color palette: navy (#1B2A4A), teal, white, with subtle accent colors
- Think: geometric patterns, abstract data flows, architectural forms, network visualizations
- Style reference: high-end consulting firm report covers

IMPORTANT:
- No text, words, letters, or numbers in the image
- No faces or human figures
- No logos or brand marks
- Keep it abstract enough to pair with any enterprise AI article

Return ONLY the image generation prompt, nothing else."""

    prompt_user = f"""Article title: {title}

Article opening:
{article_md[:500]}

Create an image generation prompt for the header graphic."""

    try:
        image_prompt = gpt4o_text(prompt_system, prompt_user)
        lot.info("image prompt created, generating graphic...")

        b64_image = _generate_image(image_prompt)
        lot.info("graphic ready")

        # Store base64 in Redis (NOT XCom) to avoid bloating agent context
        graphic_key = f"pulse:graphic:{uuid.uuid4()}"
        _cache_set(graphic_key, {"base64": b64_image}, ttl=REDIS_ARTICLE_TTL)
        logger.info(f"Graphic stored in Redis: {graphic_key}")

        return {"graphic_key": graphic_key, "prompt_used": image_prompt}
    except Exception as e:
        logger.error(f"Graphic generation failed: {e}")
        lot.info("graphic generation failed, article will be created without header image")
        return {"graphic_key": None, "prompt_used": None, "error": str(e)}


def assemble_article(**context):
    """Assemble the final article, persist to Redis, and push to XCom."""
    set_request_id(context)
    p = context["params"]
    ti = context["ti"]

    article_md = ti.xcom_pull(task_ids="humanize_article") or ""
    graphic_data = ti.xcom_pull(task_ids="generate_graphics") or {}
    content_type = ti.xcom_pull(task_ids="load_context", key="content_type") or "blog_outline"
    title = ti.xcom_pull(task_ids="load_context", key="content_title") or "Untitled"
    existing_article = ti.xcom_pull(task_ids="load_context", key="existing_article")
    research = ti.xcom_pull(task_ids="deep_research") or {}
    report_id = ti.xcom_pull(task_ids="load_context", key="report_id") or p["report_id"]
    content_index = p.get("content_index", 0)

    # Determine article_id and version
    # Use the resolved article_id from load_context (may have been auto-resolved from index)
    resolved_id = ti.xcom_pull(task_ids="load_context", key="resolved_article_id")
    if resolved_id and existing_article:
        article_id = resolved_id
        version = existing_article.get("version", 1) + 1
    else:
        article_id = str(uuid.uuid4())
        version = 1

    timestamp = pendulum.now("Asia/Kolkata").format("YYYY-MM-DD h:mm A") + " IST"
    word_count = len(article_md.split())
    dag_version = "v0.3"

    # Retrieve graphic from Redis (stored there to avoid bloating XCom/agent context)
    graphic_key = graphic_data.get("graphic_key")
    graphic_reused = graphic_data.get("reused", False)
    graphic_b64 = None
    if graphic_key:
        graphic_stored = _cache_get(graphic_key)
        if graphic_stored:
            graphic_b64 = graphic_stored.get("base64")

    if graphic_b64:
        lot.info(f"header graphic: {'reused from previous version' if graphic_reused else 'newly generated'}")
    else:
        lot.info("no header graphic available")

    # Build final markdown with graphic reference and metadata
    final_parts = []

    if graphic_b64:
        final_parts.append(f"![Header](data:image/png;base64,{graphic_b64})")
        final_parts.append("")

    final_parts.append(article_md)
    final_parts.append("")
    final_parts.append("---")
    final_parts.append(f"*Article ID: {article_id} | Report: {report_id} | {word_count} words | Generated: {timestamp} | v{version} | Pulse Article Creator {dag_version}*")

    full_article_md = "\n".join(final_parts)

    # Extract headline from article markdown (first # heading)
    article_title = title
    for line in article_md.split("\n"):
        if line.startswith("# "):
            article_title = line[2:].strip()
            break

    # Persist to Redis
    article_data = {
        "article_id": article_id,
        "report_id": report_id,
        "content_type": content_type,
        "content_index": content_index,
        "generated_at": timestamp,
        "title": article_title,
        "article_md": full_article_md,
        "graphic_base64": graphic_b64,
        "graphic_prompt": graphic_data.get("prompt_used"),
        "research": research,
        "version": version,
    }

    redis_key = f"pulse:article:{article_id}"
    try:
        _cache_set(redis_key, article_data, ttl=REDIS_ARTICLE_TTL)
        # Write index key so future edits can auto-resolve article_id
        # even if the agent forgets to pass it
        index_key = f"pulse:article_index:{report_id}:{content_type}:{content_index}"
        r = _get_redis()
        r.setex(index_key, REDIS_ARTICLE_TTL, article_id)
        lot.info(f"article saved to redis: {redis_key}")
        logger.info(f"Article persisted to Redis key {redis_key} (7d TTL), index: {index_key}")
    except Exception as e:
        logger.warning(f"Failed to persist article to Redis: {e}")
        lot.info("warning: could not save article to redis, but article is still available")

    # Push to XCom (article markdown without embedded base64 to keep XCom payload manageable)
    # Build a display version: replace the massive base64 blob with a placeholder
    if graphic_b64:
        display_parts = []
        display_parts.append("**[Header graphic generated]**")
        display_parts.append("")
        display_parts.append(article_md)
        display_parts.append("")
        display_parts.append("---")
        display_parts.append(f"*Article ID: {article_id} | Report: {report_id} | {word_count} words | Generated: {timestamp} | v{version} | Pulse Article Creator {dag_version}*")
        display_md = "\n".join(display_parts)
    else:
        display_md = full_article_md

    ti.xcom_push(key="article_md", value=display_md)
    ti.xcom_push(key="article_id", value=article_id)

    lot.info(f"article ready: {article_title}")
    lot.info(f"article id: {article_id} (v{version}, {word_count} words, graphic={'reused' if graphic_reused else 'new' if graphic_b64 else 'none'})")
    logger.info(f"Article {article_id} v{version} assembled: {word_count} words, graphic={'reused' if graphic_reused else 'new' if graphic_b64 else 'none'}")

    return display_md


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    "pulse_article_creator",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["pulse", "content", "article"],
    description=(
        "Creates or edits a LinkedIn article from a Pulse weekly report content item. "
        "IMPORTANT: Always pass the user's message/prompt as the 'instructions' parameter. "
        "The DAG auto-detects whether to create a new article or edit an existing one. "
        "Examples: instructions='create an article on blog outline 2', "
        "instructions='expand the case studies', instructions='make it shorter'."
    ),
    params={
        "report_id": Param(
            type="string",
            description="UUID of the content report to pull source data from",
        ),
        "content_type": Param(
            default="blog_outline",
            type="string",
            enum=["blog_outline", "linkedin_post", "youtube_idea", "carousel", "reel", "founders_notebook"],
            description="Which content section to expand into an article",
        ),
        "content_index": Param(
            default=0,
            type="integer",
            description="0-based index of the item within that content section",
        ),
        "article_id": Param(
            default=None,
            type=["string", "null"],
            description="If editing an existing article, pass its UUID to load and revise it",
        ),
        "instructions": Param(
            type="string",
            description=(
                "REQUIRED. Copy-paste the user's EXACT message here, word for word. "
                "Do NOT rephrase, summarize, or substitute the article title. "
                "WRONG: 'write the article on topic X'. "
                "RIGHT: 'expand each bullet under case studies to be more descriptive'."
            ),
        ),
        "regenerate_graphic": Param(
            default=False,
            type="boolean",
            description="Set to true to generate a fresh header graphic. Default false (reuses existing graphic during edits).",
        ),
    },
) as dag:

    load = PythonOperator(task_id="load_context", python_callable=load_context)
    research = PythonOperator(task_id="deep_research", python_callable=deep_research)
    write = PythonOperator(task_id="write_article", python_callable=write_article)
    humanize = PythonOperator(task_id="humanize_article", python_callable=humanize_article)
    graphics = PythonOperator(task_id="generate_graphics", python_callable=generate_graphics)
    assemble = PythonOperator(task_id="assemble_article", python_callable=assemble_article)

    load >> research >> write >> humanize >> graphics >> assemble
