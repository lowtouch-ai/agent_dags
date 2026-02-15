from airflow.sdk import DAG, Param, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
import pendulum
import json
import logging
import os
import time
import uuid

import redis as redis_lib

from agent_dags.utils.think_logging import get_logger, set_request_id

logger = logging.getLogger(__name__)
lot = get_logger("pulse_content_engine")

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

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _get_variable(key):
    """Get an Airflow Variable, trying PULSE_ prefix first then unprefixed."""
    prefixed = f"PULSE_{key}"
    try:
        return Variable.get(prefixed)
    except Exception:
        return Variable.get(key)


REDIS_CACHE_TTL = 900  # 15 minutes
REDIS_CACHE_KEY = "pulse:youtube_cache"


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


def _cache_set(key, value, ttl=REDIS_CACHE_TTL):
    """Set data in Redis cache with TTL."""
    try:
        r = _get_redis()
        r.setex(key, ttl, json.dumps(value))
        logger.info(f"Cached {key} with {ttl}s TTL")
    except Exception as e:
        logger.warning(f"Redis cache write failed: {e}")


def build_youtube_service():
    """Create a YouTube Data API v3 client."""
    from googleapiclient.discovery import build

    api_key = _get_variable("YOUTUBE_API_KEY")
    return build("youtube", "v3", developerKey=api_key)


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


# ---------------------------------------------------------------------------
# Task callables — Layer 1: Intelligence Gathering
# ---------------------------------------------------------------------------


def discover_channels_and_videos(**context):
    """Discover top YouTube channels and videos by searching for recent content on relevant keywords."""
    set_request_id(context)
    ti = context["ti"]

    # Check Redis cache first — skip all YouTube API calls if fresh data exists
    p = context["params"]
    use_cache = p.get("use_cache", True)
    cached = _cache_get(REDIS_CACHE_KEY) if use_cache else None
    if not use_cache:
        lot.info("cache bypassed, forcing fresh YouTube API calls...")
    if cached:
        lot.info(f"using cached YouTube data ({cached.get('video_count', 0)} videos, {cached.get('channel_count', 0)} channels)")
        ti.xcom_push(key="videos", value=cached["videos"])
        return cached["channels"]

    lot.info("discovering top YouTube content from the past week...")

    youtube = build_youtube_service()
    published_after = pendulum.now("UTC").subtract(days=7).isoformat()

    # Search for top videos across relevant keywords (sorted by view count)
    search_queries = [
        "agentic AI",
        "AI agents enterprise",
        "building AI agents",
        "AI automation workflow",
        "LangChain AI agents",
        "CrewAI AutoGen agents",
        "Claude AI coding",
        "enterprise AI strategy",
        "AI governance compliance",
        "multi-agent orchestration",
        "no-code AI platform",
        "AI agents production deployment",
        "Airflow AI orchestration",
        "AI copilot enterprise",
        "RAG retrieval augmented generation",
    ]

    # Collect video IDs from search results
    video_ids_seen = set()
    all_video_ids = []

    for query in search_queries:
        try:
            search_response = youtube.search().list(
                q=query,
                type="video",
                part="id",
                order="viewCount",
                publishedAfter=published_after,
                maxResults=15,
                relevanceLanguage="en",
            ).execute()

            for item in search_response.get("items", []):
                vid = item["id"]["videoId"]
                if vid not in video_ids_seen:
                    video_ids_seen.add(vid)
                    all_video_ids.append(vid)
        except Exception as e:
            logger.warning(f"Video search failed for '{query}': {e}")

    lot.info(f"found {len(all_video_ids)} unique videos across {len(search_queries)} keyword searches, fetching details...")

    # Fetch full video details in batches of 50
    videos = []
    for i in range(0, len(all_video_ids), 50):
        batch = all_video_ids[i : i + 50]
        try:
            video_response = youtube.videos().list(
                id=",".join(batch),
                part="snippet,statistics",
            ).execute()

            for item in video_response.get("items", []):
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})
                videos.append({
                    "video_id": item["id"],
                    "title": snippet.get("title", ""),
                    "channel_id": snippet.get("channelId", ""),
                    "channel_title": snippet.get("channelTitle", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "tags": snippet.get("tags", []),
                })
        except Exception as e:
            logger.warning(f"Video details fetch failed for batch: {e}")

    # Sort videos by view count descending
    videos.sort(key=lambda v: v["view_count"], reverse=True)

    # Extract unique channels from discovered videos
    channel_map = {}
    channel_views = {}  # track total views per channel this week
    for v in videos:
        cid = v.get("channel_id", "")
        if not cid:
            continue
        if cid not in channel_map:
            channel_map[cid] = {
                "channel_id": cid,
                "title": v["channel_title"],
                "subscriber_count": 0,
                "video_count": 0,
                "weekly_views": 0,
            }
            channel_views[cid] = 0
        channel_views[cid] += v["view_count"]

    # Set weekly_views from video data first (before API call, so it's always populated)
    for cid in channel_map:
        channel_map[cid]["weekly_views"] = channel_views.get(cid, 0)

    lot.info(f"extracted {len(channel_map)} unique channels, fetching channel statistics...")

    # Fetch channel stats in batches of 50 (enriches with subscriber_count)
    channel_ids = list(channel_map.keys())
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        try:
            stats_response = youtube.channels().list(
                id=",".join(batch),
                part="statistics,snippet",
            ).execute()

            for item in stats_response.get("items", []):
                cid = item["id"]
                stats = item.get("statistics", {})
                if item.get("snippet", {}).get("title"):
                    channel_map[cid]["title"] = item["snippet"]["title"]
                channel_map[cid]["subscriber_count"] = int(stats.get("subscriberCount", 0))
                channel_map[cid]["video_count"] = int(stats.get("videoCount", 0))
        except Exception as e:
            logger.warning(f"Channel stats fetch failed for batch: {e}")
            lot.info(f"warning: could not fetch stats for {len(batch)} channels, subscriber counts may show 0")

    # Sort channels by total weekly views from discovered videos, take top 30
    channels = sorted(channel_map.values(), key=lambda c: c.get("weekly_views", 0), reverse=True)
    channels = channels[:30]

    channel_count = len(channels)
    video_count = len(videos)
    top_channel = channels[0]["title"] if channels else "none"
    lot.info(f"discovery complete: {video_count} videos from {channel_count} channels (top: {top_channel})")
    logger.info(f"Discovered {video_count} videos from {channel_count} channels")

    # Cache results in Redis for 15 minutes
    _cache_set(REDIS_CACHE_KEY, {
        "channels": channels,
        "videos": videos,
        "channel_count": channel_count,
        "video_count": video_count,
    })

    # Push videos to XCom separately so downstream tasks can access them
    ti.xcom_push(key="videos", value=videos)

    return channels


# ---------------------------------------------------------------------------
# Task callables — Layer 2: Analysis
# ---------------------------------------------------------------------------


def analyze_trends(**context):
    """Analyze trends across fetched video data using GPT-4o."""
    set_request_id(context)
    ti = context["ti"]
    videos = ti.xcom_pull(task_ids="discover_channels_and_videos", key="videos") or []

    if not videos:
        logger.warning("No video data available for trend analysis")
        lot.info("no video data available, generating baseline analysis")

    lot.info(f"analyzing trends across {len(videos)} videos...")

    # Prepare video summary for the LLM
    video_summary = []
    for v in videos[:100]:  # Cap at 100 to stay within context limits
        video_summary.append(
            f"- \"{v['title']}\" by {v['channel_title']} | "
            f"views: {v['view_count']:,} | likes: {v['like_count']:,} | "
            f"comments: {v['comment_count']:,}"
        )

    video_text = "\n".join(video_summary) if video_summary else "No video data available this week."

    system_msg = f"""You are an enterprise AI market analyst for lowtouch.ai, a private, no-code Agentic AI platform.

{POSITIONING_PILLARS}

Analyze the following YouTube video data from the past week in the enterprise agentic AI space.
Produce a structured analysis with:

1. "keywords" - array of top 15-20 trending keywords/phrases with frequency counts
2. "themes" - array of 5-8 theme clusters, each with a name, description, related keywords, and trend_direction (rising/stable/declining)
3. "pillar_mapping" - object mapping each pillar name to relevant themes and content opportunities
4. "gaps" - array of 3-5 high-interest topics where lowtouch.ai has a strong POV but the market content is thin
5. "top_patterns" - array of 5 top-performing content patterns (what types of titles, formats, angles get the most engagement)
6. "competitor_signals" - array of notable competitor activity observed in the data (Microsoft Copilot, ServiceNow, UiPath, CrewAI, LangChain, AutoGen)

Return valid JSON only."""

    user_msg = f"Here are the top YouTube videos from the past week in enterprise agentic AI:\n\n{video_text}"

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Trend analysis failed: {e}")
        result = {"keywords": [], "themes": [], "pillar_mapping": {}, "gaps": [], "top_patterns": [], "competitor_signals": []}

    keyword_count = len(result.get("keywords", []))
    theme_count = len(result.get("themes", []))
    lot.info(f"identified {keyword_count} trending keywords and {theme_count} themes")
    logger.info(f"Trend analysis complete: {keyword_count} keywords, {theme_count} themes")
    return result


# ---------------------------------------------------------------------------
# Task callables — Layer 3: Content Drafting (all run in parallel)
# ---------------------------------------------------------------------------


def _build_video_context(ti):
    """Build top videos and channels summary text for content drafting prompts."""
    videos = ti.xcom_pull(task_ids="discover_channels_and_videos", key="videos") or []
    channels = ti.xcom_pull(task_ids="discover_channels_and_videos") or []

    top_vids = sorted(videos, key=lambda v: v.get("view_count", 0), reverse=True)[:15]
    vid_lines = []
    for v in top_vids:
        tags = ", ".join(v.get("tags", [])[:5])
        url = f"https://youtube.com/watch?v={v['video_id']}"
        vid_lines.append(
            f"- \"{v['title']}\" by {v['channel_title']} "
            f"({v['view_count']:,} views, {v['like_count']:,} likes, {v['comment_count']:,} comments) "
            f"URL: {url} [tags: {tags}]"
        )
    vid_text = "\n".join(vid_lines) if vid_lines else "No video data available."

    top_chs = sorted(channels, key=lambda c: c.get("weekly_views", 0), reverse=True)[:10]
    ch_lines = [f"- {c['title']} ({c.get('subscriber_count', 0):,} subs, {c.get('weekly_views', 0):,} weekly views)" for c in top_chs]
    ch_text = "\n".join(ch_lines) if ch_lines else "No channel data available."

    return vid_text, ch_text


def draft_youtube_ideas(**context):
    """Draft 3 YouTube video ideas based on trend analysis and top-performing videos."""
    set_request_id(context)
    lot.info("drafting YouTube video ideas...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}
    vid_text, ch_text = _build_video_context(ti)

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO.

{POSITIONING_PILLARS}
{VOICE_RULES}

YouTube-specific rules:
- First 30 seconds must state what the viewer will learn and why it matters
- Target length: 8-15 minutes
- Content mix: 1 Trend Explainer, 1 Build/Demo Walkthrough, 1 Market Commentary/POV
- Titles should be search-friendly, specific, not clickbait

IMPORTANT: Your ideas must be directly inspired by the top-performing videos below. Study what titles, angles, and formats are getting the most views and engagement this week. Then create ideas that:
1. Ride the same trending topics but from lowtouch.ai's unique angle (private, no-code, production-ready)
2. Respond to, build on, or offer a contrarian take on the most popular videos
3. Use similar title patterns and keywords that are proven to attract views this week
4. Reference specific trends, tools, or debates visible in the top videos

Do NOT generate generic AI content ideas. Every idea must trace back to a specific trend or pattern you observed in the data.

Generate 3 YouTube video ideas as a JSON object with key "ideas" containing an array of 3 objects.
Each object must have: title, type (Trend Explainer/Build-Demo Walkthrough/Market Commentary),
target_length, hook_first_30_seconds, outline (array of 4-6 segment objects with segment_title and talking_points),
inspired_by (array of 1-2 objects, each with "title" and "url" of the specific top video that inspired this idea),
trend_tie_in, pillar, visual_demo_notes, cta, seo_keywords (array of 5-8).

Return valid JSON only."""

    user_msg = f"""TOP 15 PERFORMING VIDEOS THIS WEEK:
{vid_text}

TOP 10 CHANNELS BY WEEKLY VIEWS:
{ch_text}

TREND ANALYSIS:
{json.dumps(trends, indent=2)}"""

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"YouTube ideas drafting failed: {e}")
        result = {"ideas": []}

    count = len(result.get("ideas", []))
    lot.info(f"drafted {count} YouTube video ideas")
    return result


def draft_linkedin_posts(**context):
    """Draft 2-3 LinkedIn text posts based on trend analysis and top-performing videos."""
    set_request_id(context)
    lot.info("drafting LinkedIn posts...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}
    vid_text, ch_text = _build_video_context(ti)

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO. Write in his voice as a builder/practitioner.

{POSITIONING_PILLARS}
{VOICE_RULES}

LinkedIn-specific rules:
- No external links in post body (kills reach ~30%). Links go in first comment.
- No emoji in post body.
- Hook (first 2 lines) must be specific and attention-grabbing. No generic "AI is changing everything" openings.
- Body: 3-6 short paragraphs. Practical, opinionated, grounded in real enterprise context.
- CTA: question or invitation to comment, never a hard sell.
- Content mix: 1 Trend Commentary, 1 Contrarian/POV, optionally 1 Industry Signal

IMPORTANT: Your posts must be directly inspired by the top-performing YouTube videos below. These videos show what topics, debates, and angles are resonating with the enterprise AI audience right now. Use them to:
1. Comment on or extend the conversation these videos started
2. Offer lowtouch.ai's practitioner perspective on the same trending topics
3. Take a contrarian stance on popular narratives visible in the top content

Do NOT generate generic AI thought leadership. Every post must trace back to a specific trend or talking point visible in this week's data.

Generate LinkedIn posts as a JSON object with key "posts" containing an array of 2-3 objects.
Each object must have: type (Trend Commentary/Contrarian POV/Industry Signal),
hook (first 2 lines as string), body (full post text as string), cta (string),
hashtags (array of 3-5 niche hashtags like #AgenticAI #EnterpriseAI),
inspired_by (array of 1-2 objects, each with "title" and "url" of the specific top video that inspired this post),
posting_slot (suggested day and time IST), trend_tie_in, pillar.

Return valid JSON only."""

    user_msg = f"""TOP 15 PERFORMING VIDEOS THIS WEEK:
{vid_text}

TOP 10 CHANNELS BY WEEKLY VIEWS:
{ch_text}

TREND ANALYSIS:
{json.dumps(trends, indent=2)}"""

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"LinkedIn posts drafting failed: {e}")
        result = {"posts": []}

    count = len(result.get("posts", []))
    lot.info(f"drafted {count} LinkedIn posts")
    return result


def draft_linkedin_carousels(**context):
    """Draft 2 LinkedIn carousel outlines based on trend analysis."""
    set_request_id(context)
    lot.info("drafting LinkedIn carousels...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO.

{POSITIONING_PILLARS}
{VOICE_RULES}

LinkedIn carousel rules:
- First slide must work as a standalone hook in the feed
- 8-12 slides per carousel
- Content mix: 1 Framework/How-To, 1 Insight Breakdown
- Each slide: heading + 1-2 sentences of body text

Generate 2 LinkedIn carousels as a JSON object with key "carousels" containing an array of 2 objects.
Each object must have: type (Framework How-To/Insight Breakdown), title_slide (string),
slides (array of 8-12 objects with slide_number, heading, body_text),
visual_direction (color scheme, icons, diagrams), final_slide_cta (string),
caption (3-5 sentences with hashtags as string), posting_slot, trend_tie_in, pillar.

Return valid JSON only."""

    user_msg = f"Trend analysis for this week:\n{json.dumps(trends, indent=2)}"

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"LinkedIn carousels drafting failed: {e}")
        result = {"carousels": []}

    count = len(result.get("carousels", []))
    lot.info(f"drafted {count} LinkedIn carousels")
    return result


def draft_reel_scripts(**context):
    """Draft 3 Instagram Reel scripts based on trend analysis and top-performing videos."""
    set_request_id(context)
    lot.info("drafting Instagram Reel scripts...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}
    vid_text, ch_text = _build_video_context(ti)

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO. He is on camera (talking head) for all reels.

{POSITIONING_PILLARS}
{VOICE_RULES}

Instagram Reel rules:
- Duration: 30-60 seconds each
- Hook must land in first 1.5 seconds. Open with a surprising fact, a common misconception, or a "did you know" angle.
- Every reel must teach the viewer ONE clear, specific takeaway they can remember or act on.
- Structure: Hook (problem or question) -> Teach (explain the concept, show the "how" or "why") -> Takeaway (one sentence the viewer walks away with).
- Teleprompter format: no line longer than 10 words
- Each spoken segment on its own line
- Conversational, direct, confident. No corporate speak. Talk like you are explaining to a smart colleague over coffee.
- One idea per reel, no more. Go deep, not wide.
- Content mix: 1 "Here's how X actually works" explainer, 1 "Most people get this wrong" myth buster, 1 "What I learned building X" practitioner insight
- Avoid vague advice ("AI is important"). Be specific ("Here's why 80% of AI agents fail in production: they don't have a human-in-the-loop checkpoint before taking action").

IMPORTANT: Your reel ideas must be directly inspired by the top-performing YouTube videos below. These videos show what topics are trending with the enterprise AI audience right now. Distill one specific, teachable insight from a trending topic into each reel. The viewer should walk away knowing something concrete they did not know before.

Do NOT generate generic AI reels. Every reel must trace back to a specific trend or talking point visible in this week's data.

Generate 3 Reel scripts as a JSON object with key "reels" containing an array of 3 objects.
Each object must have: type (Explainer/Myth Buster/Practitioner Insight),
teaching_point (one sentence: what the viewer will learn),
duration_seconds (30-60), hook (object with time "0-3 sec", text_overlay, spoken_line),
body (object with time "3-45 sec", spoken_lines as array of strings each max 10 words, teaching_steps as array of 2-3 key points being taught),
close (object with time "45-60 sec", spoken_line, text_overlay, viewer_takeaway),
inspired_by (array of 1-2 objects, each with "title" and "url" of the specific top video that inspired this reel),
visual_direction, audio_note, hashtags (array of 3-5), trend_tie_in, pillar.

Return valid JSON only."""

    user_msg = f"""TOP 15 PERFORMING VIDEOS THIS WEEK:
{vid_text}

TOP 10 CHANNELS BY WEEKLY VIEWS:
{ch_text}

TREND ANALYSIS:
{json.dumps(trends, indent=2)}"""

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Reel scripts drafting failed: {e}")
        result = {"reels": []}

    count = len(result.get("reels", []))
    lot.info(f"drafted {count} Instagram Reel scripts")
    return result


def draft_blog_outlines(**context):
    """Draft 2 blog/article outlines based on trend analysis."""
    set_request_id(context)
    lot.info("drafting blog outlines...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO.

{POSITIONING_PILLARS}
{VOICE_RULES}

Blog/article outline rules:
- Title: clear, professional, positions Rejith as a practitioner
- Target a specific persona (CIO, CTO, CISO, etc.)
- Include thesis, 4-6 sections, key data points, lowtouch.ai angle
- Not a product pitch. Connect to the platform through genuine insight.

Generate 2 blog outlines as a JSON object with key "outlines" containing an array of 2 objects.
Each object must have: title, target_audience, thesis (one sentence),
sections (array of 4-6 objects with heading and description),
key_data_points (array of stats/references), lowtouch_angle (string),
distribution (LinkedIn article/company blog/both), seo_keywords (array of 5-8),
trend_tie_in, pillar.

Return valid JSON only."""

    user_msg = f"Trend analysis for this week:\n{json.dumps(trends, indent=2)}"

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Blog outlines drafting failed: {e}")
        result = {"outlines": []}

    count = len(result.get("outlines", []))
    lot.info(f"drafted {count} blog outlines")
    return result


def draft_engagement_targets(**context):
    """Generate 10-15 suggested engagement targets based on trend data."""
    set_request_id(context)
    lot.info("identifying engagement targets...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO.

{POSITIONING_PILLARS}

Generate strategic LinkedIn engagement targets. These are AI-generated suggestions for the types of
posts Rejith should comment on this week, based on trending topics.

Target persona categories:
- CIOs and CTOs evaluating or deploying AI
- AI/ML leaders at Fortune 500 companies
- Analyst firms (Gartner, Forrester analysts covering AI)
- Competitor executives (engage respectfully, position through contrast)
- Open-source community leaders (LangChain, CrewAI, AutoGen ecosystem)
- Enterprise tech media and journalists

Each comment should add value, demonstrate expertise, or offer a complementary perspective.
Not "great post" comments. 2-3 substantive sentences.

Generate as a JSON object with key "targets" containing an array of 10-15 objects.
Each object must have: topic (what the post is about), author_type (persona category),
post_theme (what they likely posted), suggested_comment (2-3 sentence comment),
priority (High/Medium), pillar_connection.

Return valid JSON only."""

    user_msg = f"This week's trending topics and themes:\n{json.dumps(trends, indent=2)}"

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Engagement targets drafting failed: {e}")
        result = {"targets": []}

    count = len(result.get("targets", []))
    lot.info(f"identified {count} engagement targets")
    return result


def draft_founders_notebook(**context):
    """Generate a Founder's Notebook prompt connecting trends to Rejith's experience."""
    set_request_id(context)
    lot.info("drafting Founder's Notebook prompt...")
    ti = context["ti"]
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}

    system_msg = f"""You are a content strategist for lowtouch.ai, a private, no-code Agentic AI platform.
Owner: Rejith Krishnan, Founder/CEO.

{POSITIONING_PILLARS}

Generate a Founder's Notebook prompt. This is a weekly personal writing prompt for Rejith that
connects a trending topic to his experience building lowtouch.ai. The goal is to inspire a
genuine, personal LinkedIn post that cannot be fully automated because it requires real experience.

Generate as a JSON object with keys:
- "trending_topic" (string): the topic from this week's trends
- "prompt" (string): a specific question or scenario tying the trend to Rejith's experience
- "example_angles" (array of 2-3 strings): starter directions Rejith could take
- "pillar_connection" (string): which positioning pillar this reinforces
- "series_name" (string): "Friday Field Notes"
- "posting_slot" (string): "Friday 10:00 AM IST"

Return valid JSON only."""

    user_msg = f"This week's trending topics and themes:\n{json.dumps(trends, indent=2)}"

    try:
        result = gpt4o_json(system_msg, user_msg)
    except Exception as e:
        logger.error(f"Founder's Notebook drafting failed: {e}")
        result = {"trending_topic": "", "prompt": "", "example_angles": [], "pillar_connection": "", "series_name": "Friday Field Notes", "posting_slot": "Friday 10:00 AM IST"}

    lot.info("Founder's Notebook prompt ready")
    return result


# ---------------------------------------------------------------------------
# Task callables — Post-drafting
# ---------------------------------------------------------------------------


def generate_posting_calendar(**context):
    """Slot all drafted content into a day-by-day posting calendar."""
    set_request_id(context)
    lot.info("generating posting calendar...")
    ti = context["ti"]

    youtube_ideas = ti.xcom_pull(task_ids="content_drafting.draft_youtube_ideas") or {}
    linkedin_posts = ti.xcom_pull(task_ids="content_drafting.draft_linkedin_posts") or {}
    carousels = ti.xcom_pull(task_ids="content_drafting.draft_linkedin_carousels") or {}
    reels = ti.xcom_pull(task_ids="content_drafting.draft_reel_scripts") or {}
    founders = ti.xcom_pull(task_ids="content_drafting.draft_founders_notebook") or {}

    # Build calendar
    posts = linkedin_posts.get("posts", [])
    carousel_list = carousels.get("carousels", [])
    reel_list = reels.get("reels", [])
    yt_ideas = youtube_ideas.get("ideas", [])

    calendar = {
        "week_of": pendulum.now("Asia/Kolkata").start_of("week").add(days=1).format("YYYY-MM-DD"),
        "schedule": [
            {
                "day": "Tuesday",
                "slots": [
                    {"time": "9:00-9:10 AM IST", "channel": "LinkedIn", "type": "Strategic Commenting", "content": "4-5 comments from engagement targets"},
                    {"time": "10:00 AM IST", "channel": "LinkedIn", "type": "Carousel", "content": carousel_list[0].get("title_slide", "Carousel #1") if carousel_list else "Carousel #1"},
                ],
            },
            {
                "day": "Wednesday",
                "slots": [
                    {"time": "9:00-9:10 AM IST", "channel": "LinkedIn", "type": "Strategic Commenting", "content": "4-5 comments from engagement targets"},
                    {"time": "10:00 AM IST", "channel": "LinkedIn", "type": "Text Post", "content": posts[0].get("hook", "Text post #1")[:80] if posts else "Text post #1"},
                    {"time": "12:30 PM IST", "channel": "Instagram", "type": "Reel", "content": reel_list[0].get("type", "Reel #1") if reel_list else "Reel #1"},
                ],
            },
            {
                "day": "Thursday",
                "slots": [
                    {"time": "9:00-9:10 AM IST", "channel": "LinkedIn", "type": "Strategic Commenting", "content": "4-5 comments from engagement targets"},
                    {"time": "10:00 AM IST", "channel": "LinkedIn", "type": "Carousel or Text Post", "content": (carousel_list[1].get("title_slide", "Carousel #2") if len(carousel_list) > 1 else posts[1].get("hook", "Text post #2")[:80] if len(posts) > 1 else "Content #2")},
                    {"time": "12:30 PM IST", "channel": "Instagram", "type": "Reel", "content": reel_list[1].get("type", "Reel #2") if len(reel_list) > 1 else "Reel #2"},
                ],
            },
            {
                "day": "Friday",
                "slots": [
                    {"time": "9:00-9:10 AM IST", "channel": "LinkedIn", "type": "Strategic Commenting", "content": "4-5 comments from engagement targets"},
                    {"time": "10:00 AM IST", "channel": "LinkedIn", "type": "Friday Field Notes", "content": founders.get("trending_topic", "Founder's Notebook")},
                    {"time": "12:30 PM IST", "channel": "Instagram", "type": "Reel", "content": reel_list[2].get("type", "Reel #3") if len(reel_list) > 2 else "Reel #3"},
                ],
            },
            {
                "day": "Weekend",
                "slots": [
                    {"time": "Flexible", "channel": "YouTube", "type": "Video", "content": yt_ideas[0].get("title", "YouTube video") if yt_ideas else "YouTube video (if produced)"},
                ],
            },
        ],
    }

    lot.info("posting calendar ready")
    logger.info("Posting calendar generated")
    return calendar


# ---------------------------------------------------------------------------
# Task callable — Final Assembly
# ---------------------------------------------------------------------------


def _fmt(n):
    """Format number with commas."""
    return f"{n:,}" if isinstance(n, int) else str(n)


def _esc(s):
    """Remove pipe characters that break markdown table cells."""
    return str(s).replace("|", " / ").replace("\n", " ")


def _trunc(s, max_len=60):
    """Truncate string with ellipsis and escape for table use."""
    s = _esc(s)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def _kw(tags, limit=5):
    """Format keyword tags, escaped for table use."""
    if not tags:
        return "-"
    return _esc(", ".join(str(t) for t in tags[:limit]))


def _age(published_at):
    """Return human-readable age like '2d', '5h' from an ISO date string."""
    if not published_at:
        return "-"
    try:
        pub = pendulum.parse(published_at)
        diff = pendulum.now("UTC") - pub
        days = diff.days
        if days >= 1:
            return f"{days}d"
        hours = diff.seconds // 3600
        return f"{hours}h" if hours > 0 else "<1h"
    except Exception:
        return "-"


def _render_markdown(channels, videos, trends, youtube_ideas, linkedin_posts,
                     carousels, reels, blog_outlines, engagement, founders, calendar,
                     report_id=None):
    """Render the full report as markdown."""
    md = []
    week_of = calendar.get("week_of", pendulum.now("Asia/Kolkata").format("YYYY-MM-DD"))

    # --- Header ---
    md.append(f"# Weekly Content Report for lowtouch.ai")
    md.append(f"**Week of:** {week_of}  ")
    md.append(f"**Generated:** {pendulum.now('Asia/Kolkata').format('YYYY-MM-DD h:mm A')} IST  ")
    if report_id:
        md.append(f"**Report ID:** `{report_id}`  ")
    md.append("")

    # --- YouTube Intelligence ---
    md.append("---")
    md.append("## YouTube Intelligence")
    md.append(f"**Data snapshot:** {_fmt(len(channels))} channels analyzed, {_fmt(len(videos))} videos discovered (past 7 days)")
    md.append("")

    # Top 10 channels by weekly views
    channel_top_video = {}
    for v in videos:
        ct = v.get("channel_title", "")
        if ct not in channel_top_video or v.get("view_count", 0) > channel_top_video[ct].get("view_count", 0):
            channel_top_video[ct] = v

    top_channels = sorted(channels, key=lambda c: c.get("weekly_views", 0), reverse=True)[:10]

    md.append("### Top 10 Channels by Weekly Views")
    md.append("")
    md.append("| # | Channel | Subscribers | Weekly Views | Top Video This Week | Views | Age | Keywords |")
    md.append("|---|---------|-------------|--------------|---------------------|-------|-----|----------|")
    for i, c in enumerate(top_channels):
        ch_name = _esc(c["title"])
        ch_url = f"https://youtube.com/channel/{c['channel_id']}"
        subs = _fmt(c.get("subscriber_count", 0))
        wv = _fmt(c.get("weekly_views", 0))
        tv = channel_top_video.get(c["title"])
        if tv:
            tv_title = _trunc(tv["title"], 45)
            tv_url = f"https://youtube.com/watch?v={tv['video_id']}"
            tv_views = _fmt(tv.get("view_count", 0))
            tv_age = _age(tv.get("published_at", ""))
            tv_kw = _kw(tv.get("tags", []), 3)
            md.append(f"| {i+1} | [{ch_name}]({ch_url}) | {subs} | {wv} | [{tv_title}]({tv_url}) | {tv_views} | {tv_age} | {tv_kw} |")
        else:
            md.append(f"| {i+1} | [{ch_name}]({ch_url}) | {subs} | {wv} | - | - | - | - |")
    md.append("")

    # Top 10 videos
    top_videos = sorted(videos, key=lambda v: v.get("view_count", 0), reverse=True)[:10]

    md.append("### Top 10 Videos (Past 7 Days)")
    md.append("")
    md.append("| # | Video | Channel | Views | Likes | Comments | Age | Keywords |")
    md.append("|---|-------|---------|-------|-------|----------|-----|----------|")
    for i, v in enumerate(top_videos):
        title = _trunc(v["title"], 45)
        url = f"https://youtube.com/watch?v={v['video_id']}"
        ch = _esc(v.get("channel_title", ""))
        age = _age(v.get("published_at", ""))
        md.append(f"| {i+1} | [{title}]({url}) | {ch} | {_fmt(v['view_count'])} | {_fmt(v['like_count'])} | {_fmt(v['comment_count'])} | {age} | {_kw(v.get('tags', []), 3)} |")
    md.append("")

    # --- Trend Analysis ---
    md.append("---")
    md.append("## Trend Analysis")
    md.append("")

    keywords = trends.get("keywords", [])
    if keywords:
        md.append("### Trending Keywords")
        kw_list = []
        for k in keywords[:15]:
            if isinstance(k, dict):
                # Try common key names for the keyword text
                text = (k.get("keyword") or k.get("name") or k.get("phrase")
                        or k.get("term") or k.get("text") or "")
                # If still empty, grab the first string value from the dict
                if not text:
                    text = next((v for v in k.values() if isinstance(v, str)), "")
                # Try common key names for the count
                count = (k.get("count") or k.get("frequency") or k.get("mentions")
                         or k.get("occurrences") or "")
                if not count:
                    count = next((v for v in k.values() if isinstance(v, (int, float))), "")
                kw_list.append(f"**{text}** ({count})" if count else f"**{text}**")
            else:
                kw_list.append(f"**{k}**")
        md.append(", ".join(kw_list))
        md.append("")

    themes = trends.get("themes", [])
    if themes:
        md.append("### Theme Clusters")
        for t in themes:
            name = t.get("name", "") if isinstance(t, dict) else str(t)
            desc = t.get("description", "") if isinstance(t, dict) else ""
            direction = t.get("trend_direction", "") if isinstance(t, dict) else ""
            related = ", ".join(t.get("related_keywords", [])) if isinstance(t, dict) else ""
            arrow = {"rising": "^", "stable": "-", "declining": "v"}.get(direction, "")
            md.append(f"- **{name}** {f'({arrow} {direction})' if direction else ''}: {desc}")
            if related:
                md.append(f"  Keywords: {related}")
        md.append("")

    gaps = trends.get("gaps", [])
    if gaps:
        md.append("### Content Gaps")
        for g in gaps:
            if isinstance(g, dict):
                md.append(f"- {g.get('topic', g.get('name', str(g)))}: {g.get('description', g.get('opportunity', ''))}")
            else:
                md.append(f"- {g}")
        md.append("")

    pillar_map = trends.get("pillar_mapping", {})
    if pillar_map:
        md.append("### Pillar Mapping")
        for pillar, data in pillar_map.items():
            if isinstance(data, dict):
                themes_list = ", ".join(data.get("themes", data.get("relevant_themes", [])))
                md.append(f"- **{pillar}**: {themes_list}")
            elif isinstance(data, list):
                md.append(f"- **{pillar}**: {', '.join(str(d) for d in data)}")
            else:
                md.append(f"- **{pillar}**: {data}")
        md.append("")

    # --- YouTube Ideas ---
    yt_ideas = youtube_ideas.get("ideas", [])
    if yt_ideas:
        md.append("---")
        md.append("## YouTube Video Ideas")
        md.append("")
        for idx, idea in enumerate(yt_ideas, 1):
            md.append(f"### {idx}. {idea.get('title', 'Untitled')}")
            md.append(f"**Type:** {idea.get('type', '')} | **Length:** {idea.get('target_length', '8-15 min')} | **Pillar:** {idea.get('pillar', '')}")
            md.append("")
            md.append(f"**Hook (first 30 seconds):**")
            md.append(f"> {idea.get('hook_first_30_seconds', idea.get('hook', ''))}")
            md.append("")
            outline = idea.get("outline", [])
            if outline:
                md.append("**Outline:**")
                for seg in outline:
                    if isinstance(seg, dict):
                        md.append(f"1. **{seg.get('segment_title', '')}** - {', '.join(seg.get('talking_points', [])) if isinstance(seg.get('talking_points'), list) else seg.get('talking_points', '')}")
                    else:
                        md.append(f"1. {seg}")
                md.append("")
            if idea.get("visual_demo_notes"):
                md.append(f"**Visual/Demo notes:** {idea['visual_demo_notes']}")
            if idea.get("cta"):
                md.append(f"**CTA:** {idea['cta']}")
            seo = idea.get("seo_keywords", [])
            if seo:
                md.append(f"**SEO Keywords:** {', '.join(seo)}")
            inspired = idea.get("inspired_by", [])
            if inspired:
                if isinstance(inspired, list):
                    links = [f"[{_esc(ref.get('title', 'video'))}]({ref.get('url', '')})" if isinstance(ref, dict) else str(ref) for ref in inspired]
                    md.append(f"**Inspired by:** {', '.join(links)}")
                else:
                    md.append(f"**Inspired by:** {inspired}")
            if idea.get("trend_tie_in"):
                md.append(f"**Trend tie-in:** {idea['trend_tie_in']}")
            md.append("")

    # --- LinkedIn Posts ---
    li_posts = linkedin_posts.get("posts", [])
    if li_posts:
        md.append("---")
        md.append("## LinkedIn Text Posts")
        md.append("")
        for idx, post in enumerate(li_posts, 1):
            md.append(f"### Post {idx}: {post.get('type', '')}")
            md.append(f"**Posting slot:** {post.get('posting_slot', '')} | **Pillar:** {post.get('pillar', '')}")
            md.append("")
            md.append(f"**Hook:**")
            md.append(f"> {post.get('hook', '')}")
            md.append("")
            md.append(f"{post.get('body', '')}")
            md.append("")
            if post.get("cta"):
                md.append(f"**CTA:** {post['cta']}")
            tags = post.get("hashtags", [])
            if tags:
                md.append(f"**Hashtags:** {' '.join(tags)}")
            inspired = post.get("inspired_by", [])
            if inspired:
                if isinstance(inspired, list):
                    links = [f"[{_esc(ref.get('title', 'video'))}]({ref.get('url', '')})" if isinstance(ref, dict) else str(ref) for ref in inspired]
                    md.append(f"**Inspired by:** {', '.join(links)}")
                else:
                    md.append(f"**Inspired by:** {inspired}")
            md.append("")

    # --- LinkedIn Carousels ---
    car_list = carousels.get("carousels", [])
    if car_list:
        md.append("---")
        md.append("## LinkedIn Carousels")
        md.append("")
        for idx, car in enumerate(car_list, 1):
            md.append(f"### Carousel {idx}: {car.get('type', '')}")
            md.append(f"**Title slide:** {car.get('title_slide', '')} | **Pillar:** {car.get('pillar', '')}")
            md.append("")
            slides = car.get("slides", [])
            for s in slides:
                if isinstance(s, dict):
                    md.append(f"**Slide {s.get('slide_number', '')}:** {s.get('heading', '')}  ")
                    md.append(f"{s.get('body_text', '')}")
                    md.append("")
            if car.get("visual_direction"):
                md.append(f"**Visual direction:** {car['visual_direction']}")
            if car.get("final_slide_cta"):
                md.append(f"**Final slide CTA:** {car['final_slide_cta']}")
            if car.get("caption"):
                md.append(f"**Caption:** {car['caption']}")
            md.append("")

    # --- Instagram Reels ---
    reel_list = reels.get("reels", [])
    if reel_list:
        md.append("---")
        md.append("## Instagram Reel Scripts")
        md.append("")
        for idx, reel in enumerate(reel_list, 1):
            md.append(f"### Reel {idx}: {reel.get('type', '')}")
            md.append(f"**Duration:** {reel.get('duration_seconds', '30-60')}s | **Pillar:** {reel.get('pillar', '')}")
            if reel.get("teaching_point"):
                md.append(f"**Viewer learns:** {reel['teaching_point']}")
            md.append("")
            hook = reel.get("hook", {})
            if isinstance(hook, dict):
                md.append(f"**Hook ({hook.get('time', '0-3 sec')}):**  ")
                md.append(f"Text overlay: {hook.get('text_overlay', '')}  ")
                md.append(f"Spoken: {hook.get('spoken_line', '')}")
            md.append("")
            body = reel.get("body", {})
            if isinstance(body, dict):
                md.append(f"**Body ({body.get('time', '3-45 sec')}):**")
                teaching_steps = body.get("teaching_steps", [])
                if teaching_steps:
                    for step in teaching_steps:
                        md.append(f"- {step}")
                    md.append("")
                md.append("*Teleprompter:*")
                for line in body.get("spoken_lines", []):
                    md.append(f"> {line}")
            md.append("")
            close = reel.get("close", {})
            if isinstance(close, dict):
                md.append(f"**Close ({close.get('time', '45-60 sec')}):**  ")
                md.append(f"Spoken: {close.get('spoken_line', '')}  ")
                md.append(f"Text overlay: {close.get('text_overlay', '')}")
                if close.get("viewer_takeaway"):
                    md.append(f"**Takeaway:** {close['viewer_takeaway']}")
            md.append("")
            if reel.get("visual_direction"):
                md.append(f"**Visual:** {reel['visual_direction']}")
            tags = reel.get("hashtags", [])
            if tags:
                md.append(f"**Hashtags:** {' '.join(tags)}")
            inspired = reel.get("inspired_by", [])
            if inspired:
                if isinstance(inspired, list):
                    links = [f"[{_esc(ref.get('title', 'video'))}]({ref.get('url', '')})" if isinstance(ref, dict) else str(ref) for ref in inspired]
                    md.append(f"**Inspired by:** {', '.join(links)}")
                else:
                    md.append(f"**Inspired by:** {inspired}")
            md.append("")

    # --- Blog Outlines ---
    outlines = blog_outlines.get("outlines", [])
    if outlines:
        md.append("---")
        md.append("## Blog/Article Outlines")
        md.append("")
        for idx, o in enumerate(outlines, 1):
            md.append(f"### {idx}. {o.get('title', 'Untitled')}")
            md.append(f"**Audience:** {o.get('target_audience', '')} | **Distribution:** {o.get('distribution', '')} | **Pillar:** {o.get('pillar', '')}")
            md.append("")
            md.append(f"**Thesis:** {o.get('thesis', '')}")
            md.append("")
            sections = o.get("sections", [])
            if sections:
                md.append("**Outline:**")
                for s in sections:
                    if isinstance(s, dict):
                        md.append(f"1. **{s.get('heading', '')}** - {s.get('description', '')}")
                    else:
                        md.append(f"1. {s}")
                md.append("")
            data_pts = o.get("key_data_points", [])
            if data_pts:
                md.append(f"**Key data points:** {', '.join(str(d) for d in data_pts)}")
            if o.get("lowtouch_angle"):
                md.append(f"**lowtouch.ai angle:** {o['lowtouch_angle']}")
            seo = o.get("seo_keywords", [])
            if seo:
                md.append(f"**SEO Keywords:** {', '.join(seo)}")
            md.append("")

    # --- Engagement Targets ---
    targets = engagement.get("targets", [])
    if targets:
        md.append("---")
        md.append("## Engagement Targets")
        md.append("")
        md.append("| # | Topic | Author Type | Priority | Suggested Comment |")
        md.append("|---|-------|-------------|----------|-------------------|")
        for i, t in enumerate(targets, 1):
            comment = _esc(t.get("suggested_comment", "").replace("\n", " "))
            topic = _esc(t.get("topic", ""))
            author = _esc(t.get("author_type", ""))
            priority = _esc(t.get("priority", ""))
            md.append(f"| {i} | {topic} | {author} | {priority} | {comment} |")
        md.append("")

    # --- Founder's Notebook ---
    if founders and founders.get("prompt"):
        md.append("---")
        md.append("## Founder's Notebook")
        md.append(f"**Series:** {founders.get('series_name', 'Friday Field Notes')} | **Posting slot:** {founders.get('posting_slot', 'Friday 10:00 AM IST')}")
        md.append("")
        md.append(f"**Trending topic:** {founders.get('trending_topic', '')}")
        md.append("")
        md.append(f"**Prompt:** {founders.get('prompt', '')}")
        md.append("")
        angles = founders.get("example_angles", [])
        if angles:
            md.append("**Example angles:**")
            for a in angles:
                md.append(f"- {a}")
        md.append(f"**Pillar:** {founders.get('pillar_connection', '')}")
        md.append("")

    # --- Posting Calendar ---
    schedule = calendar.get("schedule", [])
    if schedule:
        md.append("---")
        md.append("## Posting Calendar")
        md.append("")
        md.append("| Day | Time | Channel | Type | Content |")
        md.append("|-----|------|---------|------|---------|")
        for day in schedule:
            day_name = _esc(day.get("day", ""))
            for slot in day.get("slots", []):
                md.append(f"| {day_name} | {_esc(slot.get('time', ''))} | {_esc(slot.get('channel', ''))} | {_esc(slot.get('type', ''))} | {_trunc(slot.get('content', ''), 50)} |")
                day_name = ""  # only show day name on first row
        md.append("")

    md.append("---")
    md.append("*Generated by Pulse Content Engine v0.3*")

    return "\n".join(md)


REDIS_REPORT_TTL = 86400 * 7  # 7 days


def assemble_report(**context):
    """Assemble all outputs into a single markdown report and persist to Redis."""
    set_request_id(context)
    lot.info("assembling final report...")
    ti = context["ti"]

    channels = ti.xcom_pull(task_ids="discover_channels_and_videos") or []
    videos = ti.xcom_pull(task_ids="discover_channels_and_videos", key="videos") or []
    trends = ti.xcom_pull(task_ids="analysis.analyze_trends") or {}
    youtube_ideas = ti.xcom_pull(task_ids="content_drafting.draft_youtube_ideas") or {}
    linkedin_posts = ti.xcom_pull(task_ids="content_drafting.draft_linkedin_posts") or {}
    carousels = ti.xcom_pull(task_ids="content_drafting.draft_linkedin_carousels") or {}
    reels = ti.xcom_pull(task_ids="content_drafting.draft_reel_scripts") or {}
    blog_outlines = ti.xcom_pull(task_ids="content_drafting.draft_blog_outlines") or {}
    engagement = ti.xcom_pull(task_ids="content_drafting.draft_engagement_targets") or {}
    founders = ti.xcom_pull(task_ids="content_drafting.draft_founders_notebook") or {}
    calendar = ti.xcom_pull(task_ids="generate_posting_calendar") or {}

    # Generate report ID
    report_id = str(uuid.uuid4())
    lot.info(f"report id: {report_id}")

    report_md = _render_markdown(
        channels, videos, trends, youtube_ideas, linkedin_posts,
        carousels, reels, blog_outlines, engagement, founders, calendar,
        report_id=report_id,
    )

    yt_count = len(youtube_ideas.get("ideas", []))
    li_count = len(linkedin_posts.get("posts", []))
    car_count = len(carousels.get("carousels", []))
    reel_count = len(reels.get("reels", []))
    blog_count = len(blog_outlines.get("outlines", []))
    eng_count = len(engagement.get("targets", []))

    # Persist source data and report to Redis (7 day TTL)
    report_data = {
        "report_id": report_id,
        "generated_at": pendulum.now("Asia/Kolkata").isoformat(),
        "week_of": calendar.get("week_of", ""),
        "source": {
            "channels": channels,
            "videos": videos,
        },
        "analysis": {
            "trends": trends,
        },
        "content": {
            "youtube_ideas": youtube_ideas,
            "linkedin_posts": linkedin_posts,
            "carousels": carousels,
            "reels": reels,
            "blog_outlines": blog_outlines,
            "engagement_targets": engagement,
            "founders_notebook": founders,
        },
        "calendar": calendar,
        "report_md": report_md,
    }

    redis_key = f"pulse:report:{report_id}"
    try:
        _cache_set(redis_key, report_data, ttl=REDIS_REPORT_TTL)
        lot.info(f"report saved to redis: {redis_key}")
        logger.info(f"Report persisted to Redis key {redis_key} (7d TTL)")
    except Exception as e:
        logger.warning(f"Failed to persist report to Redis: {e}")
        lot.info("warning: could not save report to redis, but report is still available")

    ti.xcom_push(key="report", value=report_md)
    ti.xcom_push(key="report_id", value=report_id)

    logger.info(
        f"Report {report_id} assembled: {yt_count} YouTube ideas, {li_count} LinkedIn posts, "
        f"{car_count} carousels, {reel_count} reels, {blog_count} blog outlines, "
        f"{eng_count} engagement targets"
    )
    lot.info(
        f"report ready: {yt_count} YouTube ideas, {li_count} LinkedIn posts, "
        f"{car_count} carousels, {reel_count} reels, {blog_count} blog outlines, "
        f"{eng_count} engagement targets"
    )
    return report_md


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


with DAG(
    "pulse_content_engine",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["pulse", "content", "weekly"],
    description=(
        "Weekly content engine that monitors enterprise agentic AI trends on YouTube, "
        "analyzes themes and gaps, and drafts multi-channel content (YouTube ideas, "
        "LinkedIn posts, carousels, Instagram Reels, blog outlines, engagement targets) "
        "mapped to lowtouch.ai's positioning pillars. YouTube data is cached in Redis "
        "for 15 minutes by default; set use_cache to false to force fresh API calls."
    ),
    params={
        "content_types": Param(
            default="all",
            type="string",
            enum=["all", "youtube", "linkedin", "reels", "blog", "engagement"],
            description="Which content types to generate (all or specific category)",
        ),
        "use_cache": Param(
            default=True,
            type="boolean",
            description="Use cached YouTube data if available (15-min TTL). Set to false to force fresh YouTube API calls.",
        ),
    },
) as dag:

    discover = PythonOperator(task_id="discover_channels_and_videos", python_callable=discover_channels_and_videos)

    with TaskGroup("analysis") as analysis_tg:
        trends = PythonOperator(task_id="analyze_trends", python_callable=analyze_trends)

    with TaskGroup("content_drafting") as drafting_tg:
        yt = PythonOperator(task_id="draft_youtube_ideas", python_callable=draft_youtube_ideas)
        li = PythonOperator(task_id="draft_linkedin_posts", python_callable=draft_linkedin_posts)
        car = PythonOperator(task_id="draft_linkedin_carousels", python_callable=draft_linkedin_carousels)
        reels_task = PythonOperator(task_id="draft_reel_scripts", python_callable=draft_reel_scripts)
        blog = PythonOperator(task_id="draft_blog_outlines", python_callable=draft_blog_outlines)
        eng = PythonOperator(task_id="draft_engagement_targets", python_callable=draft_engagement_targets)
        fn = PythonOperator(task_id="draft_founders_notebook", python_callable=draft_founders_notebook)

    cal = PythonOperator(task_id="generate_posting_calendar", python_callable=generate_posting_calendar)
    assemble = PythonOperator(task_id="assemble_report", python_callable=assemble_report)

    # Pipeline: discover -> analysis -> drafting -> calendar -> assemble
    discover >> analysis_tg >> drafting_tg >> cal >> assemble
