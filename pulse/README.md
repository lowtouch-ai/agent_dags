# Pulse Content Engine

Intelligence-to-content pipeline that monitors the enterprise agentic AI landscape and produces multi-channel thought leadership content for [lowtouch.ai](https://lowtouch.ai).

---

## DAGs

### pulse_content_engine

Weekly content engine that monitors enterprise agentic AI trends on YouTube (mainstream channels and emerging 5K-50K subscriber channels), analyzes themes and gaps using video transcripts, and drafts multi-channel content mapped to lowtouch.ai's positioning pillars.

**Schedule:** None (triggered by agent or manual) | **Tags:** pulse, content, weekly

#### Pipeline

```
discover_channels_and_videos ──┐
                               ├──► analyze_trends ──► draft_* (6 parallel) ──► assemble_report
discover_emerging_channels ────┘
```

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| content_types | string (enum) | "all" | Which content types to generate: all, youtube, linkedin, reels, blog, engagement |
| use_cache | boolean | true | Use cached YouTube data if fresh (6-hour window). Set to false to force a full 7-day YouTube API pull. |

---

### pulse_article_creator

Creates or edits a LinkedIn article from a Pulse weekly report content item. Auto-detects create vs edit mode and whether to regenerate the header graphic.

**Schedule:** None (triggered by agent or manual) | **Tags:** pulse, content, article

#### Pipeline

```
load_context ──► deep_research ──► write_article ──► humanize_article ──► generate_graphics ──► assemble_article
```

#### Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| report_id | string | *(required)* | UUID of the content report to pull source data from |
| content_type | string (enum) | "blog_outline" | Content section to expand: blog_outline, linkedin_post, youtube_idea, carousel, reel, founders_notebook |
| content_index | integer | 0 | 0-based index of the item within that content section |
| article_id | string or null | null | If editing an existing article, pass its UUID to load and revise it |

---

## Deployment

### Python Packages (non-stdlib)

| Package | Used For |
|---------|----------|
| openai | GPT-4o: trend analysis, content drafting, article writing, humanization, intent classification |
| google-genai | Gemini 2.5 Flash Image: article header graphics |
| google-api-python-client | YouTube Data API v3: channel discovery, video metadata |
| youtube-transcript-api | Fetching video transcripts for analysis |
| redis | Caching (YouTube data, articles), thought logging |
| pendulum | Timezone-aware dates |

### Airflow Variables

| Variable | Description |
|----------|-------------|
| OPENAI_API_KEY | OpenAI API key for GPT-4o calls |
| YOUTUBE_API_KEY | YouTube Data API v3 key |
| GEMINI_API_KEY | Google Gemini API key for image generation |

### Supporting Files

- agent_dags/branding.md (one level up from pulse/) must be deployed. The article creator loads it at runtime for header graphic style rules.
