# lowtouch.ai Weekly Content Engine

## Workflow Specification for LinkedIn, YouTube, and Instagram Content Production

---

**Owner:** Rejith Krishnan, Founder/CEO
**Version:** 1.0
**Date:** February 2026
**Status:** Final for Implementation

---

## 1. Purpose and Objective

This document specifies the workflow for a weekly, automated content engine that monitors the enterprise agentic AI landscape and produces multi-channel content for lowtouch.ai's marketing.

The workflow turns market intelligence into thought leadership. Each week, it delivers a trend report summarizing what is happening in the enterprise agentic AI space, plus a full set of draft content across LinkedIn, YouTube, and Instagram, all tied to trending topics and anchored in lowtouch.ai's positioning.

This is not a generic social media scheduler. It is an intelligence-to-content pipeline designed to keep lowtouch.ai's voice present, relevant, and opinionated in the conversations that matter to CIOs, CTOs, and enterprise AI decision-makers.

---

## 2. Scope

### 2.1 What This Workflow Covers

- Automated discovery and monitoring of enterprise agentic AI content across YouTube, LinkedIn, and key industry sources
- Weekly trend analysis: top keywords, emerging themes, engagement patterns, competitor signals
- Content recommendations mapped to lowtouch.ai's five positioning pillars
- Draft LinkedIn text posts (2-3 per week) with hooks, body copy, and CTAs
- LinkedIn carousel outlines (2 per week) with slide-by-slide copy
- YouTube long-form video ideas (3 per week) with full outlines
- Instagram Reel scripts (3 per week, 15-30 seconds, teleprompter-ready)
- Blog/article outlines (2 per week) for longer-form content
- Strategic engagement targets (10-15 per week) for LinkedIn commenting
- Weekly Founder's Notebook prompt
- Competitor content snapshot
- Day-by-day posting calendar with specific times and channels
- Platform-specific discoverability tags (hashtags, SEO keywords) for every content piece
- Weekly summary report delivered via email (auto mode) or markdown (interactive mode)

### 2.2 What This Workflow Does Not Cover

- Automated posting to any platform (all content goes through human review first)
- Full blog article writing (outlines only; Rejith or a writer completes them)
- YouTube video production, filming, or editing
- Instagram Reel filming or editing (scripts only)
- Paid ad copy, email marketing sequences, or newsletter content

---

## 3. Content Positioning Anchors

Every piece of content produced by this workflow must connect back to one or more of lowtouch.ai's positioning pillars. These are non-negotiable guardrails for messaging consistency.

| Pillar | Messaging Anchor |
|---|---|
| **Private by Architecture** | Runs entirely inside customer infrastructure. No data leaves. Supports air-gapped environments. |
| **No-Code** | Business and technical teams assemble, configure, and orchestrate agents without writing code. |
| **Production in 4-6 Weeks** | Pre-built, customizable agents with predictable timelines from idea to governed production. |
| **Governed Autonomy** | HITL controls, thought-logging, compliance readiness (ISO 27001, SOC 2, GDPR, RBI). |
| **Enterprise Architecture** | ReAct/CodeAct frameworks, Apache Airflow orchestration, multi-LLM support, full observability. |

**Content tone rules:** Write as a founder/practitioner, not a marketer. Lead with business outcomes. Avoid hype words ("revolutionary," "game-changing," "cutting-edge"). Use commas, periods, and parentheses instead of em dashes. Never position lowtouch.ai as a chatbot, copilot, or consumer AI tool.

---

## 4. Workflow Architecture

The workflow operates as a three-layer pipeline that runs weekly. Each layer feeds the next, with a human review checkpoint before any content is published.

### 4.1 Layer 1: Intelligence Gathering

This layer collects raw data from multiple sources to build a comprehensive picture of what is trending in enterprise agentic AI.

#### Data Sources

| Source | What to Collect | Method |
|---|---|---|
| **YouTube** | Top videos from 25-30 AI/automation channels. Titles, view counts, likes, comments, upload dates. | YouTube Data API v3. Batch fetch. Weekly cron. |
| **LinkedIn Signals** | Trending hashtags, high-engagement posts from target personas (CIOs, CTOs, AI leaders), competitor company page activity. | LinkedIn API (where available), manual curation list, or third-party monitoring tool (e.g., Shield, Taplio). |
| **Industry Sources** | Analyst reports (Gartner, Forrester), vendor announcements, open-source project releases, regulatory updates. | RSS feeds, Google Alerts, curated URL list. Parsed via web scraping or API. |
| **Competitor Watch** | Public content from Microsoft Copilot, ServiceNow AI, UiPath, Automation Anywhere, CrewAI, LangChain ecosystem. | Company blog RSS, LinkedIn page monitoring, GitHub release tracking. |

#### YouTube Channel Discovery (Automated)

The system autonomously identifies top YouTube channels in the enterprise AI and AI automation niche rather than relying on a static list. Channel discovery refreshes monthly. Video data pulls weekly.

- Batch capacity: approximately 30 channels and 180+ videos per run
- Metrics extracted: video titles, IDs, view counts, like counts, comment counts, upload dates
- Rate limit management: built-in throttling to stay within YouTube API quotas during standard weekly execution

### 4.2 Layer 2: Analysis and Synthesis

Raw data from Layer 1 is processed into actionable intelligence. This is where trends become content opportunities.

#### Trend Extraction

- **Keyword analysis:** Extract trending keywords and phrases from video titles, post text, and article headlines (e.g., identify that "multi-agent orchestration" or "Claude Code" is surging)
- **Performance aggregation:** Calculate median view counts, median engagement scores, and posting frequency patterns for the reporting period
- **Theme clustering:** Group related keywords into broader themes and map them to the five positioning pillars (e.g., "privacy," "data sovereignty," "air-gapped" all cluster under Private by Architecture)
- **Gap analysis:** Identify high-interest topics where lowtouch.ai has a strong POV but limited existing content

#### Content Recommendation Engine

Based on trend data, the system generates specific content recommendations. Each recommendation includes:

1. The trending topic or keyword cluster
2. Which lowtouch.ai positioning pillar it maps to
3. A suggested angle (contrarian take, practical insight, case study reference, market commentary)
4. Content format (text post, carousel, YouTube video, Reel, blog outline)
5. Priority score (based on trend velocity, relevance to positioning, and content gap)

---

## 5. Layer 3: Content Drafting

The workflow produces content across five categories each week, all tied to that week's trending topics and mapped to lowtouch.ai's positioning pillars.

### 5.1 YouTube Long-Form Ideas (3 per week)

Each idea includes:

- **Working title:** Specific, search-friendly, not clickbait
- **Target length:** 8-15 minutes
- **Hook summary:** The first 30 seconds, written out. What grabs the viewer and why they should stay.
- **Episode outline:** 4-6 segments with timestamps and talking points
- **Trend tie-in:** Which trending topic or keyword cluster this responds to
- **Pillar mapping:** Which lowtouch.ai positioning pillar it reinforces
- **Visual/demo notes:** Where to show platform demos, architecture diagrams, or screen shares
- **CTA:** What the viewer should do next (subscribe, visit site, comment)
- **SEO keywords:** 5-8 keywords for title, description, and tags

#### YouTube Content Mix

| Type | Description |
|---|---|
| **Trend Explainer** | Break down a trending topic for enterprise leaders. "What [topic] actually means for your AI strategy." |
| **Build/Demo Walkthrough** | Show how something works on lowtouch.ai. Practical, not salesy. "How we built X agent in 3 weeks." |
| **Market Commentary / POV** | Take a strong position on an industry development. "Why [prevailing narrative] is wrong, and what to do instead." |

### 5.2 LinkedIn Text Posts (2-3 per week)

Each draft LinkedIn post follows this structure:

- **Hook (first 2 lines):** A specific, attention-grabbing opening that references the trend. No generic "AI is changing everything" openings.
- **Body (3-6 short paragraphs):** The insight, framed through lowtouch.ai's lens. Practical, opinionated, grounded in real enterprise context.
- **CTA:** A question, invitation to comment, or soft pointer to lowtouch.ai. Never a hard sell.
- **Metadata:** Source trend, target pillar, suggested posting day/time, 3-5 niche hashtags.

#### LinkedIn Text Post Type Mix

| Type | Frequency | Description |
|---|---|---|
| **Trend Commentary** | 1x/week | React to a specific trend or announcement with a practitioner's perspective. |
| **Contrarian / POV** | 1x/week | Challenge a prevailing narrative. Take a clear, opinionated position. |
| **Industry Signal** | 0-1x/week | Comment on analyst reports, regulatory changes, or competitor moves. |

### 5.3 LinkedIn Carousels (2 per week)

Each carousel outline includes:

- **Title slide:** Headline that works as a standalone hook in the feed
- **Slide-by-slide copy:** 8-12 slides, each with a heading and 1-2 sentences of body text
- **Visual direction:** Color scheme, icon suggestions, diagram descriptions per slide
- **Final slide CTA:** Follow, comment, visit, or save prompt
- **Caption:** 3-5 sentences summarizing the carousel's value, with hashtags
- **Metadata:** Source trend, target pillar, suggested posting day

#### Carousel Content Mix

| Type | Frequency | Description |
|---|---|---|
| **Framework / How-To** | 1x/week | "5 questions to ask before deploying AI agents," "The architecture behind a governed AI agent." Actionable, saveable. |
| **Insight Breakdown** | 1x/week | Visual breakdown of a trending topic, data point, or comparison. Makes complex ideas scannable. |

### 5.4 Instagram Reels (3 per week, 15-30 seconds each)

Each reel includes a ready-to-shoot, teleprompter-friendly script with timing cues. Rejith is on camera (talking head) for all reels.

#### Script Format

- **Hook (0-3 sec):** Text overlay + spoken line that stops the scroll
- **Body (3-22 sec):** Core message, delivered in 2-3 punchy statements
- **Close (22-30 sec):** Takeaway or CTA, with text overlay
- **On-screen text:** Exact words for each text overlay
- **Visual direction:** What is on screen (talking head framing, screen recording insert, text animation)
- **Audio note:** Background music vibe, voiceover pacing
- **Hashtags:** 3-5 Instagram hashtags plus suggested trending audio if applicable

#### Teleprompter Formatting Rules

Scripts are written for direct-to-camera delivery with short lines, natural breath points, and conversational rhythm. No line longer than 10 words. Each spoken segment is on its own line for easy reading while recording.

#### Reel Content Mix

| Type | Frequency | Example |
|---|---|---|
| **Quick Insight** | 1x/week | "Most enterprises fail at AI not because of the model, but because of governance. Here's the one thing they skip." |
| **Myth Buster or Trend React** | 1x/week (alternating) | "You don't need a team of ML engineers to deploy AI agents. Here's why." / "Everyone's talking about [topic] this week. Here's what it means for enterprise." |
| **Behind the Build** | 1x/week | Quick peek at how something works on lowtouch.ai. 15-second screen capture with voiceover. |

#### Reel Voice Rules

Conversational, direct, confident. No corporate speak. Speak like you are explaining something to a smart friend, not presenting at a conference. Short sentences. One idea per reel, no more.

### 5.5 Blog / LinkedIn Article Outlines (2 per week)

Each outline includes:

- **Working title:** Clear, professional, positions Rejith as a practitioner
- **Target audience:** Specific persona (CIO evaluating AI platforms, CTO building agent infrastructure, CISO worried about data governance, etc.)
- **Thesis statement:** One sentence that captures the core argument
- **Outline:** 4-6 section headers with 2-3 sentence descriptions per section
- **Key data points:** Specific stats, references, or examples to include
- **lowtouch.ai angle:** How this connects back to the platform without making it a product pitch
- **Suggested distribution:** LinkedIn article (native), company blog, or both
- **SEO keywords:** 5-8 target keywords for the article

---

## 6. Recurring Content Series

The following branded series anchor the weekly content cadence. Series build audience habits, improve recognition, and simplify content planning because the format is fixed; only the topic changes each week.

### 6.1 YouTube Series

| Series Name | Format | Frequency | Description |
|---|---|---|
| **Agent Architecture Deep Dive** | 10-15 min explainer | Biweekly | How a specific type of agent works under the hood. Architecture diagrams, design decisions, trade-offs. Positions Rejith as a builder, not a vendor. |
| **Enterprise AI Myths** | 8-10 min debunk | Biweekly (alternating with above) | Pick one widely held belief about enterprise AI and dismantle it with evidence and experience. Contrarian, specific, grounded. |
| **Build It Live** | 12-15 min demo | Monthly (bonus) | Live building or configuring an agent on lowtouch.ai. Raw, unscripted, showing the real workflow. |

### 6.2 LinkedIn Series

| Series Name | Format | Frequency | Description |
|---|---|---|
| **Friday Field Notes** | Short text post (150-250 words) | Weekly (Friday) | One specific thing Rejith learned that week from building or deploying agents. Personal, reflective, practical. Ends with a question to spark discussion. |
| **The Enterprise AI Playbook** | Carousel (8-12 slides) | Weekly | Frameworks, checklists, and decision guides for enterprise AI leaders. Designed to be saved and shared. Builds a library of reference content over time. |

### 6.3 Instagram Series

| Series Name | Format | Frequency | Description |
|---|---|---|
| **30 Seconds of Agentic AI** | Talking head Reel (15-30 sec) | 2-3x/week | Consistent branded intro and outro. Same visual style every time. One sharp insight per reel. Builds recognition so followers know exactly what to expect. |

---

## 7. Strategic Engagement Targets

Each weekly report includes 10-15 high-value LinkedIn posts that Rejith should comment on. This is not optional filler; strategic commenting is one of the highest-ROI activities on LinkedIn, with data showing it can increase profile views by up to 50%.

### What the Report Provides

For each target post:

- **Post author:** Name, title, company, follower count
- **Post summary:** 1-2 sentence description of what they posted
- **Why it matters:** Connection to enterprise agentic AI or lowtouch.ai's positioning
- **Suggested comment angle:** A specific 2-3 sentence comment that adds value, demonstrates expertise, or offers a complementary perspective. Not "great post" comments.
- **Priority:** High / Medium (based on author influence and topic relevance)

### Target Persona Categories

- CIOs and CTOs at enterprises evaluating or deploying AI
- AI/ML leaders at Fortune 500 companies
- Analyst firms (Gartner, Forrester analysts who cover AI)
- Competitor executives (engage respectfully, position through contrast)
- Open-source community leaders (LangChain, CrewAI, AutoGen ecosystem)
- Enterprise tech media and journalists

### Time Budget

10 minutes per day, Tuesday through Friday. That is 4-5 substantive comments per day. The workflow identifies the targets; Rejith personalizes and posts.

---

## 8. Founder's Notebook Prompt

Each weekly report includes one Founder's Notebook prompt designed to inspire a personal, human post from Rejith. These cannot be fully automated because they require genuine personal experience and reflection. The workflow's job is to make it easy by connecting a trending topic to a personal prompt.

### Prompt Format

- **This week's trending topic:** [topic from the trend report]
- **Prompt:** A specific question or scenario that ties the trend to Rejith's experience building lowtouch.ai
- **Example angles:** 2-3 starter directions Rejith could take the post
- **Pillar connection:** Which positioning pillar this naturally reinforces

### Example

> **Trending topic:** Enterprise AI governance frameworks
>
> **Prompt:** Share a time when a customer's compliance requirement forced you to rethink how you built a feature. What did you change, and what did it teach you about building for regulated industries?
>
> **Example angles:** (1) A specific compliance conversation that changed your architecture decisions. (2) Why you chose to build governance into the platform from day one instead of bolting it on later. (3) What most AI vendors get wrong about compliance.
>
> **Pillar:** Governed Autonomy

---

## 9. Competitor Content Snapshot

Each weekly report includes a full competitor content snapshot covering what the major players published, what messaging they are pushing, and where lowtouch.ai's positioning creates white space.

### Competitors Tracked

| Competitor | What to Monitor |
|---|---|
| **Microsoft Copilot** | LinkedIn company page, blog, product announcements |
| **ServiceNow AI** | LinkedIn, blog, analyst briefing mentions |
| **UiPath** | LinkedIn, blog, community posts |
| **Automation Anywhere** | LinkedIn, blog, product updates |
| **CrewAI** | GitHub releases, blog, LinkedIn, community |
| **LangChain / LangGraph** | GitHub releases, blog, LinkedIn, community |
| **AutoGen (Microsoft)** | GitHub releases, blog, LinkedIn |

### Snapshot Format

For each competitor with notable activity that week:

- **What they posted:** Brief summary of key content pieces
- **Messaging themes:** What narrative they are pushing (e.g., "democratizing AI," "AI for every employee," "agent orchestration")
- **Engagement signals:** Visible engagement levels (likes, comments) if available
- **White space for lowtouch.ai:** Where their messaging creates an opening for lowtouch.ai's positioning (e.g., they talk about cloud-first but don't address data sovereignty; they promote no-code but lack governance depth)

---

## 10. Discoverability Tags

Every content piece produced by the workflow includes platform-specific discoverability tags.

### LinkedIn (text posts and carousels)

- 3-5 niche hashtags per post (e.g., #AgenticAI #EnterpriseAI #AIGovernance #NoCodeAI, not generic tags like #AI or #Technology)
- Tags selected based on trending hashtag data from the intelligence layer

### Instagram (Reels)

- 3-5 hashtags per Reel, mix of niche and discoverability tags
- Suggested trending audio if a relevant sound is gaining traction
- Caption optimized for Instagram search (keyword-rich first line)

### YouTube (long-form ideas)

- 5-8 SEO keywords per video idea, based on YouTube search trends from the intelligence layer
- Suggested title variations optimized for search
- Description outline with keyword placement
- Tag list for YouTube Studio

---

## 11. Operating Modes

The workflow supports two execution modes with identical intelligence gathering and analysis layers. The only difference is how it is triggered and how output is delivered.

### Mode 1: Interactive

**Trigger:** Invoked manually from this Claude project chat interface.

**How it works:** Rejith or a team member sends a command like "run weekly content report" or "what's trending this week in agentic AI." The workflow executes the full pipeline and returns results directly in chat.

**Output format:** Markdown, structured for easy reading in a chat window.

**Interactive mode capabilities:**

- Follow-up questions in the same session: "give me 2 more reel ideas on the governance topic," "rewrite YouTube idea 2 with a stronger hook," "what did CrewAI post this week"
- System maintains context within the session, so refinements build on the report
- Partial runs on request: "just give me the reel scripts" or "just the trend summary, skip the content drafts"

### Mode 2: Auto

**Trigger:** Cron schedule, every Monday at 6:00 AM IST (Indian Standard Time, UTC+5:30). This translates to Sunday 7:30 PM CT (America/Chicago).

**How it works:** The full pipeline runs unattended. No human input required. The complete report is formatted as a well-designed HTML email and sent to rejith@lowtouch.ai via Gmail API.

**Output format:** HTML email with inline styling (no external stylesheets). Renders cleanly in Gmail, Apple Mail, and Outlook.

**Email structure:**

| Section | Formatting |
|---|---|
| **Header** | lowtouch.ai branding, report title, date range |
| **Executive Summary** | 5-7 sentences in a light background card |
| **Trending Keywords** | Inline tags with engagement indicators (up/down arrows, percentage changes) |
| **YouTube Ideas (3)** | Each in its own bordered card. Title bold, type as colored label, hook highlighted, outline numbered, demo notes in italic, SEO keywords listed |
| **LinkedIn Text Posts (2-3)** | Each in a card. Full draft text, hashtags, posting slot |
| **LinkedIn Carousels (2)** | Each in a card. Slide-by-slide copy, visual direction, caption |
| **Reel Scripts (3)** | Compact cards. Title with duration badge, timing sections separated, spoken lines in regular text, on-screen text in accent style, visual direction in italic |
| **Blog/Article Outlines (2)** | Cards with thesis, outline, data points, distribution plan |
| **Engagement Targets (10-15)** | Table with author, post summary, suggested comment angle |
| **Founder's Notebook Prompt** | Highlighted card with prompt and example angles |
| **Competitor Snapshot** | Table by competitor with activity, messaging, white space |
| **Posting Calendar** | Day-by-day grid (see Section 12) |
| **Data Snapshot** | Key metrics in a 2-column mini table |
| **Footer** | "Generated by lowtouch.ai Weekly Content Engine" with timestamp |

**Email design rules:**

- Inline CSS only (Gmail strips style blocks)
- Arial/Helvetica, 14px body, 1.5 line height
- Max width 640px, centered
- Colors: dark navy headers (#1B2A4A), accent pink for labels/highlights (#E91E8C), light gray card backgrounds (#F5F5F5), body text #333333
- No images in email body (lightweight, avoids rendering issues)
- Each content piece visually distinct for fast scanning

**Auto mode also writes to Google Sheets** (3-tab structure: Channel Stats, Top Videos, Weekly Summary) for historical tracking.

### Mode Comparison

| Dimension | Interactive | Auto |
|---|---|---|
| Trigger | Manual, from Claude project chat | Cron, Monday 6 AM IST |
| Output format | Markdown in chat | HTML email to rejith@lowtouch.ai |
| Follow-up capability | Yes, conversational refinement | No (static delivery) |
| Partial runs | Yes ("just the reels") | No (full report every time) |
| Google Sheets update | Optional (on request) | Automatic |
| Content identical? | Yes, same pipeline | Yes, same pipeline |
| Best for | Mid-week inspiration, ad-hoc requests, refining drafts | Consistent weekly cadence, Monday morning review ritual |

---

## 12. Day-by-Day Posting Calendar

Each weekly report ends with a specific posting calendar. This is the template; actual content pieces are slotted in each week based on what was produced.

| Day | Time (IST) | Channel | Content Type | Notes |
|---|---|---|---|---|
| **Monday** | 6:00 AM | Email inbox | Weekly report lands | Review and approve/edit content |
| **Tuesday** | 9:00 AM - 9:10 AM | LinkedIn | Strategic commenting (4-5 comments) | Use engagement targets from report |
| **Tuesday** | 10:00 AM | LinkedIn | Carousel #1 (The Enterprise AI Playbook) | Series post, high save potential |
| **Wednesday** | 9:00 AM - 9:10 AM | LinkedIn | Strategic commenting (4-5 comments) | |
| **Wednesday** | 10:00 AM | LinkedIn | Text post #1 (Trend Commentary) | Strongest trend-reactive piece |
| **Wednesday** | 12:30 PM | Instagram | Reel #1 (Quick Insight) | Part of "30 Seconds of Agentic AI" series |
| **Thursday** | 9:00 AM - 9:10 AM | LinkedIn | Strategic commenting (4-5 comments) | |
| **Thursday** | 10:00 AM | LinkedIn | Carousel #2 or Text post #2 | Contrarian POV or Insight Breakdown |
| **Thursday** | 12:30 PM | Instagram | Reel #2 (Myth Buster or Trend React) | Alternating format week to week |
| **Friday** | 9:00 AM - 9:10 AM | LinkedIn | Strategic commenting (4-5 comments) | |
| **Friday** | 10:00 AM | LinkedIn | Friday Field Notes (Founder's Notebook) | Personal, reflective, uses the weekly prompt |
| **Friday** | 12:30 PM | Instagram | Reel #3 (Behind the Build) | Screen capture + voiceover |
| **Weekend** | Flexible | YouTube | Video published (if produced that week) | Based on one of the 3 weekly ideas |

**Total weekly time commitment for Rejith:**

- Monday: 15-20 min reviewing report, approving/editing content
- Tuesday-Friday: 10 min/day strategic commenting
- Tuesday-Friday: 2-3 min/day approving scheduled posts
- Reel recording: 15-20 min total for 3 reels (batch record)
- Founder's Notebook: 10 min writing on Friday
- **Total: approximately 1.5-2 hours per week**

---

## 13. Google Sheets Database

Structured data exports to a Google Sheet with three tabs, maintained as a running historical record.

| Tab | Fields |
|---|---|
| **Channel Stats** | Channel ID, Name, Subscriber Count, Total Views, Video Count, Last Updated |
| **Top Videos** | Video ID, Title, Channel, Engagement Rate, Age, Relevance Tags |
| **Weekly Summary** | Week Date, Median Views, Top Keywords, Theme Clusters, Content Recommendations Count, Competitor Highlights |

---

## 14. Content Voice and Style Guidelines

These guidelines apply to all draft content produced by the workflow. They ensure every post sounds like it came from Rejith, not from an AI tool or a marketing team.

### 14.1 Voice Principles

**Write as a builder, not a vendor.** Rejith builds enterprise AI systems. The voice should reflect that. Practical, specific, grounded in real implementation experience.

**Be opinionated.** The best content takes a clear position. "Here is what I think, and here is why." Not "there are many perspectives on this topic."

**Lead with the problem.** Start with the pain point or the question that enterprise leaders are actually dealing with. Then offer the insight.

**Skip the jargon.** No "synergy," "paradigm shift," "unlock value," or "leverage AI." Write the way you would explain something to a smart colleague over coffee.

### 14.2 Hard Rules for All Drafts

- No em dashes. Use commas, periods, semicolons, or parentheses.
- No exclamation marks (except very rarely for genuine emphasis).
- No emoji in LinkedIn post body (acceptable in Instagram captions sparingly).
- No "revolutionary," "game-changing," "cutting-edge," or similar hype words.
- Never position lowtouch.ai as a chatbot, copilot, or consumer AI product.
- Always say "private, no-code Agentic AI platform" (not just "AI platform").
- If referencing competitors, be factual and respectful. Never trash-talk.
- LinkedIn posts: no external links in the post body (kills reach by ~30%). Put links in first comment.
- LinkedIn carousels: first slide must work as a standalone hook in the feed.
- Instagram Reels: hook must land in the first 1.5 seconds (before viewers scroll past).
- YouTube: first 30 seconds must clearly state what the viewer will learn and why it matters.

---

## 15. Automation and Infrastructure

### 15.1 Schedule

| Parameter | Value |
|---|---|
| **Trigger** | Cron schedule, every Monday at 6:00 AM IST (Sunday 7:30 PM CT) |
| **Data Collection Window** | Previous 7 days (Monday to Sunday) |
| **Report Delivery** | Email to rejith@lowtouch.ai by 6:30 AM IST |
| **Content Review Deadline** | Monday EOD (Rejith reviews, edits, approves) |
| **Posting Window** | Tuesday through Friday per calendar |

### 15.2 Deployment

- Serverless execution on Modal infrastructure (no local dependencies)
- API keys for YouTube, Gmail, and Google Sheets managed via Modal Secrets and .env files
- No credentials hardcoded or exposed in the repository
- Error handling: if any API call fails, the workflow logs the error and continues with available data rather than failing entirely

---

## 16. Success Metrics

After 4 weeks of operation, evaluate the workflow against these benchmarks.

| Metric | Target (Month 1) | Target (Month 3) |
|---|---|---|
| LinkedIn posts published per week (text + carousel) | 4 | 5 |
| Avg. LinkedIn engagement rate (likes + comments / impressions) | 2-3% | 4%+ |
| LinkedIn profile views (Rejith personal) | +25% vs. baseline | +50% vs. baseline |
| Inbound connection requests from target personas | Track (establish baseline) | +30% vs. baseline |
| YouTube subscriber growth | +50/month | +150/month |
| Instagram Reel avg. views | Track baseline | +40% vs. baseline |
| Rejith review time per week | Under 2 hours | Under 1.5 hours |
| Content drafts requiring major rewrites | Under 40% | Under 20% |
| Strategic comments posted per week | 15+ | 20+ |

---

## 17. Implementation Roadmap

### Phase 1: Foundation (Week 1-2)

- Set up YouTube Data API integration with channel discovery logic
- Configure Google Sheets database with three-tab structure
- Build trend extraction pipeline (keyword analysis, performance aggregation)
- Deploy to Modal with secrets management
- Define recurring series branding (names, visual templates, intro/outro formats)
- Test: Manual run, verify data quality and report output

### Phase 2: Content Layer (Week 3-4)

- Add content recommendation engine with pillar mapping
- Build LinkedIn text post and carousel drafting with voice guidelines baked in
- Build YouTube idea generator with SEO keyword integration
- Build Instagram Reel script generator with teleprompter formatting
- Build blog outline generator
- Add engagement target identification and comment angle suggestion
- Add Founder's Notebook prompt generation
- Add competitor content monitoring and snapshot generation
- Add discoverability tag generation (hashtags, SEO keywords)
- Generate first full report (PDF for auto mode, markdown for interactive)
- Test: Rejith reviews first full output, provides feedback on voice/quality

### Phase 3: Automation and Refinement (Week 5-6)

- Enable cron schedule (Monday 6:00 AM IST)
- Configure Gmail API for automated HTML email delivery
- Build posting calendar generator
- Add LinkedIn and industry source monitoring (beyond YouTube)
- Tune content quality based on first 2 weeks of feedback
- Establish success metric baselines
- Document interactive mode prompt templates for Claude project

---

## 18. Interactive Mode Prompt Templates

These are the standard prompts for invoking the workflow from the Claude project chat. The team can use these directly or modify as needed.

### Full Weekly Run
```
Run the weekly content report for lowtouch.ai. Analyze trends in enterprise agentic AI from the past 7 days and produce the full output: trend report, 3 YouTube ideas, 2-3 LinkedIn text posts, 2 LinkedIn carousels, 3 Instagram Reel scripts, 2 blog outlines, 10-15 engagement targets, Founder's Notebook prompt, competitor snapshot, and posting calendar with discoverability tags for all content.
```

### Partial Runs
```
Just give me the Instagram Reel scripts for this week.
```
```
What's trending in enterprise agentic AI this week? Just the trend summary.
```
```
Generate 2 LinkedIn carousel outlines based on this week's top trends.
```

### Refinement
```
Rewrite YouTube idea 2 with a stronger hook and more specific demo notes.
```
```
Give me 2 more engagement targets focused on CISOs discussing AI governance.
```
```
The contrarian LinkedIn post is too soft. Make it sharper and more opinionated.
```

---

## 19. Appendix: Channel Baselines

| Channel | Platform | Current Followers | Baseline Date |
|---|---|---|---|
| Rejith Krishnan (personal) | LinkedIn | TBD | To be established Week 1 |
| lowtouch.ai (company page) | LinkedIn | TBD | To be established Week 1 |
| lowtouch.ai | YouTube | ~900 subscribers | February 2026 |
| lowtouch.ai | Instagram | ~3,000 followers | February 2026 |

---

*This workflow is designed to make consistent, high-quality multi-channel presence sustainable without consuming significant founder time. The goal is approximately 1.5 to 2 hours per week from Rejith, with the system handling intelligence gathering, analysis, and first-draft content creation across all channels.*
