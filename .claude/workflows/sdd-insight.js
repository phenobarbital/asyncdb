export const meta = {
  name: 'sdd-insight',
  description: 'Two-layer AI-fluency + SDD analysis: Sonnet 4.6 explores the evidence (four personal AI-fluency competencies + one repo SDD-process lens), Opus 4.8 writes a skill map and an SDD process read grounded in the framework, then verifies both are evidence-grounded.',
  whenToUse: 'Run by the /sdd-insight command after scripts/sdd/insight.py emits evidence.json. Args: {evidence, framework} absolute paths.',
  phases: [
    { title: 'Explore', detail: 'Sonnet 4.6 — one explorer per 4D competency + one SDD-process explorer' },
    { title: 'Analyze', detail: 'Opus 4.8 — skill map + SDD process read grounded in the framework' },
    { title: 'Verify', detail: 'check the map + SDD read are grounded; repair if not' },
  ],
}

// Resolve inputs. The /sdd-insight command passes absolute paths via args, but if they don't
// arrive the defaults MUST still point at the exact files the command writes — never a bare
// relative path, which an agent will "helpfully" resolve to a stale copy elsewhere on disk.
const EV = (args && args.evidence) || '~/.claude/sdd-insight/evidence.json'
const FW = (args && args.framework) || 'reference/sdd-insight/ai-fluency-framework.md'

const COMPETENCIES = [
  { key: 'Delegation',  focus: 'What they hand to the agent vs keep, and how they split work: end-to-end hand-offs vs micro-stepping, sub-agents / background jobs / planning, tool breadth (platform & path awareness). Signals: delegation_events, tool_usage, scope of prompts.' },
  { key: 'Description',  focus: 'How concretely they brief the agent: do action prompts name a file/error (artifact), carry a constraint, and state a why/acceptance test? Terse offloading vs front-loaded specific briefs. Signals: Direction detail (constraint/artifact/intent rates) + sample prompts.' },
  { key: 'Discernment',  focus: 'How they evaluate outputs: verification after edit-bursts (tests/build/run), grounding edits in a prior read, correcting precisely (symptom + rule) vs vague rejection. Signals: Verification, Context, Iteration detail. NOTE agency: verification/grounding are partly Claude-driven — credit the USER moderately.' },
  { key: 'Diligence',    focus: 'Responsibility: verifying before things go live, tearing down what was spun up, owning the result rather than blind-shipping. In a coding transcript this overlaps Discernment — weight the responsibility angle. Signals: verification teardown bonus, grounded edits.' },
]

const FINDING = {
  type: 'object', additionalProperties: false,
  required: ['competency', 'level_estimate', 'confidence', 'strengths', 'gaps', 'evidence_quotes'],
  properties: {
    competency: { type: 'string' },
    level_estimate: { type: 'integer', minimum: 1, maximum: 5 },
    confidence: { type: 'string', enum: ['low', 'medium', 'high'] },
    strengths: { type: 'array', items: { type: 'string' } },
    gaps: { type: 'array', items: { type: 'string' } },
    evidence_quotes: { type: 'array', items: { type: 'string' }, description: 'real quotes/observations from the evidence' },
    notes: { type: 'string' },
  },
}

// The SDD-process explorer reasons about the REPO's workflow adherence (evidence.sdd), not
// the individual. Its finding feeds the analyst's sdd_read — framed as "the team/repo".
const SDD_FINDING = {
  type: 'object', additionalProperties: false,
  required: ['summary', 'strengths', 'gaps', 'evidence_metrics'],
  properties: {
    summary: { type: 'string' },
    strengths: { type: 'array', items: { type: 'string' } },
    gaps: { type: 'array', items: { type: 'string' }, description: 'concrete process gaps, e.g. low review coverage' },
    top_fix: { type: 'string', description: 'the single highest-leverage process fix for this repo' },
    evidence_metrics: { type: 'array', items: { type: 'string' }, description: 'real numbers cited from evidence.sdd' },
  },
}

const SKILL_ENTRY = {
  type: 'object', additionalProperties: false,
  required: ['competency', 'level', 'level_label', 'summary', 'evidence', 'next_move'],
  properties: {
    competency: { type: 'string', enum: ['Delegation', 'Description', 'Discernment', 'Diligence'] },
    level: { type: 'integer', minimum: 1, maximum: 5 },
    level_label: { type: 'string', enum: ['Emerging', 'Developing', 'Proficient', 'Advanced', 'Expert'] },
    summary: { type: 'string' },
    evidence: { type: 'array', items: { type: 'string' }, minItems: 1 },
    next_move: { type: 'string' },
  },
}

const ANALYSIS = {
  type: 'object', additionalProperties: false,
  required: ['overall_read', 'skill_map', 'top_growth', 'strengths'],
  properties: {
    overall_read: { type: 'string' },
    skill_map: { type: 'array', items: SKILL_ENTRY, minItems: 4, maxItems: 4 },
    top_growth: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['title', 'why', 'how', 'example_before', 'example_after'],
        properties: {
          title: { type: 'string' }, why: { type: 'string' }, how: { type: 'string' },
          example_before: { type: 'string' }, example_after: { type: 'string' },
        },
      },
    },
    strengths: { type: 'array', items: { type: 'string' } },
    // Layer 2: a written read of the REPO's SDD process discipline, grounded in evidence.sdd.
    // Empty string when the evidence carries no sdd block.
    sdd_read: { type: 'string', description: 'A grounded paragraph on the repo SDD workflow adherence (the team/repo, not "you"), naming the real sub-scores and the top process fix. Empty string if evidence.sdd is null.' },
  },
}

const VERDICT = {
  type: 'object', additionalProperties: false,
  required: ['is_grounded', 'ungrounded_claims'],
  properties: {
    is_grounded: { type: 'boolean' },
    ungrounded_claims: { type: 'array', items: { type: 'string' } },
    notes: { type: 'string' },
  },
}

const READ = `Using your Read tool, read EXACTLY these two files and use ONLY them — the framework at ` +
  `${FW} and the de-contaminated evidence bundle (JSON) at ${EV}. ` +
  `Do NOT search for, guess at, or substitute any other path: there may be stale evidence.json ` +
  `copies elsewhere on disk — ignore them entirely. If either ` +
  `file is missing, STOP and report that rather than reading a different one. (A leading ~ means your ` +
  `home directory — expand it to $HOME before reading.) The evidence is real and ` +
  `local; ground everything in it — quote real prompts; never invent.`

// ---- Explore: Sonnet 4.6, one explorer per competency + one SDD-process explorer -----
phase('Explore')
const findings = await parallel(COMPETENCIES.map(c => () =>
  agent(
    `${READ}\n\nYou are a careful, thorough analyst exploring ONE AI-fluency competency: ` +
    `**${c.key}**.\n${c.focus}\n\nUse the level rubric in the framework. Estimate a level (1–5), ` +
    `set confidence from how much evidence exists (hedge thin signals), and list concrete strengths, ` +
    `gaps, and real evidence quotes. Be specific to THIS person.`,
    { label: `explore:${c.key}`, phase: 'Explore', model: 'sonnet', schema: FINDING }
  ).then(f => f && { ...f, competency: f.competency || c.key })
)).then(r => r.filter(Boolean))

// The SDD-process lens reads the repo-level evidence.sdd block (deterministic adherence
// metrics). It returns a "no data" summary gracefully if there is no sdd block.
const sddFinding = await agent(
  `${READ}\n\nYou are a Spec-Driven-Development process auditor. Look ONLY at the evidence's ` +
  `top-level "sdd" object (overall, band, sub_scores: Pipeline / Decomposition / Acceptance / ` +
  `Closure / Review, and metrics with real counts). This describes how the REPO/TEAM runs its SDD ` +
  `workflow — NOT any individual. If "sdd" is null or missing, return a summary of "no SDD data ` +
  `available" and empty lists. Otherwise: summarize the repo's process health, list real strengths ` +
  `and concrete gaps (call out the lowest sub-score — e.g. weak review coverage — with its real ` +
  `numbers), and name the single highest-leverage process fix. Cite the actual metric values.`,
  { label: 'explore:Process', phase: 'Explore', model: 'sonnet', schema: SDD_FINDING }
)

log(`Explored ${findings.length}/4 competencies + SDD process (Sonnet 4.6)`)

// ---- Analyze: Opus 4.8 writes the grounded skill map + SDD read --------------------
phase('Analyze')
const findingsJson = JSON.stringify(findings, null, 2)
const sddFindingJson = JSON.stringify(sddFinding, null, 2)
const analystPrompt =
  `${READ}\n\nYou are the senior AI-fluency assessor (write like a kind, exacting teacher). ` +
  `Four Sonnet explorers produced these competency findings:\n\n${findingsJson}\n\n` +
  `A fifth explorer audited the repo's SDD process discipline:\n\n${sddFindingJson}\n\n` +
  `Reconcile the four competencies with the deterministic scores in the evidence bundle and the ` +
  `framework's level rubric and "what good looks like". Produce the final assessment per the ` +
  `framework's OUTPUT CONTRACT: an overall_read, a skill_map with EXACTLY the four competencies ` +
  `(Delegation, Description, Discernment, Diligence) in that order, top_growth, and strengths. ` +
  `\n\nThe top_growth section is the heart of the personal report — it MUST be fully custom, never ` +
  `generic advice. Produce 3 items (2 only if data is thin). For each: a sharp title; a "why" that ` +
  `cites THIS person's own numbers/pattern; a concrete "how"; and before/after where example_before ` +
  `is a REAL prompt copied VERBATIM from the evidence's sample_prompts or weak_examples (do not invent ` +
  `or paraphrase) and example_after is your tailored rewrite of THAT exact prompt. Name their files, ` +
  `tools, projects. No example may repeat. Respect agency (discount Claude-driven habits) and ` +
  `confidence (hedge thin signals). ` +
  `\n\nFINALLY, set sdd_read: if evidence.sdd is null, set it to an empty string. Otherwise write ONE ` +
  `grounded paragraph on the REPO's SDD workflow adherence — say "the repo"/"the team", never "you"; ` +
  `name the band and the real sub-scores; call out the weakest dimension with its numbers; and state ` +
  `the single highest-leverage process fix. Ground every personal claim AND the sdd_read in the evidence.`
let analysis = await agent(analystPrompt, { label: 'analyze', phase: 'Analyze', model: 'opus', schema: ANALYSIS })

// ---- Verify: is the map (and SDD read) actually grounded? repair once if not -------
phase('Verify')
const verdict = await agent(
  `${READ}\n\nAdversarially check this assessment against the evidence. Flag any claim that ` +
  `is generic, ungrounded, inflated, or ignores low confidence — including the sdd_read (it must cite ` +
  `the real sdd sub-scores and never address an individual as "you"). Default to is_grounded=false ` +
  `if unsure.\n\nASSESSMENT:\n${JSON.stringify(analysis, null, 2)}`,
  { label: 'verify', phase: 'Verify', model: 'opus', schema: VERDICT }
)
if (verdict && verdict.is_grounded === false && (verdict.ungrounded_claims || []).length) {
  log(`Repairing ${verdict.ungrounded_claims.length} ungrounded claim(s)`)
  analysis = await agent(
    `${READ}\n\nRevise this assessment to fix these grounding problems — replace generic/inflated/ungrounded ` +
    `statements with evidence-grounded, appropriately-hedged ones. Keep the same JSON shape.\n\n` +
    `PROBLEMS:\n${(verdict.ungrounded_claims || []).map((c, i) => `${i + 1}. ${c}`).join('\n')}\n\n` +
    `CURRENT:\n${JSON.stringify(analysis, null, 2)}`,
    { label: 'repair', phase: 'Verify', model: 'opus', schema: ANALYSIS }
  )
}

return analysis
