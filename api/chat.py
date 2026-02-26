from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# Solo regole comportamentali e stile.
_QUICK_SYSTEM = (
    "Rispondi in modo conciso, chiaro e orientato all'azione. "
    "Se i dati non bastano, dichiaralo esplicitamente e indica cosa manca. "
    "Non inventare fonti o numeri; usa solo il contesto fornito."
)


def _safe_parse_date(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        candidate = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _article_text_len(article: dict[str, Any]) -> int:
    text = " ".join(
        str(article.get(key, ""))
        for key in ("title", "summary", "content", "snippet", "body")
    )
    return len(text.strip())


def _article_relevance(article: dict[str, Any]) -> float:
    for key in ("relevance", "score", "rank"):
        value = article.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def _select_corpus_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not articles:
        return []

    avg_len = sum(max(1, _article_text_len(a)) for a in articles) / len(articles)

    # Limite dinamico: meno articoli se mediamente molto lunghi.
    if avg_len <= 500:
        limit = min(14, len(articles))
    elif avg_len <= 1200:
        limit = min(10, len(articles))
    elif avg_len <= 2400:
        limit = min(7, len(articles))
    else:
        limit = min(5, len(articles))

    now = datetime.now(timezone.utc)

    def sort_key(article: dict[str, Any]) -> tuple[float, float]:
        date = (
            _safe_parse_date(article.get("published_at"))
            or _safe_parse_date(article.get("date"))
            or _safe_parse_date(article.get("created_at"))
        )
        if date is None:
            recency = 0.0
        else:
            age_days = max(0.0, (now - date).total_seconds() / 86400.0)
            recency = 1.0 / (1.0 + age_days)
        return (_article_relevance(article), recency)

    ranked = sorted(articles, key=sort_key, reverse=True)
    return ranked[:limit]


def _history_window(corpus_articles: list[dict[str, Any]], default_window: int = 10) -> int:
    corpus_chars = sum(_article_text_len(article) for article in corpus_articles)
    if corpus_chars > 18000:
        return min(default_window, 3)
    if corpus_chars > 12000:
        return min(default_window, 5)
    if corpus_chars > 8000:
        return min(default_window, 7)
    return default_window


def _format_context_message(
    statistics: str | dict[str, Any] | None,
    corpus_articles: list[dict[str, Any]],
) -> str:
    parts: list[str] = []

    if statistics:
        stats_text = statistics if isinstance(statistics, str) else str(statistics)
        parts.append("STATISTICHE:\n" + stats_text)

    if corpus_articles:
        lines = ["CORPUS:"]
        for idx, article in enumerate(corpus_articles, start=1):
            title = article.get("title", "(senza titolo)")
            date = article.get("published_at") or article.get("date") or "n/d"
            snippet = (
                article.get("summary")
                or article.get("snippet")
                or article.get("content")
                or ""
            )
            snippet = str(snippet).strip().replace("\n", " ")[:500]
            lines.append(f"{idx}. [{date}] {title} — {snippet}")
        parts.append("\n".join(lines))

    return "\n\n".join(parts).strip()


def _quick_answer(
    user_question: str,
    history: list[dict[str, str]] | None = None,
    statistics: str | dict[str, Any] | None = None,
    articles: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Costruisce i messaggi per una risposta rapida mantenendo il contesto entro limiti sicuri."""

    history = history or []
    articles = articles or []

    corpus_articles = _select_corpus_articles(articles)
    window = _history_window(corpus_articles)
    trimmed_history = history[-window:] if window > 0 else []

    messages: list[dict[str, str]] = [{"role": "system", "content": _QUICK_SYSTEM}]
    messages.extend(trimmed_history)

    context_payload = _format_context_message(statistics, corpus_articles)
    if context_payload:
        # STATISTICHE + CORPUS come messaggio user separato, prima della domanda.
        messages.append({"role": "user", "content": context_payload})

    messages.append({"role": "user", "content": user_question})
    return messages
