"""Схлопывание пересказов одного события — ДО вызова LLM.

Восемь изданий пишут об одном листинге восемью заголовками. Если отправить их в
классификатор как восемь разных фактов, «уверенность» вырастет на пустом месте, а
вызов подорожает. Похожесть считается по шинглам нормализованного текста.
"""

import re
from typing import Dict, List

STOP_WORDS = {
    "the", "a", "an", "of", "in", "on", "for", "to", "and", "is", "are", "as", "at",
    "by", "with", "from", "its", "it", "be", "will", "has", "have", "after", "over",
    "и", "в", "на", "по", "для", "с", "из", "за", "о", "об", "как", "что",
}


def stem(word: str) -> str:
    """Грубое усечение окончаний: «list», «lists», «listing» — об одном событии."""
    for suffix in ("ing", "ed", "es", "s"):
        if len(word) > len(suffix) + 3 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def normalize(text: str) -> List[str]:
    text = (text or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    return [stem(w) for w in text.split() if w and w not in STOP_WORDS]


def shingles(words: List[str], size: int) -> set:
    if len(words) < size:
        return {" ".join(words)} if words else set()
    return {" ".join(words[i:i + size]) for i in range(len(words) - size + 1)}


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def similarity(entry_a: dict, entry_b: dict) -> float:
    """Максимум из похожести по шинглам и по множеству слов.

    Только шингли на коротких заголовках почти не пересекаются: «Upbit will list X»
    и «Upbit to list X in KRW» дают разные триграммы при одном и том же событии.
    Множество слов эту разницу переживает.
    """
    return max(jaccard(entry_a["shingles"], entry_b["shingles"]),
               jaccard(entry_a["words"], entry_b["words"]))


def polarity(words: set, markers) -> frozenset:
    """Метки противоположного смысла: листинг и делистинг склеивать нельзя."""
    return frozenset(m for m in markers if any(m in w for w in words))


def cluster(items: List[dict], cfg: dict, text_key: str = "title") -> List[Dict]:
    """Группирует похожие заголовки. Возвращает по одному представителю на событие.

    Представителем становится элемент с наиболее авторитетным источником
    (`source_tier`, если есть), иначе самый ранний — остальные едут в `duplicates`.
    """
    size = int(cfg["dedup"]["shingle_size"])
    threshold = float(cfg["dedup"]["similarity_threshold"])

    markers = cfg["dedup"].get("opposite_markers") or []
    prepared = []
    for item in items:
        words = normalize(item.get(text_key, ""))
        word_set = set(words)
        prepared.append({"item": item, "shingles": shingles(words, size), "words": word_set,
                         "polarity": polarity(word_set, markers)})

    clusters: List[dict] = []
    for entry in prepared:
        for group in clusters:
            if entry["polarity"] != group["polarity"]:
                continue     # «листинг» и «делистинг» об одной монете — разные события
            # сравнение с каждым участником, а не с их объединением: объединение
            # растёт при склейке и искусственно занижает похожесть следующих
            if any(similarity(entry, member) >= threshold for member in group["entries"]):
                group["entries"].append(entry)
                group["members"].append(entry["item"])
                break
        else:
            clusters.append({"polarity": entry["polarity"], "entries": [entry],
                             "members": [entry["item"]]})

    out = []
    for group in clusters:
        members = group["members"]
        best = min(members, key=lambda m: (m.get("source_tier", 3), m.get("ts", 0)))
        out.append({**best, "duplicates": len(members) - 1,
                    "duplicate_titles": [m.get(text_key) for m in members if m is not best]})
    return out
