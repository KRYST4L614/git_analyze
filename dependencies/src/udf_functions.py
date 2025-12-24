# Все константы и функции вынесены в отдельный модуль
from typing import List

CORPORATE_INDICATORS = {
    'microsoft', 'google', 'apple', 'amazon', 'meta', 'facebook', 'twitter',
    'netflix', 'uber', 'airbnb', 'spotify', 'slack', 'github', 'gitlab',
    'oracle', 'ibm', 'intel', 'amd', 'nvidia', 'docker', 'kubernetes',
    'apache', 'mongodb', 'redis', 'elastic', 'splunk', 'datadog',
    'cisco', 'mozilla', 'jetbrains', 'atlassian', 'salesforce', 'adobe'
}

CORPORATE_SUFFIXES = {
    'inc', 'ltd', 'corp', 'corporation', 'company', 'tech', 'technologies',
    'software', 'systems', 'solutions', 'enterprise', 'studio', 'labs',
    'holdings', 'group', 'llc', 'gmbh', 'limited', 'co', 'org'
}

CORPORATE_TOPICS = {
    'api', 'sdk', 'framework', 'library', 'cloud', 'platform',
    'enterprise', 'production', 'backend', 'frontend', 'mobile',
    'web', 'database', 'server', 'client', 'toolkit', 'engine'
}

EDUCATIONAL_INDICATORS = {
    'course', 'tutorial', 'homework', 'assignment', 'student', 'lab',
    'exercise', 'project', 'university', 'college', 'school', 'learn',
    'training', 'education', 'academic', 'coursera', 'edx', 'udemy',
    'lesson', 'lecture', 'workshop', 'bootcamp', 'curriculum', 'syllabus',
    'exam', 'quiz', 'thesis', 'dissertation'
}

PERSONAL_INDICATORS = {
    'my', 'personal', 'private', 'test', 'demo', 'example', 'sandbox',
    'playground', 'experiment', 'trial', 'prototype', 'mock', 'sample',
    'blog', 'portfolio', 'website', 'resume', 'cv'
}

PERSONAL_NAMES = {
    'user', 'test', 'demo', 'example', 'temp', 'tmp', 'admin',
    'my', 'personal', 'private', 'backup', 'guest', 'anonymous'
}


def extract_organization_udf(full_name: str) -> str:
    if not full_name or '/' not in full_name:
        return None

    org = full_name.split('/')[0].lower().strip()

    if org in PERSONAL_NAMES or len(org) < 2:
        return None

    return org


def calculate_corporate_weight_udf(description: str, topics: List[str], org_name: str) -> float:
    weight = 0.0

    if org_name:
        org_lower = org_name.lower()

        for company in CORPORATE_INDICATORS:
            if company in org_lower:
                weight += 3.0
                break

        for suffix in CORPORATE_SUFFIXES:
            if org_lower.endswith(suffix) or f"-{suffix}" in org_lower:
                weight += 2.0
                break

    topic_weight = 0.0
    if topics:
        for topic in topics:
            topic_lower = topic.lower()
            for corp_topic in CORPORATE_TOPICS:
                if corp_topic in topic_lower:
                    topic_weight += 0.5

    weight += min(topic_weight, 2.0)

    if description:
        desc_lower = description.lower()
        desc_corp_indicators = 0
        for indicator in CORPORATE_INDICATORS:
            if indicator in desc_lower:
                desc_corp_indicators += 1

        weight += min(desc_corp_indicators * 0.5, 1.5)

    return min(weight, 10.0)


def calculate_educational_weight_udf(full_name: str, description: str, topics: List[str]) -> float:
    weight = 0.0

    text_parts = [full_name or "", description or ""]
    if topics:
        text_parts.extend(topics)

    text_to_check = " ".join(text_parts).lower()

    edu_matches = 0
    for indicator in EDUCATIONAL_INDICATORS:
        if indicator in text_to_check:
            edu_matches += 1

    if edu_matches >= 2:
        weight = edu_matches * 1.0
    elif edu_matches == 1:
        weight = 0.5

    return min(weight, 10.0)


def calculate_personal_weight_udf(full_name: str, description: str, topics: List[str],
                                  corp_weight: float, edu_weight: float) -> float:
    weight = 0.0

    text_parts = [full_name or "", description or ""]
    if topics:
        text_parts.extend(topics)

    text_to_check = " ".join(text_parts).lower()

    personal_matches = 0
    for indicator in PERSONAL_INDICATORS:
        if indicator in text_to_check:
            personal_matches += 1

    if not full_name or '/' not in full_name:
        weight += 2.0

    weight += personal_matches * 1.0

    if corp_weight < 1.0 and edu_weight < 1.0:
        weight += 2.0

    return min(weight, 10.0)


def determine_repository_type_udf(corp_weight: float, edu_weight: float, pers_weight: float):
    weights = {
        'corporate': corp_weight,
        'educational': edu_weight,
        'personal': pers_weight
    }

    max_type = max(weights.items(), key=lambda x: x[1])[0]
    max_weight = weights[max_type]
    total_weight = sum(weights.values())

    if max_weight < 1.0:
        return ('personal', 0.1)

    if max_type == 'corporate' and max_weight < 2.0:
        sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_weights) > 1 and sorted_weights[1][1] > 1.0:
            second_type = sorted_weights[1][0]
            confidence = sorted_weights[1][1] / total_weight if total_weight > 0 else 0.5
            return (second_type, confidence)
        return ('personal', 0.3)

    confidence = max_weight / total_weight if total_weight > 0 else 0.5
    return (max_type, min(confidence, 1.0))
