import multiprocessing as mp
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, List, Optional, Any

import pandas as pd

from src.analysis.clustering.type.repo.repo import AnalysisRepository
from src.storage.unit_of_work import UnitOfWork

warnings.filterwarnings('ignore')


class RepositoryClassifier:
    def __init__(self, database_url: str, n_workers: int = None):
        self.database_url = database_url
        self.n_workers = n_workers or mp.cpu_count()

        self.uow = UnitOfWork(self.database_url)
        self.uow.create_type_clustering_tables()

        self.analysis_repo = AnalysisRepository(self.database_url)
        self.analysis_repo.uow = self.uow

        self.corporate_indicators = {
            'microsoft', 'google', 'apple', 'amazon', 'meta', 'facebook', 'twitter',
            'netflix', 'uber', 'airbnb', 'spotify', 'slack', 'github', 'gitlab',
            'oracle', 'ibm', 'intel', 'amd', 'nvidia', 'docker', 'kubernetes',
            'apache', 'mongodb', 'redis', 'elastic', 'splunk', 'datadog',
            'cisco', 'mozilla', 'jetbrains', 'atlassian', 'salesforce', 'adobe'
        }

        self.corporate_suffixes = {
            'inc', 'ltd', 'corp', 'corporation', 'company', 'tech', 'technologies',
            'software', 'systems', 'solutions', 'enterprise', 'studio', 'labs',
            'holdings', 'group', 'llc', 'gmbh', 'limited', 'co', 'org'
        }

        self.corporate_topics = {
            'api', 'sdk', 'framework', 'library', 'cloud', 'platform',
            'enterprise', 'production', 'backend', 'frontend', 'mobile',
            'web', 'database', 'server', 'client', 'toolkit', 'engine'
        }

        self.educational_indicators = {
            'course', 'tutorial', 'homework', 'assignment', 'student', 'lab',
            'exercise', 'project', 'university', 'college', 'school', 'learn',
            'training', 'education', 'academic', 'coursera', 'edx', 'udemy',
            'lesson', 'lecture', 'workshop', 'bootcamp', 'curriculum', 'syllabus',
            'exam', 'quiz', 'thesis', 'dissertation'
        }

        self.personal_indicators = {
            'my', 'personal', 'private', 'test', 'demo', 'example', 'sandbox',
            'playground', 'experiment', 'trial', 'prototype', 'mock', 'sample',
            'blog', 'portfolio', 'website', 'resume', 'cv'
        }

    def __del__(self):
        if hasattr(self, 'uow'):
            self.uow.dispose()

    def map_extract_textual_data(self, chunk: pd.DataFrame) -> List[Dict]:
        results = []

        for _, row in chunk.iterrows():
            repo_id = row['repo_id']
            full_name = str(row['full_name'] or '')
            description = str(row['description'] or '')
            topics = row['topics'] or []
            owner_login = str(row['owner_login'] or '')

            org_name = self._extract_organization(full_name)

            corporate_weight = self._calculate_corporate_weight(description, topics, org_name)
            educational_weight = self._calculate_educational_weight(full_name, description, topics)
            personal_weight = self._calculate_personal_weight(full_name, description, topics)

            repo_type, confidence = self._determine_repository_type(
                corporate_weight, educational_weight, personal_weight
            )

            results.append({
                'repo_id': repo_id,
                'full_name': full_name,
                'org_name': org_name,
                'corporate_weight': corporate_weight,
                'educational_weight': educational_weight,
                'personal_weight': personal_weight,
                'final_type': repo_type,
                'confidence_score': confidence,
                'topics': topics,
                'owner_login': owner_login
            })

        return results

    def _determine_repository_type(self, corp_weight: float, edu_weight: float, pers_weight: float) -> tuple[
        str, float]:
        weights = {
            'corporate': corp_weight,
            'educational': edu_weight,
            'personal': pers_weight
        }

        max_type = max(weights.items(), key=lambda x: x[1])[0]
        max_weight = weights[max_type]
        total_weight = sum(weights.values())

        if max_weight < 1.0:
            return 'personal', 0.1

        if max_type == 'corporate' and max_weight < 2.0:
            sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
            if len(sorted_weights) > 1 and sorted_weights[1][1] > 1.0:
                second_type = sorted_weights[1][0]
                confidence = sorted_weights[1][1] / total_weight if total_weight > 0 else 0.5
                return second_type, confidence
            return 'personal', 0.3

        confidence = max_weight / total_weight if total_weight > 0 else 0.5
        return max_type, min(confidence, 1.0)

    def reduce_textual_features(self, mapped_results: List[List[Dict]]) -> pd.DataFrame:
        all_features = []
        for chunk_results in mapped_results:
            all_features.extend(chunk_results)

        return pd.DataFrame(all_features)

    def load_data_in_chunks(self, chunk_size: int = 1000) -> List[pd.DataFrame]:
        print("Loading data in chunks...")

        chunks = self.analysis_repo.load_repository_data(chunk_size)

        print(f"Loaded {len(chunks)} chunks, total {sum(len(chunk) for chunk in chunks)} repositories")
        return chunks

    def parallel_text_processing(self, chunks: List[pd.DataFrame]) -> pd.DataFrame:
        print("Parallel classification of repositories...")

        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            map_func = partial(self.map_extract_textual_data)
            mapped_results = list(executor.map(map_func, chunks))

        results_df = self.reduce_textual_features(mapped_results)

        print(f"Processed {len(results_df)} repositories")
        return results_df

    def _extract_organization(self, full_name: str) -> Optional[str]:
        if not isinstance(full_name, str) or '/' not in full_name:
            return None

        org = full_name.split('/')[0].lower().strip()

        personal_names = {
            'user', 'test', 'demo', 'example', 'temp', 'tmp', 'admin',
            'my', 'personal', 'private', 'backup', 'guest', 'anonymous'
        }

        if org in personal_names or len(org) < 2:
            return None

        return org

    def _calculate_corporate_weight(self, description: str, topics: List[str], org_name: str) -> float:
        weight = 0.0

        if org_name:
            org_lower = org_name.lower()

            for company in self.corporate_indicators:
                if company in org_lower:
                    weight += 3.0
                    break

            for suffix in self.corporate_suffixes:
                if org_lower.endswith(suffix) or f"-{suffix}" in org_lower:
                    weight += 2.0
                    break

        topic_weight = 0.0
        for topic in topics:
            topic_lower = topic.lower()
            for corp_topic in self.corporate_topics:
                if corp_topic in topic_lower:
                    topic_weight += 0.5

        weight += min(topic_weight, 2.0)

        description_lower = description.lower()
        desc_corp_indicators = 0
        for indicator in self.corporate_indicators:
            if indicator in description_lower:
                desc_corp_indicators += 1

        weight += min(desc_corp_indicators * 0.5, 1.5)

        return min(weight, 10.0)

    def _calculate_educational_weight(self, full_name: str, description: str, topics: List[str]) -> float:
        weight = 0.0

        text_to_check = f"{full_name} {description} {' '.join(topics)}".lower()

        edu_matches = 0
        for indicator in self.educational_indicators:
            if indicator in text_to_check:
                edu_matches += 1

        if edu_matches >= 2:
            weight = edu_matches * 1.0
        elif edu_matches == 1:
            weight = 0.5

        return min(weight, 10.0)

    def _calculate_personal_weight(self, full_name: str, description: str, topics: List[str]) -> float:
        weight = 0.0

        text_to_check = f"{full_name} {description} {' '.join(topics)}".lower()

        personal_matches = 0
        for indicator in self.personal_indicators:
            if indicator in text_to_check:
                personal_matches += 1

        if '/' not in full_name:
            weight += 2.0

        weight += personal_matches * 1.0

        corp_weight = self._calculate_corporate_weight(description, topics, self._extract_organization(full_name))
        edu_weight = self._calculate_educational_weight(full_name, description, topics)

        if corp_weight < 1.0 and edu_weight < 1.0:
            weight += 2.0

        return min(weight, 10.0)

    def analyze_results(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        print("Analyzing results...")

        type_distribution = results_df['final_type'].value_counts()

        type_stats = results_df.groupby('final_type').agg({
            'corporate_weight': 'mean',
            'educational_weight': 'mean',
            'personal_weight': 'mean',
            'confidence_score': 'mean',
            'repo_id': 'count'
        }).rename(columns={'repo_id': 'count'})

        confidence_stats = {
            'high_confidence': len(results_df[results_df['confidence_score'] > 0.7]),
            'medium_confidence': len(
                results_df[(results_df['confidence_score'] > 0.4) & (results_df['confidence_score'] <= 0.7)]),
            'low_confidence': len(results_df[results_df['confidence_score'] <= 0.4])
        }

        analysis_results = {
            'type_distribution': type_distribution.to_dict(),
            'type_stats': type_stats.to_dict(),
            'confidence_stats': confidence_stats,
            'total_repositories': len(results_df),
            'avg_confidence': results_df['confidence_score'].mean()
        }

        return analysis_results

    def run_analysis(self, chunk_size: int = 1000) -> Dict[str, Any]:
        print("Starting rule-based repository analysis...")
        print(f"Parameters: {self.n_workers} workers")

        try:
            chunks = self.load_data_in_chunks(chunk_size)

            if not chunks:
                return {"error": "No data available for analysis"}

            results_df = self.parallel_text_processing(chunks)

            print("\nSample repository classification:")
            sample = results_df[['full_name', 'final_type', 'confidence_score',
                                 'corporate_weight', 'educational_weight', 'personal_weight']].head(10)
            for _, row in sample.iterrows():
                org_name = self._extract_organization(row['full_name'])
                print(f"  {row['full_name']}")
                print(f"    Type: {row['final_type']} (confidence: {row['confidence_score']:.2f})")
                print(f"    Org: {org_name}, Weights: corp={row['corporate_weight']:.1f}, "
                      f"edu={row['educational_weight']:.1f}, pers={row['personal_weight']:.1f}")

            analysis = self.analyze_results(results_df)

            self.analysis_repo.save_clustering_results(results_df)

            self._print_results_summary(analysis)

            return {
                'analysis': analysis,
                'processed_repositories': len(results_df)
            }

        except Exception as e:
            print(f"Error during analysis: {e}")
            return {"error": str(e)}
        finally:
            self.uow.dispose()

    def _print_results_summary(self, analysis: Dict):
        print("\nCLASSIFICATION RESULTS SUMMARY")
        print("=" * 45)

        print("Type Distribution:")
        for repo_type, count in analysis['type_distribution'].items():
            percentage = (count / analysis['total_repositories']) * 100
            print(f"  {repo_type.upper():<12}: {count:>4} repositories ({percentage:>5.1f}%)")

        print(f"\nConfidence Statistics:")
        print(f"  High confidence (>0.7):   {analysis['confidence_stats']['high_confidence']:>4}")
        print(f"  Medium confidence (0.4-0.7): {analysis['confidence_stats']['medium_confidence']:>4}")
        print(f"  Low confidence (<=0.4):   {analysis['confidence_stats']['low_confidence']:>4}")
        print(f"  Average confidence: {analysis['avg_confidence']:.2f}")

        print(f"\nTotal processed: {analysis['total_repositories']} repositories")


class RepositoryAnalyzer:
    def __init__(self, database_url: str, n_workers: int = None):
        UnitOfWork(database_url).create_type_clustering_tables()
        self.classifier = RepositoryClassifier(database_url, n_workers)

    def analyze(self) -> Dict:
        return self.classifier.run_analysis()
