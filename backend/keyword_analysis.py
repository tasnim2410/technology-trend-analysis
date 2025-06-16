# import nltk
# import string
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
# from nltk.stem import PorterStemmer, WordNetLemmatizer
# from sklearn.feature_extraction.text import TfidfVectorizer
# #preprocessing : lowercase and remove puctuation , remove stop words and perform stemming an lemmatization 
# # Initialize stemmer and lemmatizer
# import re
# import string
# import unicodedata

# import contractions
# import nltk
# from nltk.corpus import stopwords
# from nltk.stem import PorterStemmer, WordNetLemmatizer
# from nltk.tokenize import word_tokenize

# import numpy as np
# import pandas as pd
# from sklearn.decomposition import LatentDirichletAllocation
# from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
# from scipy.stats import entropy
# import matplotlib.pyplot as plt
# from wordcloud import WordCloud
# from collections import defaultdict
# import seaborn as sns

# stemmer = PorterStemmer()
# lemmatizer = WordNetLemmatizer()
# # def preprocess_text(text: str,use_stemming: bool = True) -> str:
# #     # Lowercase and remove punctuation
# #     text = text.lower().translate(str.maketrans('', '', string.punctuation))
# #     tokens = word_tokenize(text)
# #     # Remove stopwords
# #     tokens = [word for word in tokens if word not in stopwords.words('english')]
# #     if use_stemming:
# #         tokens = [stemmer.stem(word) for word in tokens]
# #     else:
# #         tokens = [lemmatizer.lemmatize(word) for word in tokens]
# #     return " ".join(tokens)
  


# # Make sure you’ve run these once in your environment:
# # nltk.download("punkt")
# # nltk.download("stopwords")
# # nltk.download("wordnet")

# # Pre-instantiate to avoid rebuilding on each call:
# _stop_words = set(stopwords.words("english"))
# _stemmer    = PorterStemmer()
# _lemmatizer = WordNetLemmatizer()


# def preprocess_text(text: str, use_stemming: bool = True) -> str:
#     """
#     1) Unicode‐normalize and lowercase.
#     2) Expand contractions (“don’t” → “do not”).
#     3) Strip HTML, URLs, emails.
#     4) Remove punctuation, digits, extra whitespace.
#     5) Tokenize, drop non-alphabetic, stopwords, or tokens with len <= 2.
#     6) Either stem or lemmatize.
#     """
#     # ——— 1) Unicode normalize & lowercase ———
#     text = unicodedata.normalize("NFKC", text)
#     text = text.lower()

#     # ——— 2) Expand contractions ———
#     # contractions.fix will transform “don’t” → “do not”, etc.
#     text = contractions.fix(text)

#     # ——— 3) Remove HTML tags, URLs, emails ———
#     #   a) HTML tags
#     text = re.sub(r"<[^>]+>", " ", text)
#     #   b) URLs (http://xxx or www.xxx)
#     text = re.sub(r"http\S+|www\.\S+", " ", text)
#     #   c) Email addresses
#     text = re.sub(r"\S+@\S+", " ", text)

#     # ——— 4) Remove punctuation, digits ———
#     #   a) strip punctuation
#     text = text.translate(str.maketrans("", "", string.punctuation))
#     #   b) strip digits
#     text = re.sub(r"\d+", " ", text)
#     #   c) collapse multiple spaces into one
#     text = re.sub(r"\s+", " ", text).strip()

#     # ——— 5) Tokenize & drop “bad” tokens ———
#     tokens = word_tokenize(text)
#     filtered_tokens = []
#     for token in tokens:
#         # keep only purely alphabetic tokens, length > 2, and not a stopword
#         if token.isalpha() and len(token) > 2 and token not in _stop_words:
#             filtered_tokens.append(token)

#     # ——— 6) Stem or Lemmatize ———
#     if use_stemming:
#         processed = [_stemmer.stem(tok) for tok in filtered_tokens]
#     else:
#         processed = [_lemmatizer.lemmatize(tok) for tok in filtered_tokens]

#     return " ".join(processed)



# def extract_keywords(texts, max_df=0.85, min_df=2, ngram_range=(1, 3), top_n=10):
#     """
#     Extracts the top N keywords from a list of texts using TF-IDF.

#     Parameters:
#     - texts: List of preprocessed texts (e.g., abstracts).
#     - max_df: Float, ignore terms that appear in more than this fraction of documents.
#     - min_df: Int, ignore terms that appear in fewer than this number of documents.
#     - ngram_range: Tuple, the lower and upper boundary of the range of n-values for n-grams.
#     - top_n: Int, number of top keywords to return.

#     Returns:
#     - List of tuples, each containing a keyword and its average TF-IDF score, sorted by score in descending order.
#     """
#     vectorizer = TfidfVectorizer(max_df=max_df, min_df=min_df, ngram_range=ngram_range)
#     tfidf_matrix = vectorizer.fit_transform(texts)
#     feature_names = vectorizer.get_feature_names_out()
#     avg_scores = tfidf_matrix.mean(axis=0).tolist()[0]
#     keywords = sorted(zip(feature_names, avg_scores), key=lambda x: x[1], reverse=True)
#     return keywords[:top_n]


# def analyze_topic_evolution(keyword_df):
#     """
#     Analyzes topic evolution over time in patent data using adaptive window detection and LDA topic modeling
    
#     Parameters:
#     keyword_df (pd.DataFrame): DataFrame containing patent data with columns:
#         - 'Title': Patent titles
#         - 'first filing year': Filing years
        
#     Returns:
#     tuple: (topic_evolution, windows) where:
#         - topic_evolution: List of dictionaries containing topic information
#         - windows: List of detected time windows
#     """
#     # ======================
#     # CONFIGURATION
#     # ======================
#     config = {
#         'min_year': 2000,
#         'min_docs_per_year': 2,
#         'global_min_df': 1,
#         'global_max_df': 0.9,
#         'divergence_percentile': 80,
#         'max_topics': 5,
#         'top_words_per_topic': 10,
#         'min_window_span': 1
#     }

#     # ======================
#     # UTILITY FUNCTIONS
#     # ======================
#     def jensen_shannon(p, q):
#         m = 0.5 * (p + q)
#         return 0.5 * (entropy(p, m) + entropy(q, m))

#     def safe_divide(a, b):
#         return np.divide(a, b, out=np.zeros_like(a), where=b != 0)

#     # ======================
#     # DATA PREPARATION
#     # ======================
#     keyword_df = keyword_df[keyword_df['first filing year'] >= config['min_year']]
#     keyword_df = keyword_df.sort_values('first filing year')

#     docs = keyword_df['Title'].str.lower().str.replace(r'[^\w\s-]', '', regex=True).tolist()
#     years = keyword_df['first filing year'].astype(int).values

#     # ======================
#     # GLOBAL VOCABULARY
#     # ======================
#     global_vectorizer = CountVectorizer(
#         min_df=config['global_min_df'],
#         max_df=config['global_max_df'],
#         ngram_range=(1, 2),
#         stop_words='english'
#     )
#     global_vocab = global_vectorizer.fit(docs)

#     # ======================
#     # ADAPTIVE WINDOW DETECTION
#     # ======================
#     yearly_counts = []
#     valid_years = []

#     unique_years = np.unique(years)
#     for year in unique_years:
#         year_mask = years == year
#         num_docs = sum(year_mask)
#         if num_docs >= config['min_docs_per_year']:
#             try:
#                 counts = global_vectorizer.transform(np.array(docs)[year_mask]).sum(axis=0).A1
#                 yearly_counts.append((year, counts))
#                 valid_years.append(year)
#             except Exception:
#                 continue

#     divergences = []
#     for i in range(1, len(yearly_counts)):
#         prev = yearly_counts[i-1][1] + 1e-12
#         curr = yearly_counts[i][1] + 1e-12
#         prev = safe_divide(prev, prev.sum())
#         curr = safe_divide(curr, curr.sum())
#         div = jensen_shannon(prev, curr)
#         divergences.append(div)

#     threshold = np.percentile(divergences, config['divergence_percentile']) if divergences else 0
#     change_points = [0] + [i for i, d in enumerate(divergences) if d > threshold] + [len(yearly_counts)]
#     change_points = sorted(list(set(change_points)))
    
#     windows = []
#     for i in range(1, len(change_points)):
#         start_idx = change_points[i-1]
#         end_idx = change_points[i]
#         window_years = [int(yearly_counts[j][0]) for j in range(start_idx, end_idx)]
#         if not window_years:
#             continue
#         window_start = int(min(window_years))
#         window_end = int(max(window_years))
#         if (window_end - window_start) < config['min_window_span']:
#             continue
#         windows.append({
#             'start': window_start,
#             'end': window_end,
#             'years': window_years
#         })

#     # ======================
#     # TOPIC MODELING
#     # ======================
#     tfidf_vectorizer = TfidfVectorizer(
#         vocabulary=global_vectorizer.vocabulary_,
#         ngram_range=(1, 2)
#     )
#     full_matrix = tfidf_vectorizer.fit_transform(docs)
#     feature_names = tfidf_vectorizer.get_feature_names_out()

#     topic_evolution = []
#     for window in windows:
#         window_mask = keyword_df['first filing year'].isin(window['years']).values
#         window_matrix = full_matrix[window_mask]
        
#         if window_matrix.shape[0] == 0:
#             continue
            
#         n_topics = max(1, min(config['max_topics'], window_matrix.shape[0] // 50))
        
#         try:
#             lda = LatentDirichletAllocation(
#                 n_components=n_topics,
#                 random_state=42,
#                 learning_method='online'
#             )
#             lda.fit(window_matrix)
            
#             for topic_idx, topic in enumerate(lda.components_):
#                 top_indices = topic.argsort()[-config['top_words_per_topic']:][::-1]
#                 top_words = [feature_names[i] for i in top_indices]
#                 topic_evolution.append({
#                     'start': int(window['start']),
#                     'end': int(window['end']),
#                     'topic_id': f"{int(window['start'])}-{topic_idx+1}",
#                     'words': top_words,
#                     'weights': topic[top_indices].tolist()
#                 })
#         except Exception:
#             continue

#     return topic_evolution, windows ,divergences


import nltk
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
import re
import string
import unicodedata
import contractions
import numpy as np
import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from scipy.stats import entropy
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import defaultdict
import seaborn as sns

stemmer = PorterStemmer()
lemmatizer = WordNetLemmatizer()

_stop_words = set(stopwords.words("english")) | set(stopwords.words("french"))
_stemmer    = PorterStemmer()
_lemmatizer = WordNetLemmatizer()

def preprocess_text(text: str, use_stemming: bool = True) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = contractions.fix(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"http\S+|www\.\S+", " ", text)
    text = re.sub(r"\S+@\S+", " ", text)
    text = text.translate(str.maketrans("", "", string.punctuation))
    text = re.sub(r"\d+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = word_tokenize(text)
    filtered_tokens = [token for token in tokens if token.isalpha() and len(token) > 2 and token not in _stop_words]
    if use_stemming:
        processed = [_stemmer.stem(tok) for tok in filtered_tokens]
    else:
        processed = [_lemmatizer.lemmatize(tok) for tok in filtered_tokens]
    return " ".join(processed)

def extract_keywords(texts, max_df=0.85, min_df=2, ngram_range=(1, 3), top_n=10):
    vectorizer = TfidfVectorizer(max_df=max_df, min_df=min_df, ngram_range=ngram_range)
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    avg_scores = tfidf_matrix.mean(axis=0).tolist()[0]
    keywords = sorted(zip(feature_names, avg_scores), key=lambda x: x[1], reverse=True)
    return keywords[:top_n]

def analyze_topic_evolution(keyword_df):
    config = {
        'min_year': 2000,
        'min_docs_per_year': 2,
        'global_min_df': 1,
        'global_max_df': 0.9,
        'divergence_percentile': 80,
        'max_topics': 5,
        'top_words_per_topic': 10,
        'min_window_span': 1
    }

    def jensen_shannon(p, q):
        m = 0.5 * (p + q)
        return 0.5 * (entropy(p, m) + entropy(q, m))

    def safe_divide(a, b):
        return np.divide(a, b, out=np.zeros_like(a), where=b != 0)

    keyword_df = keyword_df[keyword_df['first filing year'] >= config['min_year']]
    keyword_df = keyword_df.sort_values('first filing year')
    docs = keyword_df['Title'].str.lower().str.replace(r'[^\w\s-]', '', regex=True).tolist()
    years = keyword_df['first filing year'].astype(int).values

    global_vectorizer = CountVectorizer(
        min_df=config['global_min_df'],
        max_df=config['global_max_df'],
        ngram_range=(1, 2),
        stop_words='english'
    )
    global_vocab = global_vectorizer.fit(docs)

    yearly_counts = []
    valid_years = []
    unique_years = np.unique(years)
    for year in unique_years:
        year_mask = years == year
        num_docs = sum(year_mask)
        if num_docs >= config['min_docs_per_year']:
            try:
                counts = global_vectorizer.transform(np.array(docs)[year_mask]).sum(axis=0).A1
                yearly_counts.append((year, counts))
                valid_years.append(year)
            except Exception:
                continue

    divergences = []
    for i in range(1, len(yearly_counts)):
        prev = yearly_counts[i-1][1] + 1e-12
        curr = yearly_counts[i][1] + 1e-12
        prev = safe_divide(prev, prev.sum())
        curr = safe_divide(curr, curr.sum())
        div = jensen_shannon(prev, curr)
        divergences.append(div)

    threshold = np.percentile(divergences, config['divergence_percentile']) if divergences else 0
    change_points = [0] + [i for i, d in enumerate(divergences) if d > threshold] + [len(yearly_counts)]
    change_points = sorted(list(set(change_points)))
    
    windows = []
    for i in range(1, len(change_points)):
        start_idx = change_points[i-1]
        end_idx = change_points[i]
        window_years = [int(yearly_counts[j][0]) for j in range(start_idx, end_idx)]
        if not window_years:
            continue
        window_start = int(min(window_years))
        window_end = int(max(window_years))
        if (window_end - window_start) < config['min_window_span']:
            continue
        windows.append({
            'start': window_start,
            'end': window_end,
            'years': window_years
        })

    tfidf_vectorizer = TfidfVectorizer(
        vocabulary=global_vectorizer.vocabulary_,
        ngram_range=(1, 2)
    )
    full_matrix = tfidf_vectorizer.fit_transform(docs)
    feature_names = tfidf_vectorizer.get_feature_names_out()

    topic_evolution = []
    for window in windows:
        window_mask = keyword_df['first filing year'].isin(window['years']).values
        window_matrix = full_matrix[window_mask]
        
        if window_matrix.shape[0] == 0:
            continue
            
        n_topics = max(1, min(config['max_topics'], window_matrix.shape[0] // 50))
        
        try:
            lda = LatentDirichletAllocation(
                n_components=n_topics,
                random_state=42,
                learning_method='online'
            )
            lda.fit(window_matrix)
            
            for topic_idx, topic in enumerate(lda.components_):
                top_indices = topic.argsort()[-config['top_words_per_topic']:][::-1]
                top_words = [feature_names[i] for i in top_indices]
                topic_evolution.append({
                    'start': int(window['start']),
                    'end': int(window['end']),
                    'topic_id': f"{int(window['start'])}-{topic_idx+1}",
                    'words': top_words,
                    'weights': topic[top_indices].tolist()
                })
        except Exception:
            continue

    return topic_evolution, windows, divergences, valid_years


