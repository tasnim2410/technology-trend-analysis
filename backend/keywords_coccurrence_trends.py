import pandas as pd
from collections import defaultdict
from scipy.stats import linregress
from itertools import combinations
from collections import defaultdict
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import re

# Add a basic French stopword list (expand as needed)
FRENCH_STOP_WORDS = set([
    "le", "la", "les", "de", "des", "du", "un", "une", "et", "en", "à", "au", "aux", "pour", 
    "par", "avec", "sur", "dans", "ou", "où", "que", "qui", "dont", "ne", "pas", "ce", "ces",
    "cette", "son", "sa", "ses", "leur", "leurs", "nous", "vous", "ils", "elles", "il", "elle",
    "on", "mais", "plus", "moins", "a", "d", "l", "y", "se", "s", "t", "m", "n", "c", "qu", "au",
    "aux", "du", "des", "est", "été", "être", "fait", "faites", "fais", "fait", "faites", "faisons",
    "faites", "faisaient", "faisait", "faisant", "faisons", "faites", "faisaient", "faisait", "faisant"
    , "the", "and", "is", "in", "to", "of", "that", "it", "for", "with", "as", "by", 
    "this", "are", "was", "from", "at", "an", "be", "has", "have", "had", "were", "would",
    "could", "should", "may", "can", "will", "been", "all", "if", "when", "so", "than",
    "such", "into", "only", "no", "not", "any", "some", "more", "other", "what", "which",
    "their", "through", "about", "between", "after", "before", "while", "during", "under",
    "within", "without", "upon", "why", "how", "many", "most", "these", "those", "then",
    "there", "just", "now", "also", "very", "much", "well", "make", "made", "using", "based"
])
CUSTOM_STOPWORDS = ENGLISH_STOP_WORDS.union(FRENCH_STOP_WORDS)
def clean_text_remove_stopwords(text):
    # Lowercase, remove punctuation, split, remove stopwords, join back
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = [w for w in text.split() if w not in CUSTOM_STOPWORDS]
    return " ".join(tokens)

def build_cooccurrence(text: str, window_size: int = 5) -> dict:
    """
    Build a co-occurrence dictionary using a sliding window.
    
    Each window of 'window_size' tokens yields co-occurring pairs.
    """
    tokens = text.split()
    cooccurrences = defaultdict(int)
    
    # Iterate over all tokens with sliding window
    for i in range(len(tokens)):
        window = tokens[i:i + window_size]
        # Generate all combinations (order doesn't matter) from the current window
        for pair in combinations(window, 2):
            # Sort the pair to keep the ordering consistent (so ('a','b') is the same as ('b','a'))
            cooccurrences[tuple(sorted(pair))] += 1
    
    return cooccurrences
def track_cooccurrence_trends(df: pd.DataFrame, 
                             time_col: str = 'year',
                             text_col: str = 'abstract',
                             window_size: int = 5,
                             min_count: int = 5) -> pd.DataFrame:
    """
    Track co-occurrence trends over time with statistical significance
    
    Args:
        df: DataFrame containing temporal text data
        time_col: Column name for time periods (years)
        text_col: Column name containing text to analyze
        window_size: Window size for co-occurrence detection
        min_count: Minimum total occurrences to consider
        
    Returns:
        DataFrame with co-occurrence pairs and trend metrics
    """
    # Group documents by time period
    time_groups = df.groupby(time_col)[text_col].agg(list)
    
    # Store co-occurrence frequencies by year
    temporal_counts = defaultdict(lambda: defaultdict(int))
    
    # Process each time period
    for year, documents in time_groups.items():
        year_cooc = defaultdict(int)
        
        # Process each document in the time period
        for doc in documents:
            pairs = build_cooccurrence(doc, window_size)
            for pair, count in pairs.items():
                year_cooc[pair] += count
                
        # Store annual counts
        for pair, count in year_cooc.items():
            temporal_counts[pair][year] = count
    
    # Convert to DataFrame and calculate trends
    cooc_df = pd.DataFrame.from_dict(temporal_counts, orient='index')
    
    # Calculate trend metrics
    trend_data = []
    for pair, counts in cooc_df.iterrows():
        years = counts.dropna().index.astype(int)
        freqs = counts.dropna().values
        
        if len(years) < 3 or sum(freqs) < min_count:
            continue  # Skip sparse pairs
            
        # Calculate linear regression
        slope, _, _, p_value, _ = linregress(years, freqs)
        
        trend_data.append({
            'term1': pair[0],
            'term2': pair[1],
            'slope': slope,
            'p_value': p_value,
            'total_count': sum(freqs),
            'first_year': years.min(),
            'last_year': years.max(),
            'frequency_history': list(zip(years, freqs))
        })
    
    return pd.DataFrame(trend_data)
  
# keyword_df = df[['first publication number','Title','first filing year']]
# keyword_df = keyword_df.dropna-*+4
# 00000(subset=['Title'])+
# grouped = keyword_df.groupby("first filing year")["Title"].apply(lambda texts: " ".join(texts)).reset_index()

# # Example usage:
# cooc_trends = track_cooccurrence_trends(
#     grouped,
#     time_col='first filing year',
#     text_col='Title',
#     window_size=5,
#     min_count=10
# )

# # Get top emerging combinations
# emerging_tech = cooc_trends[
#     (cooc_trends.slope > 0) &
#     (cooc_trends.p_value < 0.05)
# ].sort_values('slope', ascending=False)

# print("Top emerging technology combinations:")
# print(emerging_tech[['term1', 'term2', 'slope', 'total_count']].head(5))

# # Select top term pairs for visualization
# top_terms = emerging_tech.head(5)  # Adjust the number as needed

# # Prepare data for plotting
# plot_data = []
# for _, row in top_terms.iterrows():
#     for year, freq in row['frequency_history']:
#         plot_data.append({
#             'year': year,
#             'frequency': freq,
#             'term_pair': f"{row['term1']} & {row['term2']}"
#         })

# plot_df = pd.DataFrame(plot_data)
# # Set the aesthetic style of the plots
# sns.set(style="whitegrid")

# # Create the line plot
# plt.figure(figsize=(12, 6))
# sns.lineplot(data=plot_df, x='year', y='frequency', hue='term_pair', marker='o')
# plt.title('Co-occurrence Trends of Top Emerging Technology Combinations Over Time')
# plt.xlabel('Year')
# plt.ylabel('Frequency')
# plt.legend(title='Term Pairs', bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.grid()
# plt.tight_layout()  # Adjust layout to make room for the legend
# plt.show()

# declining_tech = cooc_trends[
#     (cooc_trends.slope < 0) &
#     (cooc_trends.p_value < 0.05)
# ].sort_values('slope')

# # Prepare data for visualization of declining trends
# decline_plot_data = []
# for _, row in declining_tech.iterrows():
#     for year, freq in row['frequency_history']:
#         decline_plot_data.append({
#             'year': year,
#             'frequency': freq,
#             'term_pair': f"{row['term1']} & {row['term2']}"
#         })

# decline_plot_df = pd.DataFrame(decline_plot_data)

# # Create the line plot for declining trends
# plt.figure(figsize=(12, 6))
# sns.lineplot(data=decline_plot_df, x='year', y='frequency', hue='term_pair', marker='o')
# plt.title('Declining Co-occurrence Trends of Technology Combinations Over Time')
# plt.xlabel('Year')
# plt.ylabel('Frequency')
# plt.legend(title='Term Pairs', bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.grid()
# plt.tight_layout()
# plt.show()