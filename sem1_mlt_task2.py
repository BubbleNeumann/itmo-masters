# Чернышова Дана Кирилловна
# TASK 2


from contextlib import redirect_stdout
import requests
import re
import sys
from collections import Counter, defaultdict
from math import log
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag

output_file = 'sem1_mlt_task2_output.txt'


nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

url = "http://www.gutenberg.org/files/11/11-0.txt"
response = requests.get(url)
text = response.text[655:] # skip the outline

lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))


def preprocess_text(text):
    text = text.lower()
    words = word_tokenize(text)
    words = [
        lemmatizer.lemmatize(word)
        for word in words
        if word.isalpha() and word not in stop_words
    ]
    return words

def compute_tf(word_list):
    tf = Counter(word_list)
    total_words = len(word_list)
    return {word: count / total_words for word, count in tf.items()}


def compute_idf(corpus):
    idf = defaultdict(lambda: 0)
    total_documents = len(corpus)
    for document in corpus:
        unique_words = set(document)
        for word in unique_words:
            idf[word] += 1
    return {word: log(total_documents / count) for word, count in idf.items()}


def get_top_tfidf_words(corpus, idfs, top_n=10):
    top_words = []
    for document in corpus:
        tf = compute_tf(document)
        tfidf = {word: tf[word] * idfs[word] for word in tf if word != 'alice'}
        sorted_tfidf = sorted(tfidf.items(), key=lambda x: x[1], reverse=True)
        top_words.append([word for word, score in sorted_tfidf[:top_n]])
    return top_words

def get_top_words(chapter_tokens, top_n=10):

    word_freq = {}
    
    for word in chapter_tokens:
        word = word.lower()
        # if word not in stop_words and word.isalpha() and word != 'alice':
        if word != 'alice':
            word_freq[word] = word_freq.get(word, 0) + 1

    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return sorted_words[:top_n]



with open(output_file, 'w') as file:
    with redirect_stdout(file):

        chapters = re.split(r'\bchapter\s{0,3}.{1,5}\b', text, flags=re.IGNORECASE)

        corpus = [preprocess_text(chapter) for chapter in chapters]
        idfs = compute_idf(corpus)

        for i in range(1, len(corpus)):
            chapter_top_words = get_top_words(corpus[i], 10)
            print(f'Chapter {i} top words: {chapter_top_words}')


        def extract_verbs_with_alice(text):
            sentences = sent_tokenize(text)  # Split text into sentences
            alice_verbs = []
            
            for sentence in sentences:
                if 'alice' in sentence.lower():
                    words = word_tokenize(sentence.lower())
                    tagged_words = pos_tag(words)
                    # extract verbs (POS tags starting with 'VB')
                    for word, pos in tagged_words:
                        if pos.startswith('VB') and word.isalpha() and word not in stop_words:
                    
                            alice_verbs.append(lemmatizer.lemmatize(word, wordnet.VERB))
            
            verb_counts = Counter(alice_verbs)
            return verb_counts.most_common(10)

        # extract top verbs associated with alice
        top_alice_verbs = extract_verbs_with_alice(text)
        print("\nTop 10 verbs used in sentences with Alice:")
        for verb, count in top_alice_verbs:
            print(f"{verb}: {count}")
