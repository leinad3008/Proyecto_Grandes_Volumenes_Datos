import psycopg2
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
import os

class MovieRecommender:
    def __init__(self, db_host, db_user, db_password, db_name):
        self.conn = psycopg2.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        self.model_path = '/opt/airflow/models/movie_recommender_model.pkl'
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        
    def load_data(self):
        """Carga películas y ratings desde PostgreSQL"""
        query = """
            SELECT m.movieid, m.title, m.genres, COUNT(r.userid) as rating_count, 
                   AVG(r.rating) as avg_rating
            FROM movies m
            LEFT JOIN ratings r ON m.movieid = r.movieid
            GROUP BY m.movieid, m.title, m.genres
            ORDER BY rating_count DESC
        """
        self.movies_df = pd.read_sql(query, self.conn)
        print(f"Películas cargadas: {len(self.movies_df)}")
        
    def train_model(self):
        """Entrena modelo basado en similitud de géneros"""
        # Crear matriz TF-IDF basada en géneros
        tfidf = TfidfVectorizer(analyzer='char', ngram_range=(2, 3))
        genre_matrix = tfidf.fit_transform(self.movies_df['genres'].fillna(''))
        
        # Calcular similitud coseno
        self.similarity_matrix = cosine_similarity(genre_matrix)
        self.tfidf = tfidf
        
        print("Modelo entrenado correctamente")
        
    def save_model(self):
        """Guarda el modelo entrenado"""
        with open(self.model_path, 'wb') as f:
            pickle.dump({
                'movies_df': self.movies_df,
                'similarity_matrix': self.similarity_matrix,
                'tfidf': self.tfidf
            }, f)
        print(f"Modelo guardado en {self.model_path}")
        
    def load_model(self):
        """Carga el modelo entrenado"""
        if not os.path.exists(self.model_path):
            self.load_data()
            self.train_model()
            self.save_model()
        else:
            with open(self.model_path, 'rb') as f:
                data = pickle.load(f)
                self.movies_df = data['movies_df']
                self.similarity_matrix = data['similarity_matrix']
                self.tfidf = data['tfidf']
            print("Modelo cargado desde archivo")
            
    def recommend_by_movie(self, movie_title, n_recommendations=5):
        """Recomienda películas similares a una película dada"""
        # Buscar la película
        matches = self.movies_df[self.movies_df['title'].str.contains(movie_title, case=False, na=False)]
        
        if matches.empty:
            return f"Película '{movie_title}' no encontrada"
        
        # Tomar la primera coincidencia
        idx = matches.index[0]
        similarity_scores = self.similarity_matrix[idx]
        
        # Obtener top N películas similares
        similar_indices = np.argsort(similarity_scores)[::-1][1:n_recommendations+1]
        recommendations = self.movies_df.iloc[similar_indices][['title', 'genres', 'avg_rating']]
        
        return recommendations
    
    def recommend_by_genre(self, genres, n_recommendations=5):
        """Recomienda películas basadas en géneros"""
        matching_movies = self.movies_df[
            self.movies_df['genres'].str.contains(genres, case=False, na=False)
        ].sort_values('avg_rating', ascending=False)
        
        return matching_movies.head(n_recommendations)[['title', 'genres', 'avg_rating']]
    
    def recommend_popular(self, n_recommendations=5):
        """Retorna las películas más populares"""
        return self.movies_df.nlargest(n_recommendations, 'rating_count')[['title', 'genres', 'avg_rating']]