import pandas as pd
import os
from config import DATA_PATHS

class DataLoader:
    def __init__(self):
        self.data_paths = DATA_PATHS
    
    def load_match_data(self, match_id=None):
        """Cargar datos de partidos desde parquet"""
        pass
    
    def load_player_stats(self, player_id=None):
        """Cargar estadísticas de jugadores"""
        pass
    
    def load_team_performance(self, season=None):
        """Cargar rendimiento del equipo"""
        pass
    
    def get_available_matches(self):
        """Obtener lista de partidos disponibles"""
        pass
