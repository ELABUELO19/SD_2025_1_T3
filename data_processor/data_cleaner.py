#!/usr/bin/env python3
"""
Módulo de limpieza y normalización de datos de tráfico de Waze
Procesa, filtra, deduplica y agrupa eventos similares
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient
from geopy.distance import geodesic
import hashlib
from collections import defaultdict
import csv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/data_cleaner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WazeDataCleaner:
    """Limpiador y normalizador de datos de tráfico de Waze"""
    
    def __init__(self, mongodb_uri: str):
        self.mongodb_uri = mongodb_uri
        self.client = None
        self.db = None
        self.collection = None
        self.cleaned_collection = None
        
        # Configuración de agrupamiento
        self.PROXIMITY_THRESHOLD_KM = 0.5  # 500 metros
        self.TIME_THRESHOLD_MINUTES = 30   # 30 minutos
        
        # Mapeo de normalización de tipos de eventos
        self.EVENT_TYPE_MAPPING = {
            'HAZARD': 'hazard',
            'JAM': 'jam',
            'ACCIDENT': 'accident',
            'ROAD_CLOSED': 'road_closure',
            'WEATHERHAZARD': 'weather',
            'alert': 'alert',
            'jam': 'jam',
            'accident': 'accident',
            'road_closure': 'road_closure',
            'hazard': 'hazard'
        }
        
        # Mapeo de normalización de subtipos
        self.SUBTYPE_MAPPING = {
            'HAZARD_ON_ROAD_POT_HOLE': 'pothole',
            'HAZARD_ON_ROAD_OBJECT': 'object_on_road',
            'HAZARD_ON_ROAD_CONSTRUCTION': 'construction',
            'HAZARD_ON_ROAD_CAR_STOPPED': 'vehicle_stopped',
            'HAZARD_WEATHER_FOG': 'fog',
            'HAZARD_WEATHER_RAIN': 'rain',
            'JAM_MODERATE_TRAFFIC': 'moderate_traffic',
            'JAM_HEAVY_TRAFFIC': 'heavy_traffic',
            'JAM_STAND_STILL_TRAFFIC': 'standstill_traffic',
            'traffic_jam': 'traffic_jam'
        }
        
        # Municipios válidos de la RM
        self.VALID_MUNICIPALITIES = {
            'Santiago', 'Las Condes', 'Providencia', 'Ñuñoa', 'Maipú', 'Puente Alto',
            'La Florida', 'Peñalolén', 'San Bernardo', 'Vitacura', 'La Reina',
            'Estación Central', 'Quilicura', 'Huechuraba', 'Pedro Aguirre Cerda',
            'Lo Barnechea', 'Cerrillos', 'Renca', 'Independencia', 'Recoleta',
            'Macul', 'San Miguel', 'La Cisterna', 'El Bosque', 'San Ramón',
            'La Granja', 'San Joaquín', 'La Pintana', 'Cerro Navia', 'Pudahuel',
            'Lo Prado', 'Quinta Normal', 'Conchalí', 'Colina', 'Lampa',
            'Tiltil', 'Pirque', 'San José de Maipo', 'Calera de Tango',
            'Buin', 'Paine', 'Melipilla', 'Curacaví', 'María Pinto',
            'Alhué', 'San Pedro', 'Talagante', 'Peaflor', 'Padre Hurtado',
            'Isla de Maipo'
        }
        
    def connect_database(self) -> bool:
        """Conecta a la base de datos MongoDB"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client.traffic_db
            self.collection = self.db.waze_events
            self.cleaned_collection = self.db.waze_events_cleaned
            
            # Crear índices para la colección limpia
            self.cleaned_collection.create_index([("normalized_id", 1)], unique=True)
            self.cleaned_collection.create_index([("timestamp", -1)])
            self.cleaned_collection.create_index([("municipality", 1)])
            self.cleaned_collection.create_index([("event_type", 1)])
            self.cleaned_collection.create_index([("location", "2dsphere")])
            
            logger.info("Conexión exitosa a MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Error conectando a MongoDB: {e}")
            return False
    
    def get_raw_events(self, limit: Optional[int] = None) -> List[Dict]:
        """Obtiene eventos sin procesar de la base de datos"""
        try:
            query = {
                'source': {'$in': ['waze_api_real', 'waze_web']},
                'location.lat': {'$exists': True, '$ne': None},
                'location.lng': {'$exists': True, '$ne': None}
            }
            
            cursor = self.collection.find(query).sort('timestamp', -1)
            if limit:
                cursor = cursor.limit(limit)
                
            events = list(cursor)
            logger.info(f"Obtenidos {len(events)} eventos sin procesar")
            return events
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {e}")
            return []
    
    def validate_event(self, event: Dict) -> bool:
        """Valida si un evento tiene los datos mínimos requeridos"""
        required_fields = ['location', 'timestamp', 'event_type']
        
        # Verificar campos requeridos
        for field in required_fields:
            if field not in event or event[field] is None:
                return False
        
        # Verificar ubicación válida
        location = event.get('location', {})
        lat = location.get('lat')
        lng = location.get('lng')
        
        if not lat or not lng:
            return False
            
        # Verificar que las coordenadas estén en rango válido para Chile
        if not (-34.5 <= lat <= -32.5 and -71.5 <= lng <= -70.0):
            return False
        
        # Verificar timestamp válido
        try:
            if isinstance(event['timestamp'], str):
                datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            elif not isinstance(event['timestamp'], datetime):
                return False
        except:
            return False
            
        return True
    
    def normalize_event(self, event: Dict) -> Dict:
        """Normaliza un evento individual"""
        try:
            # Normalizar tipo de evento
            event_type = event.get('event_type', '').upper()
            normalized_type = self.EVENT_TYPE_MAPPING.get(event_type, 'unknown')
            
            # Normalizar subtipo
            subtype = event.get('subtype', '')
            if isinstance(subtype, str):
                subtype = subtype.upper()
            normalized_subtype = self.SUBTYPE_MAPPING.get(subtype, subtype.lower() if subtype else 'unknown')
            
            # Normalizar municipio
            municipality = event.get('municipality', 'Unknown')
            normalized_municipality = self.normalize_municipality(municipality)
            
            # Normalizar timestamp
            timestamp = event.get('timestamp')
            if isinstance(timestamp, str):
                normalized_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                normalized_timestamp = timestamp
                
            # Extraer datos de Waze si existen
            waze_data = event.get('waze_data', {})
            
            return {
                'event_id': event.get('event_id'),
                'normalized_id': self.generate_normalized_id(event),
                'event_type': normalized_type,
                'subtype': normalized_subtype,
                'description': self.normalize_description(event),
                'location': {
                    'lat': float(event['location']['lat']),
                    'lng': float(event['location']['lng'])
                },
                'street': event.get('street', '').strip(),
                'city': event.get('city', '').strip(),
                'municipality': normalized_municipality,
                'timestamp': normalized_timestamp,
                'confidence': self.normalize_confidence(event),
                'severity': self.normalize_severity(event),
                'reliability': waze_data.get('reliability', 0),
                'num_thumbs_up': waze_data.get('nThumbsUp', 0),
                'source': event.get('source', 'unknown'),
                'original_data': {
                    'original_type': event.get('event_type'),
                    'original_subtype': event.get('subtype'),
                    'original_municipality': event.get('municipality'),
                    'waze_uuid': waze_data.get('uuid'),
                    'waze_id': waze_data.get('id')
                }
            }
            
        except Exception as e:
            logger.error(f"Error normalizando evento {event.get('event_id')}: {e}")
            return None
    
    def normalize_municipality(self, municipality: str) -> str:
        """Normaliza el nombre del municipio"""
        if not municipality or municipality == 'Unknown':
            return 'Unknown'
            
        # Limpiar y normalizar
        clean_municipality = municipality.strip().title()
        
        # Mapeo de nombres alternativos
        alternative_names = {
            'Estacion Central': 'Estación Central',
            'La Florida ': 'La Florida',
            'Penalolen': 'Peñalolén',
            'Nunoa': 'Ñuñoa',
            'Maipu': 'Maipú',
            'Region Metropolitana': 'Unknown',
            'Santiago Centro': 'Santiago'
        }
        
        normalized = alternative_names.get(clean_municipality, clean_municipality)
        
        # Verificar si es un municipio válido
        if normalized in self.VALID_MUNICIPALITIES:
            return normalized
        else:
            return 'Unknown'
    
    def normalize_description(self, event: Dict) -> str:
        """Normaliza la descripción del evento"""
        description = event.get('description', '')
        if not description:
            # Generar descripción basada en tipo y subtipo
            event_type = event.get('event_type', '')
            subtype = event.get('subtype', '')
            return f"{event_type} - {subtype}".strip(' -')
        
        return description.strip()
    
    def normalize_confidence(self, event: Dict) -> int:
        """Normaliza el nivel de confianza (0-10)"""
        confidence = event.get('confidence', 0)
        waze_data = event.get('waze_data', {})
        
        # Usar confianza de waze_data si está disponible
        if 'confidence' in waze_data:
            confidence = waze_data['confidence']
        
        try:
            confidence = int(confidence)
            return max(0, min(10, confidence))  # Limitar entre 0 y 10
        except:
            return 0
    
    def normalize_severity(self, event: Dict) -> int:
        """Normaliza el nivel de severidad (0-5)"""
        severity = event.get('severity', 0)
        waze_data = event.get('waze_data', {})
        
        # Usar severidad de waze_data si está disponible
        if 'reportRating' in waze_data:
            severity = waze_data['reportRating']
        
        try:
            severity = int(severity)
            return max(0, min(5, severity))  # Limitar entre 0 y 5
        except:
            return 0
    
    def generate_normalized_id(self, event: Dict) -> str:
        """Genera un ID normalizado para el evento"""
        location = event.get('location', {})
        lat = round(float(location.get('lat', 0)), 4)
        lng = round(float(location.get('lng', 0)), 4)
        event_type = event.get('event_type', '')
        
        # Usar timestamp redondeado a la hora más cercana para agrupamiento temporal
        timestamp = event.get('timestamp')
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        hour_timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
        
        # Crear hash único basado en ubicación, tipo y hora
        unique_string = f"{lat}_{lng}_{event_type}_{hour_timestamp.isoformat()}"
        return hashlib.md5(unique_string.encode()).hexdigest()[:16]
    
    def find_similar_events(self, event: Dict, processed_events: List[Dict]) -> List[Dict]:
        """Encuentra eventos similares basados en proximidad geográfica y temporal"""
        similar_events = []
        event_location = (event['location']['lat'], event['location']['lng'])
        event_time = event['timestamp']
        
        for processed in processed_events:
            # Verificar mismo tipo de evento
            if processed['event_type'] != event['event_type']:
                continue
                
            # Verificar proximidad geográfica
            processed_location = (processed['location']['lat'], processed['location']['lng'])
            distance_km = geodesic(event_location, processed_location).kilometers
            
            if distance_km > self.PROXIMITY_THRESHOLD_KM:
                continue
                
            # Verificar proximidad temporal
            time_diff = abs((event_time - processed['timestamp']).total_seconds() / 60)
            
            if time_diff <= self.TIME_THRESHOLD_MINUTES:
                similar_events.append(processed)
        
        return similar_events
    
    def merge_similar_events(self, main_event: Dict, similar_events: List[Dict]) -> Dict:
        """Combina eventos similares en uno consolidado"""
        if not similar_events:
            return main_event
        
        all_events = [main_event] + similar_events
        
        # Calcular ubicación promedio ponderada por confianza
        total_weight = sum(event.get('confidence', 1) for event in all_events)
        if total_weight == 0:
            total_weight = len(all_events)
            
        weighted_lat = sum(
            event['location']['lat'] * event.get('confidence', 1) 
            for event in all_events
        ) / total_weight
        
        weighted_lng = sum(
            event['location']['lng'] * event.get('confidence', 1) 
            for event in all_events
        ) / total_weight
        
        # Combinar otros atributos
        merged_event = main_event.copy()
        merged_event.update({
            'location': {
                'lat': round(weighted_lat, 6),
                'lng': round(weighted_lng, 6)
            },
            'confidence': max(event.get('confidence', 0) for event in all_events),
            'severity': max(event.get('severity', 0) for event in all_events),
            'num_thumbs_up': sum(event.get('num_thumbs_up', 0) for event in all_events),
            'merged_count': len(all_events),
            'merged_event_ids': [event.get('event_id') for event in all_events],
            'description': f"Evento consolidado: {main_event.get('description', '')} (fusionado de {len(all_events)} reportes)"
        })
        
        return merged_event
    
    def process_and_clean_data(self) -> List[Dict]:
        """Proceso principal de limpieza y normalización"""
        logger.info("Iniciando proceso de limpieza y normalización de datos")
        
        # Obtener eventos sin procesar
        raw_events = self.get_raw_events()
        if not raw_events:
            logger.warning("No se encontraron eventos para procesar")
            return []
        
        # Validar y normalizar eventos
        valid_events = []
        for event in raw_events:
            if self.validate_event(event):
                normalized = self.normalize_event(event)
                if normalized:
                    valid_events.append(normalized)
        
        logger.info(f"Eventos válidos después de normalización: {len(valid_events)}")
        
        # Agrupar y combinar eventos similares
        processed_events = []
        used_event_ids = set()
        
        for event in valid_events:
            if event['event_id'] in used_event_ids:
                continue
                
            # Encontrar eventos similares
            similar_events = self.find_similar_events(event, valid_events)
            similar_events = [e for e in similar_events if e['event_id'] not in used_event_ids]
            
            # Combinar eventos similares
            merged_event = self.merge_similar_events(event, similar_events)
            processed_events.append(merged_event)
            
            # Marcar eventos como usados
            used_event_ids.add(event['event_id'])
            for similar in similar_events:
                used_event_ids.add(similar['event_id'])
        
        logger.info(f"Eventos después de agrupamiento: {len(processed_events)}")
        
        # Remover duplicados exactos por normalized_id
        unique_events = {}
        for event in processed_events:
            norm_id = event['normalized_id']
            if norm_id not in unique_events:
                unique_events[norm_id] = event
            else:
                # Mantener el evento con mayor confianza
                if event.get('confidence', 0) > unique_events[norm_id].get('confidence', 0):
                    unique_events[norm_id] = event
        
        final_events = list(unique_events.values())
        logger.info(f"Eventos finales después de deduplicación: {len(final_events)}")
        
        return final_events
    
    def save_cleaned_events(self, cleaned_events: List[Dict]) -> int:
        """Guarda eventos limpios en la base de datos"""
        if not cleaned_events:
            return 0
        
        try:
            # Limpiar colección anterior
            self.cleaned_collection.delete_many({})
            
            # Insertar eventos limpios
            result = self.cleaned_collection.insert_many(cleaned_events)
            logger.info(f"Guardados {len(result.inserted_ids)} eventos limpios en la base de datos")
            return len(result.inserted_ids)
            
        except Exception as e:
            logger.error(f"Error guardando eventos limpios: {e}")
            return 0
    
    def export_to_csv(self, cleaned_events: List[Dict], min_events: int = 50) -> str:
        """Exporta eventos limpios a un archivo CSV"""
        try:
            # Verificar que tengamos suficientes eventos
            if len(cleaned_events) < min_events:
                logger.warning(f"Solo {len(cleaned_events)} eventos disponibles, menos que el mínimo de {min_events}")
            
            # Preparar datos para CSV
            csv_data = []
            for event in cleaned_events[:max(len(cleaned_events), min_events)]:
                csv_row = {
                    'normalized_id': event.get('normalized_id'),
                    'event_type': event.get('event_type'),
                    'subtype': event.get('subtype'),
                    'description': event.get('description', ''),
                    'latitude': event.get('location', {}).get('lat'),
                    'longitude': event.get('location', {}).get('lng'),
                    'street': event.get('street', ''),
                    'city': event.get('city', ''),
                    'municipality': event.get('municipality'),
                    'timestamp': event.get('timestamp').isoformat() if event.get('timestamp') else '',
                    'confidence': event.get('confidence', 0),
                    'severity': event.get('severity', 0),
                    'reliability': event.get('reliability', 0),
                    'num_thumbs_up': event.get('num_thumbs_up', 0),
                    'merged_count': event.get('merged_count', 1),
                    'source': event.get('source', ''),
                    'original_event_id': event.get('event_id', ''),
                    'waze_uuid': event.get('original_data', {}).get('waze_uuid', '')
                }
                csv_data.append(csv_row)
            
            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"/app/logs/waze_events_cleaned_{timestamp}.csv"
            
            # Escribir CSV
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                if csv_data:
                    fieldnames = csv_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_data)
            
            logger.info(f"Archivo CSV exportado: {csv_filename} con {len(csv_data)} eventos")
            return csv_filename
            
        except Exception as e:
            logger.error(f"Error exportando a CSV: {e}")
            return ""
    
    def get_cleaning_statistics(self, original_count: int, cleaned_events: List[Dict]) -> Dict:
        """Genera estadísticas del proceso de limpieza"""
        stats = {
            'original_events': original_count,
            'cleaned_events': len(cleaned_events),
            'reduction_percentage': ((original_count - len(cleaned_events)) / original_count * 100) if original_count > 0 else 0,
            'event_types': {},
            'municipalities': {},
            'merged_events': 0,
            'average_confidence': 0,
            'average_severity': 0
        }
        
        if not cleaned_events:
            return stats
        
        # Estadísticas por tipo de evento
        for event in cleaned_events:
            event_type = event.get('event_type', 'unknown')
            stats['event_types'][event_type] = stats['event_types'].get(event_type, 0) + 1
            
            municipality = event.get('municipality', 'Unknown')
            stats['municipalities'][municipality] = stats['municipalities'].get(municipality, 0) + 1
            
            if event.get('merged_count', 1) > 1:
                stats['merged_events'] += 1
        
        # Promedios
        stats['average_confidence'] = sum(event.get('confidence', 0) for event in cleaned_events) / len(cleaned_events)
        stats['average_severity'] = sum(event.get('severity', 0) for event in cleaned_events) / len(cleaned_events)
        
        return stats
    
    def run_full_cleaning_process(self) -> Dict:
        """Ejecuta el proceso completo de limpieza"""
        logger.info("=== INICIANDO PROCESO COMPLETO DE LIMPIEZA DE DATOS ===")
        
        try:
            # Obtener conteo original
            original_count = self.collection.count_documents({
                'source': {'$in': ['waze_api_real', 'waze_web']}
            })
            
            # Procesar y limpiar datos
            cleaned_events = self.process_and_clean_data()
            
            if not cleaned_events:
                logger.error("No se pudieron procesar eventos")
                return {'error': 'No events processed'}
            
            # Guardar en base de datos
            saved_count = self.save_cleaned_events(cleaned_events)
            
            # Exportar a CSV
            csv_filename = self.export_to_csv(cleaned_events)
            
            # Generar estadísticas
            stats = self.get_cleaning_statistics(original_count, cleaned_events)
            
            result = {
                'success': True,
                'original_events': original_count,
                'cleaned_events': len(cleaned_events),
                'saved_events': saved_count,
                'csv_file': csv_filename,
                'statistics': stats,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info("=== PROCESO DE LIMPIEZA COMPLETADO ===")
            logger.info(f"Eventos originales: {original_count}")
            logger.info(f"Eventos procesados: {len(cleaned_events)}")
            logger.info(f"Reducción: {stats['reduction_percentage']:.1f}%")
            logger.info(f"Eventos fusionados: {stats['merged_events']}")
            logger.info(f"Archivo CSV: {csv_filename}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error en proceso de limpieza: {e}")
            return {'error': str(e)}

    def get_data_statistics(self) -> Dict:
        """Obtiene estadísticas generales de los datos"""
        try:
            # Estadísticas de eventos raw
            total_events = self.collection.count_documents({})
            real_events = self.collection.count_documents({
                'source': {'$in': ['waze_api_real', 'waze_web']}
            })
            
            # Estadísticas de eventos limpios
            clean_events = self.cleaned_collection.count_documents({})
            
            # Estadísticas adicionales de eventos limpios
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'avg_confidence': {'$avg': '$confidence'},
                        'avg_severity': {'$avg': '$severity'},
                        'total_merged': {'$sum': {'$cond': [{'$gt': ['$merged_count', 1]}, 1, 0]}}
                    }
                }
            ]
            
            aggregate_result = list(self.cleaned_collection.aggregate(pipeline))
            stats_data = aggregate_result[0] if aggregate_result else {}
            
            return {
                'total_events': total_events,
                'real_events': real_events,
                'synthetic_events': total_events - real_events,
                'unique_events': real_events,  # Para compatibilidad
                'duplicate_events': 0,  # Calculado después de deduplicación
                'clean_events': clean_events,
                'average_confidence': round(stats_data.get('avg_confidence', 0), 2),
                'average_severity': round(stats_data.get('avg_severity', 0), 2),
                'merged_events': stats_data.get('total_merged', 0)
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {
                'total_events': 0,
                'real_events': 0,
                'unique_events': 0,
                'duplicate_events': 0,
                'clean_events': 0,
                'error': str(e)
            }
        
def main():
    """Función principal"""
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/traffic_db?authSource=admin')
    
    cleaner = WazeDataCleaner(mongodb_uri)
    cleaner.connect_database()
    
    # Ejecutar proceso de limpieza
    result = cleaner.run_full_cleaning_process()
    
    if result.get('success'):
        print(f"✅ Proceso completado exitosamente")
        print(f"📊 Eventos procesados: {result['cleaned_events']}")
        print(f"📁 Archivo CSV: {result['csv_file']}")
    else:
        print(f"❌ Error: {result.get('error')}")

if __name__ == "__main__":
    main()
