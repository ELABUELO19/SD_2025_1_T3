

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import aiohttp
import pymongo
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WazeScraper:
    """Scraper para extraer eventos de tráfico reales desde Waze API"""
    
    def __init__(self, mongodb_uri: str, scrape_interval: int = 300):
        self.mongodb_uri = mongodb_uri
        self.scrape_interval = scrape_interval
        self.client = None
        self.db = None
        self.collection = None
        
        # Coordenadas de la Región Metropolitana divididas en zonas para mejor cobertura
        self.RM_ZONES = [
            # Zona Centro (Santiago, Providencia, Ñuñoa)
            {
                'name': 'Centro',
                'bounds': {
                    'top': -33.39819035279609,
                    'bottom': -33.51385372491804,
                    'left': -70.69194031180815,
                    'right': -70.66104126395659
                }
            },
            # Zona Oriente (Las Condes, Vitacura, La Reina)
            {
                'name': 'Oriente',
                'bounds': {
                    'top': -33.35,
                    'bottom': -33.45,
                    'left': -70.65,
                    'right': -70.50
                }
            },
            # Zona Poniente (Maipú, Pudahuel, Cerrillos)
            {
                'name': 'Poniente',
                'bounds': {
                    'top': -33.45,
                    'bottom': -33.55,
                    'left': -70.85,
                    'right': -70.68
                }
            },
            # Zona Sur (Puente Alto, La Florida, San Bernardo)
            {
                'name': 'Sur',
                'bounds': {
                    'top': -33.55,
                    'bottom': -33.70,
                    'left': -70.68,
                    'right': -70.55
                }
            },
            # Zona Norte (Quilicura, Huechuraba, Colina)
            {
                'name': 'Norte',
                'bounds': {
                    'top': -33.30,
                    'bottom': -33.42,
                    'left': -70.75,
                    'right': -70.60
                }
            }
        ]
        
        # Configuración de Chrome para scraping (como respaldo)
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)
        
    def connect_database(self):
        """Conecta a la base de datos MongoDB"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client.traffic_db
            self.collection = self.db.waze_events
            
            # Crear índices para optimizar consultas
            self.collection.create_index([("timestamp", pymongo.DESCENDING)])
            self.collection.create_index([("location", pymongo.GEO2D)])
            self.collection.create_index([("event_type", 1)])
            self.collection.create_index([("municipality", 1)])
            
            logger.info("Conexión exitosa a MongoDB")
            
        except Exception as e:
            logger.error(f"Error conectando a MongoDB: {e}")
            raise
    
    async def get_waze_data_api(self) -> List[Dict]:
        """
        Obtiene datos reales de Waze usando las URLs específicas de la API
        """
        all_events = []
        
        # Iterar por cada zona de la Región Metropolitana
        for zone in self.RM_ZONES:
            zone_events = await self.scrape_zone_data(zone)
            all_events.extend(zone_events)
            
            # Pequeña pausa entre solicitudes para evitar rate limiting
            await asyncio.sleep(1)
        
        logger.info(f"API Waze: Obtenidos {len(all_events)} eventos reales desde {len(self.RM_ZONES)} zonas")
        return all_events
    
    async def scrape_zone_data(self, zone: Dict) -> List[Dict]:
        """
        Obtiene datos de una zona específica usando la URL de la API de Waze
        """
        events = []
        bounds = zone['bounds']
        
        # Construir URL exactamente como el formato requerido
        waze_api_url = "https://www.waze.com/live-map/api/georss"
        
        params = {
            'top': bounds['top'],
            'bottom': bounds['bottom'],
            'left': bounds['left'],
            'right': bounds['right'],
            'env': 'row',
            'types': 'alerts,traffic'
        }
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Referer': 'https://www.waze.com/live-map',
                'Origin': 'https://www.waze.com'
            }
            
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(
                    waze_api_url, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status == 200:
                        try:
                            data = await response.json()
                            
                            # Procesar alertas con el formato exacto
                            if 'alerts' in data:
                                for alert_data in data['alerts']:
                                    event = self.process_waze_alert(alert_data, zone['name'])
                                    if event:
                                        events.append(event)
                            
                            # Procesar atascos de tráfico
                            if 'jams' in data:
                                for jam_data in data['jams']:
                                    event = self.process_waze_jam(jam_data, zone['name'])
                                    if event:
                                        events.append(event)
                                        
                            logger.info(f"Zona {zone['name']}: {len(events)} eventos obtenidos")
                            
                        except Exception as json_error:
                            logger.error(f"Error parseando JSON de zona {zone['name']}: {json_error}")
                            
                    elif response.status == 500:
                        logger.warning(f"Zona {zone['name']}: API Waze temporalmente no disponible (500)")
                    else:
                        logger.warning(f"Zona {zone['name']}: API respondió con código {response.status}")
                        
        except asyncio.TimeoutError:
            logger.warning(f"Zona {zone['name']}: Timeout en solicitud a API")
        except Exception as e:
            logger.error(f"Zona {zone['name']}: Error obteniendo datos - {e}")
            
        return events
    
    def scrape_waze_web(self) -> List[Dict]:
        """
        Scraping de la página web de Waze para obtener datos reales como método alternativo
        """
        events = []
        driver = None
        
        try:
            # Usar webdriver-manager para obtener ChromeDriver automáticamente
            logger.info("Configurando ChromeDriver con webdriver-manager...")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            
            # Navegar a Waze Live Map centrado en Santiago
            santiago_url = "https://www.waze.com/es-419/live-map/directions?"
            santiago_url += "to=ll.-33.4489%2C-70.6693&from=ll.-33.4489%2C-70.6693"
            
            logger.info(f"Navegando a: {santiago_url}")
            driver.get(santiago_url)
            
            # Esperar a que cargue la página
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Buscar elementos de eventos en el mapa
            event_selectors = [
                '[data-testid*="alert"]',
                '[data-testid*="jam"]',
                '[data-testid*="incident"]',
                '.waze-icon-alerts',
                '.waze-icon-traffic'
            ]
            
            for selector in event_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements[:50]:  # Limitar a 50 eventos por selector
                        try:
                            event_data = self.extract_event_data(element)
                            if event_data:
                                events.append(event_data)
                        except Exception as e:
                            logger.debug(f"Error extrayendo evento del DOM: {e}")
                            continue
                            
                except Exception as e:
                    logger.debug(f"Error con selector {selector}: {e}")
                    continue
            
            logger.info(f"Web scraping: Extraídos {len(events)} eventos reales")
            
        except Exception as e:
            logger.error(f"Error en web scraping de datos reales: {e}")
            
        finally:
            if driver:
                driver.quit()
        
        if not events:
            logger.warning("No se obtuvieron eventos reales desde web scraping")
                
        return events
    
    def process_waze_alert(self, alert_data: Dict, zone: str) -> Optional[Dict]:
        """Procesa una alerta de Waze con el formato completo de la API"""
        try:
            # Extraer ubicación
            location = alert_data.get('location', {})
            lat = location.get('y')
            lng = location.get('x')
            
            # Determinar municipio basado en coordenadas
            municipality = self.determine_municipality(lat, lng) if lat and lng else 'Unknown'
            
            # Procesar comentarios si existen
            comments = alert_data.get('comments', [])
            processed_comments = []
            for comment in comments:
                processed_comments.append({
                    'reportMillis': comment.get('reportMillis'),
                    'text': comment.get('text', ''),
                    'isThumbsUp': comment.get('isThumbsUp', False)
                })
            
            return {
                # Datos básicos del evento
                'event_id': alert_data.get('uuid', alert_data.get('id', f"alert_{int(time.time())}")),
                'event_type': 'alert',
                'subtype': alert_data.get('subtype', alert_data.get('type', 'unknown')),
                'description': f"{alert_data.get('type', 'HAZARD')} - {alert_data.get('subtype', '')}",
                
                # Ubicación
                'location': {
                    'lat': lat,
                    'lng': lng
                },
                'street': alert_data.get('street', ''),
                'city': alert_data.get('city', ''),
                'municipality': municipality,
                'zone': zone,
                
                # Metadatos de Waze completos
                'waze_data': {
                    'country': alert_data.get('country'),
                    'uuid': alert_data.get('uuid'),
                    'id': alert_data.get('id'),
                    'type': alert_data.get('type'),
                    'subtype': alert_data.get('subtype'),
                    'nThumbsUp': alert_data.get('nThumbsUp', 0),
                    'reportRating': alert_data.get('reportRating', 0),
                    'reportByMunicipalityUser': alert_data.get('reportByMunicipalityUser'),
                    'reliability': alert_data.get('reliability', 0),
                    'speed': alert_data.get('speed', 0),
                    'reportMood': alert_data.get('reportMood'),
                    'additionalInfo': alert_data.get('additionalInfo', ''),
                    'nComments': alert_data.get('nComments', 0),
                    'reportBy': alert_data.get('reportBy', ''),
                    'inscale': alert_data.get('inscale', False),
                    'confidence': alert_data.get('confidence', 0),
                    'roadType': alert_data.get('roadType'),
                    'magvar': alert_data.get('magvar'),
                    'wazeData': alert_data.get('wazeData'),
                    'pubMillis': alert_data.get('pubMillis'),
                    'fromNodeId': alert_data.get('fromNodeId'),
                    'toNodeId': alert_data.get('toNodeId'),
                    'comments': processed_comments
                },
                
                # Datos adicionales para análisis
                'timestamp': datetime.now(timezone.utc),
                'confidence': alert_data.get('confidence', 0),
                'num_thumbs_up': alert_data.get('nThumbsUp', 0),
                'severity': alert_data.get('reportRating', 0),
                'reliability': alert_data.get('reliability', 0),
                'source': 'waze_api_real'
            }
            
        except Exception as e:
            logger.error(f"Error procesando alerta de Waze: {e}")
            logger.debug(f"Datos de alerta problemática: {alert_data}")
            return None
    
    def process_waze_jam(self, jam_data: Dict, zone: str) -> Optional[Dict]:
        """Procesa un atasco de tráfico de Waze"""
        try:
            # Extraer ubicación (primer punto de la línea del atasco)
            line = jam_data.get('line', [])
            lat = line[0].get('y') if line else None
            lng = line[0].get('x') if line else None
            
            municipality = self.determine_municipality(lat, lng) if lat and lng else 'Unknown'
            
            return {
                # Datos básicos del evento
                'event_id': jam_data.get('uuid', f"jam_{int(time.time())}"),
                'event_type': 'jam',
                'subtype': 'traffic_jam',
                'description': f"Atasco nivel {jam_data.get('level', 0)} - {jam_data.get('type', 'TRAFFIC_JAM')}",
                
                # Ubicación
                'location': {
                    'lat': lat,
                    'lng': lng
                },
                'street': jam_data.get('street', ''),
                'city': jam_data.get('city', ''),
                'municipality': municipality,
                'zone': zone,
                
                # Metadatos específicos de atascos
                'waze_data': {
                    'uuid': jam_data.get('uuid'),
                    'id': jam_data.get('id'),
                    'type': jam_data.get('type'),
                    'level': jam_data.get('level', 0),
                    'length': jam_data.get('length', 0),
                    'delay': jam_data.get('delay', 0),
                    'speed': jam_data.get('speed', 0),
                    'speedKMH': jam_data.get('speedKMH', 0),
                    'turnType': jam_data.get('turnType'),
                    'line': line,
                    'roadType': jam_data.get('roadType'),
                    'pubMillis': jam_data.get('pubMillis'),
                    'endNodeId': jam_data.get('endNodeId'),
                    'startNodeId': jam_data.get('startNodeId')
                },
                
                # Datos adicionales para análisis
                'timestamp': datetime.now(timezone.utc),
                'length': jam_data.get('length', 0),
                'delay': jam_data.get('delay', 0),
                'speed': jam_data.get('speed', 0),
                'level': jam_data.get('level', 0),
                'source': 'waze_api_real'
            }
            
        except Exception as e:
            logger.error(f"Error procesando jam de Waze: {e}")
            logger.debug(f"Datos de jam problemático: {jam_data}")
            return None
    
    def extract_event_data(self, element) -> Optional[Dict]:
        """Extrae datos de un elemento del DOM"""
        try:
            # Obtener información básica del elemento
            event_type = 'unknown'
            description = ''
            
            # Intentar determinar el tipo de evento por clases CSS
            class_names = element.get_attribute('class') or ''
            if 'alert' in class_names.lower():
                event_type = 'alert'
            elif 'jam' in class_names.lower() or 'traffic' in class_names.lower():
                event_type = 'jam'
            elif 'accident' in class_names.lower():
                event_type = 'accident'
            
            # Intentar obtener texto descriptivo
            try:
                description = element.get_attribute('title') or element.text or ''
            except:
                description = ''
            
            return {
                'event_id': f"web_{int(time.time())}_{hash(description)}",
                'event_type': event_type,
                'subtype': 'web_scraped',
                'description': description,
                'location': {
                    'lat': -33.4489,  # Santiago centro por defecto
                    'lng': -70.6693
                },
                'street': 'Unknown',
                'city': 'Santiago',
                'municipality': 'Santiago',
                'timestamp': datetime.now(timezone.utc),
                'source': 'waze_web'
            }
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del elemento: {e}")
            return None
    
    def determine_municipality(self, lat: float, lng: float) -> str:
        """Determina la comuna basada en coordenadas"""
        if not lat or not lng:
            return 'Unknown'
        
        # Mapeo básico de coordenadas a comunas principales de la RM
        municipalities = {
            'Santiago': {'lat_range': (-33.47, -33.42), 'lng_range': (-70.68, -70.63)},
            'Las Condes': {'lat_range': (-33.42, -33.38), 'lng_range': (-70.68, -70.50)},
            'Providencia': {'lat_range': (-33.45, -33.41), 'lng_range': (-70.65, -70.60)},
            'Ñuñoa': {'lat_range': (-33.47, -33.43), 'lng_range': (-70.62, -70.58)},
            'La Reina': {'lat_range': (-33.45, -33.41), 'lng_range': (-70.58, -70.52)},
            'Vitacura': {'lat_range': (-33.40, -33.36), 'lng_range': (-70.60, -70.55)},
            'Lo Barnechea': {'lat_range': (-33.37, -33.30), 'lng_range': (-70.58, -70.45)},
            'Maipú': {'lat_range': (-33.52, -33.48), 'lng_range': (-70.82, -70.70)},
            'Puente Alto': {'lat_range': (-33.65, -33.55), 'lng_range': (-70.65, -70.55)}
        }
        
        for municipality, bounds in municipalities.items():
            if (bounds['lat_range'][0] <= lat <= bounds['lat_range'][1] and
                bounds['lng_range'][0] <= lng <= bounds['lng_range'][1]):
                return municipality
        
        return 'Región Metropolitana'
    
    def save_events(self, events: List[Dict]):
        """Guarda eventos en MongoDB"""
        if not events:
            return
        
        try:
            # Eliminar duplicados basados en event_id
            unique_events = []
            seen_ids = set()
            
            for event in events:
                if event['event_id'] not in seen_ids:
                    unique_events.append(event)
                    seen_ids.add(event['event_id'])
            
            if unique_events:
                result = self.collection.insert_many(unique_events, ordered=False)
                logger.info(f"Guardados {len(result.inserted_ids)} eventos únicos")
            
        except pymongo.errors.BulkWriteError as e:
            # Ignorar errores de duplicados
            logger.info(f"Algunos eventos ya existían: {len(e.details['writeErrors'])} duplicados")
            
        except Exception as e:
            logger.error(f"Error guardando eventos: {e}")
    
    def get_scraper_stats(self) -> Dict:
        """Obtiene estadísticas del scraper y datos reales"""
        try:
            total_events = self.collection.count_documents({})
            api_events = self.collection.count_documents({'source': 'waze_api_real'})
            web_events = self.collection.count_documents({'source': 'waze_web'})
            
            # Obtener estadísticas por zona
            zone_stats = list(self.collection.aggregate([
                {'$match': {'source': 'waze_api_real'}},
                {'$group': {'_id': '$zone', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))
            
            # Obtener estadísticas por municipio
            municipality_stats = list(self.collection.aggregate([
                {'$match': {'source': {'$in': ['waze_api_real', 'waze_web']}}},
                {'$group': {'_id': '$municipality', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))
            
            # Obtener estadísticas por tipo de evento
            event_type_stats = list(self.collection.aggregate([
                {'$match': {'source': {'$in': ['waze_api_real', 'waze_web']}}},
                {'$group': {'_id': '$event_type', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))
            
            # Obtener estadísticas por subtipo
            subtype_stats = list(self.collection.aggregate([
                {'$match': {'source': 'waze_api_real'}},
                {'$group': {'_id': '$subtype', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]))
            
            # Eventos más recientes
            recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            recent_events = self.collection.count_documents({
                'source': {'$in': ['waze_api_real', 'waze_web']},
                'timestamp': {'$gte': recent_cutoff}
            })
            
            # Calidad de datos de ubicación
            events_with_location = self.collection.count_documents({
                'source': 'waze_api_real',
                'location.lat': {'$exists': True, '$ne': None},
                'location.lng': {'$exists': True, '$ne': None}
            })
            
            return {
                'total_events': total_events,
                'real_events': {
                    'api_events': api_events,
                    'web_events': web_events,
                    'total_real': api_events + web_events
                },
                'recent_events_24h': recent_events,
                'zone_distribution': zone_stats,
                'municipality_distribution': municipality_stats,
                'event_type_distribution': event_type_stats,
                'subtype_distribution': subtype_stats,
                'data_quality': {
                    'has_sufficient_data': (api_events + web_events) >= 50,
                    'api_success_rate': api_events / (api_events + web_events) if (api_events + web_events) > 0 else 0,
                    'web_success_rate': web_events / (api_events + web_events) if (api_events + web_events) > 0 else 0,
                    'location_accuracy': events_with_location / api_events if api_events > 0 else 0
                }
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {'error': str(e)}
    
    async def run_scraper(self):
        """Ejecuta el proceso de scraping principal usando solo datos reales"""
        logger.info("Iniciando scraper de Waze - Solo datos reales")
        
        while True:
            try:
                all_events = []
                
                # Intentar API primero
                logger.info("Obteniendo datos desde API de Waze...")
                api_events = await self.get_waze_data_api()
                all_events.extend(api_events)
                
                # Si no hay suficientes eventos desde API, intentar web scraping
                if len(api_events) < 10:
                    logger.info("Pocos eventos desde API, intentando web scraping...")
                    web_events = self.scrape_waze_web()
                    all_events.extend(web_events)
                
                # Guardar solo eventos reales obtenidos
                if all_events:
                    self.save_events(all_events)
                    logger.info(f"Obtenidos y guardados {len(all_events)} eventos reales")
                else:
                    logger.warning("No se pudieron obtener eventos reales en esta iteración")
                
                # Estadísticas de la base de datos
                total_events = self.collection.count_documents({})
                real_events = self.collection.count_documents({'source': {'$in': ['waze_api_real', 'waze_web']}})
                
                # Estadísticas detalladas cada 10 iteraciones
                if hasattr(self, 'iteration_count'):
                    self.iteration_count += 1
                else:
                    self.iteration_count = 1
                    
                if self.iteration_count % 10 == 0:
                    detailed_stats = self.get_scraper_stats()
                    logger.info(f"=== ESTADÍSTICAS DETALLADAS (Iteración {self.iteration_count}) ===")
                    logger.info(f"Total de eventos: {detailed_stats.get('total_events', 0)}")
                    logger.info(f"Eventos reales: {detailed_stats.get('real_events', {}).get('total_real', 0)}")
                    logger.info(f"Distribución por zona: {detailed_stats.get('zone_distribution', [])}")
                    logger.info(f"Precisión de ubicación: {detailed_stats.get('data_quality', {}).get('location_accuracy', 0):.2%}")
                else:
                    logger.info(f"Total de eventos en base de datos: {total_events} (Reales: {real_events})")
                
                # Esperar hasta el próximo scraping
                logger.info(f"Esperando {self.scrape_interval} segundos para próximo scraping")
                await asyncio.sleep(self.scrape_interval)
                
            except Exception as e:
                logger.error(f"Error en ciclo de scraping: {e}")
                await asyncio.sleep(60)  # Esperar 1 minuto antes de reintentar

def main():
    """Función principal"""
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    scrape_interval = int(os.getenv('SCRAPE_INTERVAL', '300'))
    
    logger.info("=== INICIANDO SCRAPER DE WAZE - SOLO DATOS REALES ===")
    logger.info(f"URI MongoDB: {mongodb_uri}")
    logger.info(f"Intervalo de scraping: {scrape_interval} segundos")
    logger.info("NOTA: Este scraper NO genera datos sintéticos, solo obtiene datos reales de Waze")
    
    scraper = WazeScraper(mongodb_uri, scrape_interval)
    scraper.connect_database()
    
    # Mostrar estadísticas iniciales
    initial_stats = scraper.get_scraper_stats()
    logger.info(f"Estadísticas iniciales de la base de datos:")
    logger.info(f"- Total de eventos: {initial_stats.get('total_events', 0)}")
    logger.info(f"- Eventos reales: {initial_stats.get('real_events', {}).get('total_real', 0)}")
    
    # Ejecutar scraper
    asyncio.run(scraper.run_scraper())

if __name__ == "__main__":
    main()