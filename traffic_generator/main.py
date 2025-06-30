import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List

import aiohttp
import numpy as np
from pymongo import MongoClient

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrafficGenerator:
    """Generador de tráfico sintético para pruebas del sistema de caché"""
    
    def __init__(self, mongodb_uri: str, cache_uri: str, poisson_lambda: float = 10, exponential_lambda: float = 0.1):
        self.mongodb_uri = mongodb_uri
        self.cache_uri = cache_uri
        self.poisson_lambda = poisson_lambda
        self.exponential_lambda = exponential_lambda
        
        # Conectar a MongoDB para obtener datos reales
        self.mongodb_client = MongoClient(mongodb_uri)
        self.db = self.mongodb_client.traffic_db
        self.collection = self.db.waze_events
        
        # Estadísticas del generador
        self.requests_sent = 0
        self.responses_received = 0
        self.errors = 0
        self.response_times = []
        
        # Patrones de consulta
        self.query_patterns = self._define_query_patterns()
        
        logger.info("Generador de tráfico inicializado")
    
    def _define_query_patterns(self) -> List[Dict]:
        """Define patrones de consulta basados en datos reales"""
        
        # Obtener municipios reales de la base de datos
        try:
            municipalities = list(self.collection.distinct('municipality'))
            if not municipalities:
                logger.warning("No se encontraron municipios en la base de datos. Esperando datos reales...")
                return []
        except Exception as e:
            logger.error(f"Error obteniendo municipios reales: {e}")
            return []
        
        # Obtener tipos de eventos reales de la base de datos
        try:
            event_types = list(self.collection.distinct('event_type'))
            if not event_types:
                logger.warning("No se encontraron tipos de eventos en la base de datos. Esperando datos reales...")
                return []
        except Exception as e:
            logger.error(f"Error obteniendo tipos de eventos reales: {e}")
            return []
        
        patterns = [
            # Consultas por municipio (muy comunes)
            {
                'type': 'municipality_query',
                'weight': 0.3,
                'template': {
                    'municipality': None,  # Se llenará dinámicamente
                    'limit': 100
                }
            },
            
            # Consultas por tipo de evento
            {
                'type': 'event_type_query',
                'weight': 0.2,
                'template': {
                    'event_type': None,  # Se llenará dinámicamente
                    'limit': 50
                }
            },
            
            # Consultas por rango de tiempo (última hora, último día)
            {
                'type': 'recent_events',
                'weight': 0.25,
                'template': {
                    'date_range': {
                        'start': None,  # Se calculará dinámicamente
                        'end': None
                    },
                    'limit': 200
                }
            },
            
            # Consultas geoespaciales basadas en ubicaciones reales
            {
                'type': 'location_query',
                'weight': 0.15,
                'template': {
                    'location': {
                        'lat': None,  # Se obtendrá de datos reales
                        'lng': None,  # Se obtendrá de datos reales
                        'radius': 5  # 5 km
                    },
                    'limit': 150
                }
            },
            
            # Consultas combinadas (municipio + tipo)
            {
                'type': 'combined_query',
                'weight': 0.1,
                'template': {
                    'municipality': None,
                    'event_type': None,
                    'limit': 75
                }
            }
        ]
        
        self.municipalities = municipalities
        self.event_types = event_types
        
        logger.info(f"Patrones de consulta inicializados con datos reales:")
        logger.info(f"- Municipios: {len(municipalities)} encontrados")
        logger.info(f"- Tipos de eventos: {len(event_types)} encontrados")
        
        return patterns
    
    def generate_query(self) -> Dict:
        """Genera una consulta basada en datos reales únicamente"""
        
        # Verificar que hay patrones disponibles (basados en datos reales)
        if not self.query_patterns:
            logger.warning("No hay patrones de consulta disponibles. Esperando datos reales...")
            self.query_patterns = self._define_query_patterns()
            if not self.query_patterns:
                return None
        
        # Seleccionar patrón basado en pesos
        weights = [pattern['weight'] for pattern in self.query_patterns]
        pattern = np.random.choice(self.query_patterns, p=weights)
        
        # Crear consulta basada en el patrón
        query = pattern['template'].copy()
        
        if pattern['type'] == 'municipality_query':
            if self.municipalities:
                query['municipality'] = random.choice(self.municipalities)
            else:
                return None
            
        elif pattern['type'] == 'event_type_query':
            if self.event_types:
                query['event_type'] = random.choice(self.event_types)
            else:
                return None
            
        elif pattern['type'] == 'recent_events':
            # Generar rango de tiempo reciente
            end_time = datetime.now()
            hours_back = random.choice([1, 6, 12, 24, 48])  # Diferentes ventanas de tiempo
            start_time = end_time - timedelta(hours=hours_back)
            
            query['date_range'] = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
            
        elif pattern['type'] == 'location_query':
            # Obtener ubicación real de la base de datos
            real_locations = self.get_real_locations(limit=10)
            if real_locations:
                selected_location = random.choice(real_locations)
                # Agregar pequeña variación para simular búsquedas cercanas
                lat_offset = random.uniform(-0.01, 0.01)  # ~1km de variación
                lng_offset = random.uniform(-0.01, 0.01)
                
                query['location'] = {
                    'lat': selected_location['location']['lat'] + lat_offset,
                    'lng': selected_location['location']['lng'] + lng_offset,
                    'radius': random.choice([1, 2, 5, 10, 15])
                }
            else:
                logger.warning("No se encontraron ubicaciones reales, saltando consulta geoespacial")
                return None
            
        elif pattern['type'] == 'combined_query':
            if self.municipalities and self.event_types:
                query['municipality'] = random.choice(self.municipalities)
                query['event_type'] = random.choice(self.event_types)
            else:
                return None
        
        return query
    
    def generate_aggregation_query(self) -> tuple:
        """Genera consultas de agregación"""
        
        aggregation_types = [
            ('events_by_municipality', {}),
            ('events_by_type', {}),
            ('hourly_distribution', {}),
            ('recent_events', {'hours': random.choice([1, 6, 12, 24]), 'limit': random.choice([50, 100, 200])})
        ]
        
        return random.choice(aggregation_types)
    
    async def send_query(self, session: aiohttp.ClientSession, query: Dict) -> Dict:
        """Envía una consulta al sistema de caché"""
        
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.cache_uri}/query",
                json=query,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_data = await response.json()
                response_time = time.time() - start_time
                
                self.requests_sent += 1
                self.responses_received += 1
                self.response_times.append(response_time)
                
                return {
                    'success': True,
                    'response_time': response_time,
                    'status_code': response.status,
                    'cache_hit': response_data.get('metrics', {}).get('cache_hit', False),
                    'result_count': response_data.get('metrics', {}).get('result_count', 0),
                    'query': query
                }
                
        except Exception as e:
            self.errors += 1
            response_time = time.time() - start_time
            
            logger.error(f"Error enviando consulta: {e}")
            
            return {
                'success': False,
                'response_time': response_time,
                'error': str(e),
                'query': query
            }
    
    async def send_aggregation_query(self, session: aiohttp.ClientSession, agg_type: str, params: Dict) -> Dict:
        """Envía una consulta de agregación"""
        
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.cache_uri}/aggregation/{agg_type}",
                json=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_data = await response.json()
                response_time = time.time() - start_time
                
                self.requests_sent += 1
                self.responses_received += 1
                self.response_times.append(response_time)
                
                return {
                    'success': True,
                    'response_time': response_time,
                    'status_code': response.status,
                    'cache_hit': response_data.get('metrics', {}).get('cache_hit', False),
                    'result_count': response_data.get('metrics', {}).get('result_count', 0),
                    'aggregation_type': agg_type,
                    'params': params
                }
                
        except Exception as e:
            self.errors += 1
            response_time = time.time() - start_time
            
            return {
                'success': False,
                'response_time': response_time,
                'error': str(e),
                'aggregation_type': agg_type,
                'params': params
            }
    
    def poisson_arrival_times(self, duration_seconds: int) -> List[float]:
        """Genera tiempos de llegada siguiendo una distribución de Poisson"""
        
        arrival_times = []
        current_time = 0
        
        while current_time < duration_seconds:
            # Tiempo entre llegadas sigue distribución exponencial
            inter_arrival_time = np.random.exponential(1.0 / self.poisson_lambda)
            current_time += inter_arrival_time
            
            if current_time < duration_seconds:
                arrival_times.append(current_time)
        
        return arrival_times
    
    def exponential_arrival_times(self, duration_seconds: int) -> List[float]:
        """Genera tiempos de llegada siguiendo una distribución exponencial"""
        
        arrival_times = []
        current_time = 0
        
        while current_time < duration_seconds:
            inter_arrival_time = np.random.exponential(1.0 / self.exponential_lambda)
            current_time += inter_arrival_time
            
            if current_time < duration_seconds:
                arrival_times.append(current_time)
        
        return arrival_times
    
    async def run_poisson_traffic(self, duration_minutes: int = 10):
        """Ejecuta tráfico siguiendo distribución de Poisson usando solo datos reales"""
        
        logger.info(f"Iniciando tráfico Poisson (λ={self.poisson_lambda}) por {duration_minutes} minutos con datos reales")
        
        # Verificar disponibilidad de datos reales
        if not self.verify_data_availability():
            logger.error("No hay suficientes datos reales disponibles para generar tráfico")
            return {'error': 'Datos reales insuficientes'}
        
        duration_seconds = duration_minutes * 60
        arrival_times = self.poisson_arrival_times(duration_seconds)
        
        logger.info(f"Generadas {len(arrival_times)} llegadas para distribución Poisson")
        
        start_time = time.time()
        results = []
        
        async with aiohttp.ClientSession() as session:
            for arrival_time in arrival_times:
                # Esperar hasta el tiempo de llegada
                elapsed = time.time() - start_time
                wait_time = arrival_time - elapsed
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                # Decidir tipo de consulta (80% normales, 20% agregaciones)
                if random.random() < 0.8:
                    query = self.generate_query()
                    if query:  # Solo si se pudo generar una consulta válida
                        result = await self.send_query(session, query)
                        results.append(result)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                    results.append(result)
                
                # Log progreso cada 50 consultas
                if len(results) % 50 == 0 and results:
                    cache_hits = sum(1 for r in results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(results) if results else 0
                    avg_response_time = np.mean([r['response_time'] for r in results])
                    logger.info(f"Progreso: {len(results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
        
        return self.analyze_results(results, "Poisson (Datos Reales)")
    
    async def run_exponential_traffic(self, duration_minutes: int = 10):
        """Ejecuta tráfico siguiendo distribución exponencial usando solo datos reales"""
        
        logger.info(f"Iniciando tráfico Exponencial (λ={self.exponential_lambda}) por {duration_minutes} minutos con datos reales")
        
        # Verificar disponibilidad de datos reales
        if not self.verify_data_availability():
            logger.error("No hay suficientes datos reales disponibles para generar tráfico")
            return {'error': 'Datos reales insuficientes'}
        
        duration_seconds = duration_minutes * 60
        arrival_times = self.exponential_arrival_times(duration_seconds)
        
        logger.info(f"Generadas {len(arrival_times)} llegadas para distribución Exponencial")
        
        start_time = time.time()
        results = []
        
        async with aiohttp.ClientSession() as session:
            for arrival_time in arrival_times:
                # Esperar hasta el tiempo de llegada
                elapsed = time.time() - start_time
                wait_time = arrival_time - elapsed
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                # Decidir tipo de consulta
                if random.random() < 0.8:
                    query = self.generate_query()
                    if query:  # Solo si se pudo generar una consulta válida
                        result = await self.send_query(session, query)
                        results.append(result)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                    results.append(result)
                
                # Log progreso
                if len(results) % 50 == 0 and results:
                    cache_hits = sum(1 for r in results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(results) if results else 0
                    avg_response_time = np.mean([r['response_time'] for r in results])
                    logger.info(f"Progreso: {len(results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
        
        return self.analyze_results(results, "Exponencial (Datos Reales)")
    
    async def run_burst_traffic(self, num_bursts: int = 5, burst_size: int = 20, burst_interval: int = 30):
        """Ejecuta tráfico en ráfagas para probar el comportamiento del caché"""
        
        logger.info(f"Iniciando tráfico en ráfagas: {num_bursts} ráfagas de {burst_size} consultas cada {burst_interval}s")
        
        all_results = []
        
        async with aiohttp.ClientSession() as session:
            for burst_num in range(num_bursts):
                logger.info(f"Ejecutando ráfaga {burst_num + 1}/{num_bursts}")
                
                # Ejecutar consultas de la ráfaga concurrentemente
                burst_tasks = []
                
                for _ in range(burst_size):
                    if random.random() < 0.7:
                        query = self.generate_query()
                        task = self.send_query(session, query)
                    else:
                        agg_type, params = self.generate_aggregation_query()
                        task = self.send_aggregation_query(session, agg_type, params)
                    
                    burst_tasks.append(task)
                
                # Ejecutar todas las consultas de la ráfaga
                burst_results = await asyncio.gather(*burst_tasks, return_exceptions=True)
                
                # Filtrar resultados válidos
                valid_results = [r for r in burst_results if isinstance(r, dict)]
                all_results.extend(valid_results)
                
                cache_hits = sum(1 for r in valid_results if r.get('cache_hit', False))
                hit_rate = cache_hits / len(valid_results) if valid_results else 0
                avg_response_time = np.mean([r['response_time'] for r in valid_results]) if valid_results else 0
                
                logger.info(f"Ráfaga {burst_num + 1} completada: {len(valid_results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
                
                # Esperar antes de la siguiente ráfaga (excepto en la última)
                if burst_num < num_bursts - 1:
                    await asyncio.sleep(burst_interval)
        
        return self.analyze_results(all_results, "Ráfagas")
    
    def analyze_results(self, results: List[Dict], traffic_type: str) -> Dict:
        """Analiza los resultados de las pruebas"""
        
        if not results:
            return {'error': 'No hay resultados para analizar'}
        
        successful_results = [r for r in results if r.get('success', False)]
        cache_hits = [r for r in successful_results if r.get('cache_hit', False)]
        cache_misses = [r for r in successful_results if not r.get('cache_hit', False)]
        
        response_times = [r['response_time'] for r in successful_results]
        cache_hit_times = [r['response_time'] for r in cache_hits]
        cache_miss_times = [r['response_time'] for r in cache_misses]
        
        analysis = {
            'traffic_type': traffic_type,
            'summary': {
                'total_requests': len(results),
                'successful_requests': len(successful_results),
                'failed_requests': len(results) - len(successful_results),
                'success_rate': len(successful_results) / len(results) if results else 0
            },
            'cache_performance': {
                'total_hits': len(cache_hits),
                'total_misses': len(cache_misses),
                'hit_rate': len(cache_hits) / len(successful_results) if successful_results else 0,
                'miss_rate': len(cache_misses) / len(successful_results) if successful_results else 0
            },
            'response_times': {
                'overall': {
                    'mean': float(np.mean(response_times)) if response_times else 0,
                    'median': float(np.median(response_times)) if response_times else 0,
                    'std': float(np.std(response_times)) if response_times else 0,
                    'min': float(np.min(response_times)) if response_times else 0,
                    'max': float(np.max(response_times)) if response_times else 0,
                    'p95': float(np.percentile(response_times, 95)) if response_times else 0,
                    'p99': float(np.percentile(response_times, 99)) if response_times else 0
                },
                'cache_hits': {
                    'mean': float(np.mean(cache_hit_times)) if cache_hit_times else 0,
                    'median': float(np.median(cache_hit_times)) if cache_hit_times else 0,
                    'std': float(np.std(cache_hit_times)) if cache_hit_times else 0
                } if cache_hit_times else {},
                'cache_misses': {
                    'mean': float(np.mean(cache_miss_times)) if cache_miss_times else 0,
                    'median': float(np.median(cache_miss_times)) if cache_miss_times else 0,
                    'std': float(np.std(cache_miss_times)) if cache_miss_times else 0
                } if cache_miss_times else {}
            }
        }
        
        # Calcular speedup del caché
        if cache_hit_times and cache_miss_times:
            analysis['cache_speedup'] = {
                'mean_speedup': float(np.mean(cache_miss_times) / np.mean(cache_hit_times)),
                'median_speedup': float(np.median(cache_miss_times) / np.median(cache_hit_times))
            }
        
        return analysis
    
    async def run_comparative_test(self, duration_minutes: int = 10):
        """Ejecuta pruebas comparativas con ambas distribuciones"""
        
        logger.info("Iniciando pruebas comparativas de distribuciones")
        
        # Limpiar caché antes de empezar
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.cache_uri}/cache/clear") as response:
                    clear_result = await response.json()
                    logger.info(f"Caché limpiado: {clear_result}")
        except Exception as e:
            logger.warning(f"Error limpiando caché: {e}")
        
        results = {}
        
        # Prueba con distribución Poisson
        logger.info("=== Iniciando prueba con distribución Poisson ===")
        self.reset_stats()
        results['poisson'] = await self.run_poisson_traffic(duration_minutes)
        
        # Esperar un poco entre pruebas
        await asyncio.sleep(10)
        
        # Prueba con distribución Exponencial
        logger.info("=== Iniciando prueba con distribución Exponencial ===")
        self.reset_stats()
        results['exponential'] = await self.run_exponential_traffic(duration_minutes)
        
        # Esperar un poco entre pruebas
        await asyncio.sleep(10)
        
        # Prueba con tráfico en ráfagas
        logger.info("=== Iniciando prueba con tráfico en ráfagas ===")
        self.reset_stats()
        results['burst'] = await self.run_burst_traffic()
        
        # Obtener estadísticas finales del caché
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.cache_uri}/cache/stats") as response:
                    cache_stats = await response.json()
                    results['final_cache_stats'] = cache_stats
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas finales: {e}")
        
        return results
    
    def reset_stats(self):
        """Reinicia las estadísticas del generador"""
        self.requests_sent = 0
        self.responses_received = 0
        self.errors = 0
        self.response_times = []
    
    def get_generator_stats(self) -> Dict:
        """Retorna estadísticas del generador"""
        return {
            'requests_sent': self.requests_sent,
            'responses_received': self.responses_received,
            'errors': self.errors,
            'error_rate': self.errors / self.requests_sent if self.requests_sent > 0 else 0,
            'avg_response_time': np.mean(self.response_times) if self.response_times else 0,
            'total_response_times': len(self.response_times)
        }
    
    async def run_continuous_traffic(self, hours: int = 1):
        """Ejecuta tráfico continuo mezclando ambas distribuciones"""
        
        logger.info(f"Iniciando tráfico continuo por {hours} horas")
        
        end_time = time.time() + (hours * 3600)
        results = []
        
        async with aiohttp.ClientSession() as session:
            while time.time() < end_time:
                # Alternar entre distribuciones cada 10 minutos
                current_minute = int((time.time() - (end_time - hours * 3600)) / 60)
                use_poisson = (current_minute // 10) % 2 == 0
                
                if use_poisson:
                    # Usar llegada Poisson
                    wait_time = np.random.exponential(1.0 / self.poisson_lambda)
                else:
                    # Usar llegada Exponencial
                    wait_time = np.random.exponential(1.0 / self.exponential_lambda)
                
                await asyncio.sleep(wait_time)
                
                # Generar y enviar consulta
                if random.random() < 0.8:
                    query = self.generate_query()
                    result = await self.send_query(session, query)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                
                results.append(result)
                
                # Log cada 100 consultas
                if len(results) % 100 == 0:
                    recent_results = results[-100:]
                    cache_hits = sum(1 for r in recent_results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(recent_results)
                    avg_time = np.mean([r['response_time'] for r in recent_results])
                    
                    logger.info(f"Tráfico continuo: {len(results)} consultas totales, "
                              f"Hit rate reciente: {hit_rate:.2%}, "
                              f"Tiempo promedio: {avg_time:.3f}s, "
                              f"Distribución actual: {'Poisson' if use_poisson else 'Exponencial'}")
        
        return self.analyze_results(results, "Continuo")
    
    def get_real_locations(self, limit: int = 100) -> List[Dict]:
        """Obtiene ubicaciones reales de eventos desde la base de datos"""
        try:
            # Obtener eventos con ubicaciones válidas
            pipeline = [
                {
                    '$match': {
                        'location.lat': {'$exists': True, '$ne': None},
                        'location.lng': {'$exists': True, '$ne': None}
                    }
                },
                {
                    '$sample': {'size': limit}
                },
                {
                    '$project': {
                        'location.lat': 1,
                        'location.lng': 1
                    }
                }
            ]
            
            locations = list(self.collection.aggregate(pipeline))
            logger.info(f"Obtenidas {len(locations)} ubicaciones reales de la base de datos")
            return locations
            
        except Exception as e:
            logger.error(f"Error obteniendo ubicaciones reales: {e}")
            return []
    
    def wait_for_real_data(self, min_events: int = 10, max_wait_minutes: int = 10):
        """Espera hasta que haya suficientes datos reales en la base de datos"""
        logger.info(f"Esperando al menos {min_events} eventos reales en la base de datos...")
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while time.time() - start_time < max_wait_seconds:
            try:
                event_count = self.collection.count_documents({})
                if event_count >= min_events:
                    logger.info(f"Encontrados {event_count} eventos reales. Procediendo...")
                    return True
                    
                logger.info(f"Solo {event_count} eventos encontrados. Esperando más datos...")
                time.sleep(30)  # Esperar 30 segundos antes de verificar nuevamente
                
            except Exception as e:
                logger.error(f"Error verificando datos: {e}")
                time.sleep(30)
        
        logger.warning(f"Tiempo de espera agotado. Solo se encontraron datos limitados.")
        return False
    
    def verify_data_availability(self) -> bool:
        """Verifica que haya suficientes datos reales disponibles"""
        try:
            total_events = self.collection.count_documents({})
            # Actualizar para el nuevo campo source
            real_events = self.collection.count_documents({'source': {'$in': ['waze_api_real', 'waze_web']}})
            municipalities = list(self.collection.distinct('municipality'))
            event_types = list(self.collection.distinct('event_type'))
            
            logger.info(f"Verificación de datos:")
            logger.info(f"- Total de eventos: {total_events}")
            logger.info(f"- Eventos reales: {real_events}")
            logger.info(f"- Municipios únicos: {len(municipalities)}")
            logger.info(f"- Tipos de eventos únicos: {len(event_types)}")
            
            if real_events < 10:
                logger.warning("Pocos eventos reales disponibles para generar tráfico significativo")
                return False
                
            if len(municipalities) < 2:
                logger.warning("Pocos municipios disponibles para consultas diversas")
                
            if len(event_types) < 2:
                logger.warning("Pocos tipos de eventos disponibles para consultas diversas")
            
            return True
            
        except Exception as e:
            logger.error(f"Error verificando disponibilidad de datos: {e}")
            return False

async def main():
    """Función principal del generador de tráfico"""
    
    # Configuración desde variables de entorno
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    cache_uri = os.getenv('CACHE_URI', 'http://localhost:8080')
    poisson_lambda = float(os.getenv('POISSON_LAMBDA', '10'))
    exponential_lambda = float(os.getenv('EXPONENTIAL_LAMBDA', '0.1'))
    
    # Crear generador
    generator = TrafficGenerator(mongodb_uri, cache_uri, poisson_lambda, exponential_lambda)
    
    # Esperar a que haya datos reales disponibles
    logger.info("Esperando datos reales en la base de datos...")
    min_events = int(os.getenv('MIN_REAL_EVENTS', '50'))
    max_wait_minutes = int(os.getenv('MAX_WAIT_MINUTES', '15'))
    
    if not generator.wait_for_real_data(min_events, max_wait_minutes):
        logger.error("No se encontraron suficientes datos reales. El generador puede no funcionar correctamente.")
    
    # Esperar a que el sistema de caché esté disponible
    logger.info("Esperando que el sistema de caché esté disponible...")
    for attempt in range(30):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{cache_uri}/health", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        logger.info("Sistema de caché disponible")
                        break
        except:
            pass
        
        logger.info(f"Intento {attempt + 1}/30 - Sistema de caché no disponible, esperando...")
        await asyncio.sleep(10)
    else:
        logger.error("Sistema de caché no disponible después de 5 minutos")
        return
    
    # Ejecutar modo de operación basado solo en datos reales
    mode = os.getenv('TRAFFIC_MODE', 'comparative')
    
    logger.info(f"=== INICIANDO GENERADOR DE TRÁFICO CON DATOS REALES ÚNICAMENTE ===")
    logger.info(f"Modo seleccionado: {mode}")
    
    # Verificar datos una vez más antes de proceder
    if not generator.verify_data_availability():
        logger.warning("Datos reales limitados. Los resultados pueden no ser representativos.")
    
    if mode == 'comparative':
        # Pruebas comparativas
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_comparative_test(duration)
        
        # Guardar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/logs/traffic_test_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Resultados guardados en {filename}")
        
        # Mostrar resumen
        for dist_type, result in results.items():
            if isinstance(result, dict) and 'summary' in result:
                summary = result['summary']
                cache_perf = result['cache_performance']
                response_times = result['response_times']['overall']
                
                logger.info(f"\n=== Resumen {dist_type.upper()} ===")
                logger.info(f"Consultas exitosas: {summary['successful_requests']}/{summary['total_requests']}")
                logger.info(f"Hit rate: {cache_perf['hit_rate']:.2%}")
                logger.info(f"Tiempo promedio: {response_times['mean']:.3f}s")
                logger.info(f"P95: {response_times['p95']:.3f}s")
    
    elif mode == 'continuous':
        # Tráfico continuo
        hours = int(os.getenv('CONTINUOUS_HOURS', '1'))
        results = await generator.run_continuous_traffic(hours)
        
        logger.info("Tráfico continuo completado")
        logger.info(f"Estadísticas finales: {generator.get_generator_stats()}")
    
    elif mode == 'poisson':
        # Solo Poisson
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_poisson_traffic(duration)
        logger.info(f"Prueba Poisson completada: {results}")
    
    elif mode == 'exponential':
        # Solo Exponencial
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_exponential_traffic(duration)
        logger.info(f"Prueba Exponencial completada: {results}")
    
    elif mode == 'burst':
        # Solo ráfagas
        num_bursts = int(os.getenv('NUM_BURSTS', '5'))
        burst_size = int(os.getenv('BURST_SIZE', '20'))
        burst_interval = int(os.getenv('BURST_INTERVAL', '30'))
        results = await generator.run_burst_traffic(num_bursts, burst_size, burst_interval)
        logger.info(f"Prueba de ráfagas completada: {results}")
    
    else:
        logger.error(f"Modo no reconocido: {mode}")

if __name__ == "__main__":
    asyncio.run(main())