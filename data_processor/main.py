#!/usr/bin/env python3
"""
Data Processor - Módulo de Limpieza y Normalización de Datos
Procesa, filtra, deduplica y agrupa eventos similares de Waze
"""

import os
import time
import schedule
import logging
from typing import Dict, Any
from datetime import datetime
from data_cleaner import WazeDataCleaner

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/data_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_data_cleaning() -> Dict[str, Any]:
    """Ejecuta el proceso de limpieza de datos"""
    try:
        logger.info("Iniciando proceso programado de limpieza de datos")
        
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/traffic_db?authSource=admin')
        cleaner = WazeDataCleaner(mongodb_uri)
        
        if not cleaner.connect_database():
            return {'success': False, 'error': 'No se pudo conectar a MongoDB'}
        
        result = cleaner.run_full_cleaning_process()
        
        if result.get('success'):
            logger.info(f"✅ Limpieza completada: {result['cleaned_events']} eventos procesados")
            logger.info(f"📁 CSV generado: {result['csv_file']}")
            logger.info(f"📊 Reducción de datos: {result['statistics']['reduction_percentage']:.1f}%")
        else:
            logger.error(f"❌ Error en limpieza: {result.get('error')}")
            
        return result
            
    except Exception as e:
        logger.error(f"Error ejecutando limpieza programada: {e}")
        return {'success': False, 'error': str(e)}

def run_manual_cleaning() -> Dict[str, Any]:
    """Ejecuta limpieza manual con parámetros específicos"""
    try:
        logger.info("=== INICIANDO LIMPIEZA MANUAL DE DATOS ===")
        
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/traffic_db?authSource=admin')
        cleaner = WazeDataCleaner(mongodb_uri)
        
        # Conectar a la base de datos
        if not cleaner.connect_database():
            return {'success': False, 'error': 'No se pudo conectar a MongoDB'}
        
        # Ejecutar proceso completo de limpieza
        result = cleaner.run_full_cleaning_process()
        
        return result
        
    except Exception as e:
        logger.error(f"Error en limpieza manual: {e}")
        return {'success': False, 'error': str(e)}

def show_statistics():
    """Muestra estadísticas de los datos"""
    try:
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/traffic_db?authSource=admin')
        cleaner = WazeDataCleaner(mongodb_uri)
        
        if not cleaner.connect_database():
            logger.error("No se pudo conectar a MongoDB")
            return
        
        stats = cleaner.get_data_statistics()
        
        logger.info("=== ESTADÍSTICAS DE DATOS ===")
        logger.info(f"Eventos totales: {stats.get('total_events', 0)}")
        logger.info(f"Eventos únicos: {stats.get('unique_events', 0)}")
        logger.info(f"Eventos duplicados: {stats.get('duplicate_events', 0)}")
        logger.info(f"Eventos limpios disponibles: {stats.get('clean_events', 0)}")
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")

def main():
    """Función principal del data processor"""
    # Configurar horarios de ejecución
    cleaning_interval = int(os.getenv('CLEANING_INTERVAL_MINUTES', '2'))  # Cada 2 minutos por defecto
    run_once = os.getenv('RUN_ONCE', 'false').lower() == 'true'
    mode = os.getenv('PROCESSING_MODE', 'scheduled')  # scheduled, manual, stats
    
    logger.info(f"=== INICIANDO DATA PROCESSOR ===")
    logger.info(f"Modo: {mode}")
    logger.info(f"Intervalo de limpieza: cada {cleaning_interval} minutos")
    logger.info(f"Ejecución única: {run_once}")
    
    if mode == 'manual':
        # Ejecutar limpieza manual
        logger.info("Ejecutando limpieza manual...")
        result = run_manual_cleaning()
        
        if result.get('success'):
            logger.info("✅ Limpieza manual completada exitosamente")
            if result.get('csv_file'):
                logger.info(f"📁 Archivo CSV generado: {result['csv_file']}")
        else:
            logger.error(f"❌ Error en limpieza manual: {result.get('error')}")
        
        return result
        
    elif mode == 'stats':
        # Mostrar estadísticas
        show_statistics()
        return
        
    else:
        # Modo programado (por defecto)
        logger.info("Ejecutando limpieza inicial...")
        result = run_data_cleaning()
        
        if run_once:
            logger.info("Modo ejecución única activado. Finalizando...")
            return result
        
        # Programar limpieza recurrente cada X minutos
        schedule.every(cleaning_interval).minutes.do(run_data_cleaning)
        
        # Loop principal
        logger.info("Entrando en modo de monitoreo continuo...")
        try:
            while True:
                schedule.run_pending()
                time.sleep(30)  # Verificar cada 30 segundos para mayor precisión
        except KeyboardInterrupt:
            logger.info("Proceso interrumpido por el usuario")
        except Exception as e:
            logger.error(f"Error en loop principal: {e}")

if __name__ == "__main__":
    main()