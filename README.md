# Sistema de Procesamiento de Datos de Tráfico Waze
## SD_2025_1_T3 - Sistemas Distribuidos

![Status](https://img.shields.io/badge/Status-Activo-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Python](https://img.shields.io/badge/Python-3.11-blue)

## 📋 Descripción

Sistema distribuido que recolecta, procesa y limpia datos de tráfico en tiempo real desde la API de Waze para la Región Metropolitana de Santiago. Automatiza la obtención, normalización y exportación de eventos de tráfico cada 2 minutos.

## 🏗️ Componentes del Sistema

```
Waze API → Scraper → MongoDB → Data Processor → CSV Files
                      ↓
              API Gateway ← Cache System
                      ↓
             Visualization Dashboard
```

### Módulos Principales:

- **🕷️ Scraper**: Obtiene eventos reales de Waze cada 5 minutos
- **🧹 Data Processor**: Limpia y normaliza datos cada 2 minutos  
- **�️ MongoDB**: Almacena eventos de tráfico
- **🌐 API Gateway**: Expone endpoints para consultas
- **⚡ Cache System**: Optimiza consultas frecuentes
- **📊 Visualization**: Dashboard web para visualización

## 🚀 Instalación Rápida

```bash
# Levantar todos los servicios
docker-compose up --build -d

# Verificar estado
docker-compose ps

# Ver logs del procesador de datos
docker-compose logs data_processor
```

## 📊 Funcionamiento Principal

### 1. Recolección de Datos (Scraper)
- Obtiene eventos reales de Waze cada **5 minutos**
- Cubre 5 zonas de Santiago (Centro, Oriente, Poniente, Sur, Norte)
- Almacena datos brutos en MongoDB

### 2. Procesamiento y Limpieza (Data Processor)
- Se ejecuta automáticamente cada **2 minutos**
- **Filtra** eventos incompletos o inválidos
- **Deduplica** eventos por ID y proximidad geográfica
- **Agrupa** eventos similares (< 500m, < 30 min)
- **Exporta** datos limpios a CSV con formato estándar

### 3. Acceso a Datos
```bash
# API endpoints disponibles
GET /events                    # Todos los eventos
GET /events/municipality/name  # Por municipio
GET /events/type/jam          # Por tipo de evento
GET /events/nearby            # Por ubicación