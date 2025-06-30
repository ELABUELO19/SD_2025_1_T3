# Dockerfile para Data Processor (Filtrado y Homogeneización)
FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar directorio de trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY main.py .
COPY data_cleaner.py .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Configurar usuario no-root
RUN useradd -m -u 1001 processor && chown -R processor:processor /app
USER processor

# Variables de entorno por defecto
ENV PROCESSING_INTERVAL=600
ENV BATCH_SIZE=1000
ENV PROCESSING_MODE=scheduled
ENV RUN_ONCE=false
ENV CLEANING_INTERVAL_MINUTES=2

# Comando por defecto
CMD ["python", "main.py"]

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1