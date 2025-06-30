# Dockerfile para el módulo Scraper
FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    libxml2-dev \
    libxslt-dev \
    libz-dev \
    gcc \
    g++ \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Configurar directorio de trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python (webdriver-manager manejará ChromeDriver automáticamente)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get update && apt-get remove -y gcc g++ && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Copiar código fuente
COPY main.py .

# Crear directorio para logs y cache de webdriver-manager
RUN mkdir -p /app/logs /app/.wdm

# Configurar usuario no-root
RUN useradd -m -u 1001 scraper && chown -R scraper:scraper /app
USER scraper

# Variables de entorno para webdriver-manager
ENV WDM_LOG_LEVEL=0
ENV WDM_CACHE_PATH=/app/.wdm

# Comando por defecto
CMD ["python", "main.py"]