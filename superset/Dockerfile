FROM apache/superset:latest

USER root

# Crear carpeta donde Superset carga módulos externos
RUN mkdir -p /app/pythonpath

# Dependencias del sistema necesarias para que Pillow compile correctamente
RUN apt-get update && apt-get install -y \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libopenjp2-7-dev \
    libtiff5-dev \
    tk-dev \
    python3-tk \
    && apt-get clean

# Instalar Pillow
RUN pip install --no-cache-dir pillow

USER superset