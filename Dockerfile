FROM php:8.4-cli

# Dependências do sistema
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    libpq-dev \
    libzip-dev \
    git \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Extensões PHP
RUN pecl install rdkafka && docker-php-ext-enable rdkafka
RUN docker-php-ext-install pdo pdo_mysql pdo_pgsql zip bcmath

# Xdebug
RUN pecl install xdebug && docker-php-ext-enable xdebug

# Configuração do Xdebug
COPY config/xdebug.ini /usr/local/etc/php/conf.d/docker-php-ext-xdebug.ini

# Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

WORKDIR /app
COPY app/composer.json app/composer.lock* ./
RUN composer install --no-dev --optimize-autoloader 2>/dev/null || true

COPY app/ /app/

CMD ["php", "-a"]
