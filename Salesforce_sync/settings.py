from pathlib import Path
import os

from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY')
DEBUG = os.getenv('DEBUG')

# ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS').split(",") if os.getenv('ALLOWED_HOSTS') else []
ALLOWED_HOSTS = ['*']

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # my apps
    'googleads_sync',

]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'Salesforce_sync.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates']
        ,
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'Salesforce_sync.wsgi.application'

IN_DOCKER = os.environ.get("IN_DOCKER", "False").lower() == "true"

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.environ.get('POSTGRES_DB'),
        'USER': os.environ.get('POSTGRES_USER'),
        'PASSWORD': os.environ.get('POSTGRES_PASSWORD'),
        'HOST': os.environ.get('POSTGRES_HOST'),
        # Use 'db' in Docker, 'localhost' otherwise
        'PORT': os.environ.get('POSTGRES_PORT')
    },
}

# Password validation
# https://docs.djangoproject.com/en/5.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/5.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.2/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/5.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "memory://")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "cache+memory://")
CELERY_TASK_TIME_LIMIT = 60 * 15
CELERY_TASK_SOFT_TIME_LIMIT = 60 * 10
CELERY_TASK_TRACK_STARTED = True
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BROKER_CONNECTION_MAX_RETRIES = 100
CELERY_BROKER_HEARTBEAT = 30
CELERY_BROKER_CONNECTION_TIMEOUT = 30

CELERY_TASK_ALWAYS_EAGER = True
CELERY_TASK_EAGER_PROPAGATES = True

CELERY_BEAT_SCHEDULE = {
    "sync-google-ads-regular": {
        "task": "ads_sync.tasks.sync_google_ads_pipeline",
        "schedule": 60 * 15,
        "options": {"queue": "sync"},
    },
    "sync-google-ads-nightly": {
        "task": "ads_sync.tasks.nightly_full_reconcile",
        "schedule": 60 * 60 * 24,
        "options": {"queue": "sync"},
    },
}

# Celery / Brokers

RABBITMQ_DEFAULT_USER = os.getenv("RABBITMQ_DEFAULT_USER")
RABBITMQ_DEFAULT_PASS = os.getenv("RABBITMQ_DEFAULT_PASS")

# Google Ads Auth
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")

GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")  # Manager account
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")  # Target account

# Salesforce auth
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN", "login")  # Or 'test' for sandbox

# Pub/Sub gRPC endpoint
SF_PUBSUB_ENDPOINT = os.getenv("SF_PUBSUB_ENDPOINT", "pubsub.salesforce.com:443")
