import os

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'openlxp_xia_dau_project.settings')

app = Celery('openlxp_xia_dau_project')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
