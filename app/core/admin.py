from django.contrib import admin
from .models import XIAConfiguration


# Register your models here.

@admin.register(XIAConfiguration)
class XIAConfigurationAdmin(admin.ModelAdmin):
    list_display = (
        'source_file_name', 'source_schema', 'data_file_name', 'source_validation_schema', 'target_renaming_schema',
        'target_mapping_schema', 'target_file_name', 'target_validation_schema', 'bucket_name', 'upload_bucket_name')
    fields = ['source_file_name', 'source_schema', 'data_file_name', 'source_validation_schema',
              'target_renaming_schema',
              'target_mapping_schema', 'target_file_name', 'target_validation_schema',
              ('bucket_name', 'upload_bucket_name')]

