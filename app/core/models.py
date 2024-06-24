from django.db import models


class XSRConfiguration(models.Model):
    """Model for XSR Configuration """

    xsr_api_endpoint = models.CharField(
        help_text='Enter the XSR API endpoint',
        max_length=200
    )
    token = models.CharField(
        help_text='Enter the XSR Token',
        max_length=200
    )
