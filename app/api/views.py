import logging

from rest_framework import permissions, status
from rest_framework.decorators import permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView

from core.tasks import execute_xia_automated_workflow

logger = logging.getLogger('dict_config_logger')


@permission_classes((permissions.AllowAny,))
class WorkflowView(APIView):
    """Handles HTTP requests for Metadata for XIS"""

    def get(self, request):
        logger.info('XIA workflow api')
        task = execute_xia_automated_workflow.delay()
        response_val = {"task_id": task.id}

        return Response(response_val, status=status.HTTP_202_ACCEPTED)
