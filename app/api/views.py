from rest_framework import permissions, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from core.tasks import execute_xia_automated_workflow

# Create your views here.


@api_view(['GET'])
@permission_classes((permissions.AllowAny,))
def execute_xia_automated_workflow_api(request):
    print('XIA workflow api')
    result = execute_xia_automated_workflow()
    print("RESULT")
    print(result)
    return Response(result, status=status.HTTP_200_OK)
