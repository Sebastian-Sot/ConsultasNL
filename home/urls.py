from django.urls import path
from .views import mi_vista

urlpatterns = [
    path('hi/', mi_vista, name='inicio'),
    # Define otras rutas aquí
]