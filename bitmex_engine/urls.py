from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('engine/init', views.init_engine, name='init_engine'),
    path('engine/shutdown', views.shutdown_engine, name='shutdown_engine'),
    path('add/strategy', views.add_strategy, name='add_strategy'),
    path('remove/strategy', views.remove_strategy, name='remove_strategy'),
]
