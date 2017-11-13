from django.conf.urls import url
from PortSelect_app import views

# TEMPLATE TAGGINS
app_name = 'PS_app'

urlpatterns = [
    url(r'^$', views.PortSelectView.as_view(), name='PSindex'),
]
