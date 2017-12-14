from django import forms
from PortSelect_app.CAPMdata import get_portName_ManualList

class PSForm(forms.Form):
#    inname = forms.CharField(max_length=255)
    lt = get_portName_ManualList()
    portname = forms.ChoiceField(choices=lt, label='Index Name', required=True)
