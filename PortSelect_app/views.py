from django.shortcuts import render
from django.http import HttpResponse
from django.views.generic import View,TemplateView
from PortSelect_app.forms import PSForm
from PortSelect_app.CAPMapi import getHistPortWeighting

# Create your views here.
class PortSelectView(TemplateView):
    template_name = 'PortSelect_app/PSindex.html'

    def get(self, request):
        form = PSForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request):
        form = PSForm(request.POST)
        text = "No text received!"
        if form.is_valid():
            pname = form.cleaned_data['portname']
            text = "Calculated weightings for {} portfolio".format(pname)
            weightings = getHistPortWeighting(pname)
            html_table = weightings.to_html(classes="pstable", index=False)
        args = {'form':form, 'text': text, 'weightings': html_table}
        print(" Result weightings is {}".format(weightings))
        return render(request, self.template_name, args)
