from django import forms

PSDefaultList = {
    ('HSI', 'Hang Seng Index(HSI)'),
    ('QQQ', 'Nasdaq 100 Index(QQQ)'),
    ('test', 'Testing Stock List(test)'),
}

class PSForm(forms.Form):
#    inname = forms.CharField(max_length=255)
    portname = forms.ChoiceField(choices=PSDefaultList, label='Index Name', required=True)
