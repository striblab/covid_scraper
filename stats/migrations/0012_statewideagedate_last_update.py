# Generated by Django 3.0.5 on 2020-05-01 14:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0011_auto_20200501_0914'),
    ]

    operations = [
        migrations.AddField(
            model_name='statewideagedate',
            name='last_update',
            field=models.DateTimeField(auto_now=True),
        ),
    ]
