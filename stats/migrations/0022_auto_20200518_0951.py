# Generated by Django 3.0.6 on 2020-05-18 14:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0021_auto_20200515_0905'),
    ]

    operations = [
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_daily_change',
            field=models.IntegerField(default=0, null=True),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_newly_reported',
            field=models.IntegerField(default=0, null=True),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_removed',
            field=models.IntegerField(default=0, null=True),
        ),
    ]
