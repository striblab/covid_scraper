# Generated by Django 3.0.5 on 2020-04-10 21:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0008_auto_20200401_1243'),
    ]

    operations = [
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_0_5',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_20_44',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_45_64',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_65_plus',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_6_19',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='statewidetotaldate',
            name='cases_age_unknown',
            field=models.IntegerField(default=0),
        ),
    ]
