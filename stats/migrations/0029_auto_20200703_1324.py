# Generated by Django 3.0.7 on 2020-07-03 18:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0028_auto_20200624_1151'),
    ]

    operations = [
        migrations.AlterField(
            model_name='statewidedeathsdate',
            name='new_deaths',
            field=models.IntegerField(default=0, null=True),
        ),
        migrations.AlterField(
            model_name='statewidetotaldate',
            name='currently_hospitalized',
            field=models.IntegerField(default=0, null=True),
        ),
        migrations.AlterField(
            model_name='statewidetotaldate',
            name='currently_in_icu',
            field=models.IntegerField(default=0, null=True),
        ),
    ]
